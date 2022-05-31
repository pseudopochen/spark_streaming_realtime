package app

import bean.{ActionInfo, CommonInfo, DisplayInfo, PageActionLog, PageDisplayLog, PageInfo, PageLog, StartLog}
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.{Config, KafkaTools, OffsetsUtils}

object OdsBaseLogApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(Config.SPARK_CONF_MASTER).setAppName("ODS_BASELOG_APP")
    val ssc = new StreamingContext(conf, Seconds(5))

    val topicString = Config.MOCK_BASE_LOG_TOPIC
    val groupID = Config.BASE_LOG_CONSUMER_GROUP

    val offsets = OffsetsUtils.readOffsets(topicString, groupID)
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsets != null && offsets.nonEmpty) {
      kafkaDStream = KafkaTools.getKafkaDStream(ssc, topicString, groupID, offsets)
    } else {
      kafkaDStream = KafkaTools.getKafkaDStream(ssc, topicString, groupID)
    }
    // extract offset info to store in redis
    var offsetRanges: Array[OffsetRange] = null
    val offsetRangesDStream = kafkaDStream.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })

    // convert from ConsumerRecord[String, String] to json object stream
    val jsonObjDStream = offsetRangesDStream.map(consumerRecord => {
      val str = consumerRecord.value()
      JSON.parseObject(str)
    }) //.print(1000)

    // split the json object stream into 5 topics and write back to kafka
    jsonObjDStream.foreachRDD(
      rdd => {
        rdd.foreachPartition(jsonObjIter => {
          for (jsonObj <- jsonObjIter) {
            // error msg
            val errObj = jsonObj.getJSONObject("err")
            if (errObj != null) {
              KafkaTools.send(Config.DWD_ERROR_LOG_TOPIC, jsonObj.toJSONString)
            } else { // not error msg
              // common info
              val commonInfo = CommonInfo(jsonObj.getJSONObject("common"))

              // time stamp
              val ts = jsonObj.getLong("ts")

              // page info
              val pageObj = jsonObj.getJSONObject("page")
              if (pageObj != null) {
                val pageInfo = PageInfo(pageObj)

                val pageLog = PageLog(commonInfo, pageInfo, ts)
                // no getter/setter in case class, so use SerializeConfig(fieldBase=true) to avoid using getter/setter
                KafkaTools.send(Config.DWD_PAGE_LOG_TOPIC, JSON.toJSONString(pageLog, new SerializeConfig(true)))

                // exposure info
                val displayJsonArray = jsonObj.getJSONArray("displays")
                if (displayJsonArray != null && displayJsonArray.size() > 0) {
                  for (i <- 0 until displayJsonArray.size()) {
                    val displayObj = displayJsonArray.getJSONObject(i)
                    val displayInfo = DisplayInfo(displayObj)
                    val displayLog = PageDisplayLog(pageLog, displayInfo)
                    KafkaTools.send(Config.DWD_PAGE_DISPLAY_TOPIC, JSON.toJSONString(displayLog, new SerializeConfig(true)))
                  }
                }

                // action info
                val actionJsonArray = jsonObj.getJSONArray("actions")
                if (actionJsonArray != null && actionJsonArray.size() > 0) {
                  for (i <- 0 until actionJsonArray.size()) {
                    val actionObj = actionJsonArray.getJSONObject(i)
                    val actionInfo = ActionInfo(actionObj)
                    val actionLog = PageActionLog(pageLog, actionInfo)
                    KafkaTools.send(Config.DWD_PAGE_ACTION_TOPIC, JSON.toJSONString(actionLog, new SerializeConfig(true)))
                  }
                }
              }

              // start info
              val startObj = jsonObj.getJSONObject("start")
              if (startObj != null) {
                val startLog = StartLog(commonInfo, startObj, ts)
                KafkaTools.send(Config.DWD_START_LOG_TOPIC, JSON.toJSONString(startLog, new SerializeConfig(true)))
              }
            } // not error msg
          }
          // Executor code: flush data from producer memory buffer to disk before saveOffsets to ensure at-least-once
          KafkaTools.flush()
        })

        //        rdd.foreach(
        //          jsonObj => {
        //
        //          }
        //        ) // rdd.foreach

        // Driver code: save offsets into redis for the next iteration, guarantee at-least-once
        OffsetsUtils.saveOffsets(topicString, groupID, offsetRanges)
      }
    ) // jsonObjDStream.foreachRDD


    ssc.start()
    ssc.awaitTermination()
  }

}
