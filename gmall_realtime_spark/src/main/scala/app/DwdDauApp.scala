package app

import bean.{DauInfo, PageLog}
import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.{BeanUtils, Config, EsUtils, KafkaTools, OffsetsUtils, RedisUtil}

import java.text.SimpleDateFormat
import java.time.{LocalDate, Period}
import java.util.Date
import scala.collection.mutable

/**
 * Daily-Active-User (DAU)
 */

object DwdDauApp {
  def main(args: Array[String]): Unit = {

    revertState()

    val conf = new SparkConf().setMaster(Config.SPARK_CONF_MASTER).setAppName("DWD_DAU_APP")
    val ssc = new StreamingContext(conf, Seconds(5))

    val (kafkaDStream, offsets) = KafkaTools.getKafkaDStreamWithOffsets(ssc, Config.DWD_PAGE_LOG_TOPIC, Config.DWD_DAU_GROUP)

    // convert ConsumerRecord into PageLog stream
    val pageLogDStream = kafkaDStream.map(consumerRecord => {
      val str = consumerRecord.value()
      JSON.parseObject(str, classOf[PageLog])
    }) //.print(100)

    pageLogDStream.cache()
    pageLogDStream.foreachRDD(rdd => println("before filtering..." + rdd.count()))

    // filter pageLog to keep only the first landing page
    val filteredPageLog = pageLogDStream.filter(_.last_page_id == null)
    filteredPageLog.cache()
    filteredPageLog.foreachRDD(rdd => {
      println("after filtering..." + rdd.count())
      println("--------------------")
    })

    // use "mid" instead of "uid" to represent active user because some visitors are not registered, so do not have "uid"
    // add "mid" into redis set to remove duplicates within the same day
    val pageLogUniqMIDDStream = filteredPageLog.mapPartitions(pageLogIter => {

      val pageLogList = pageLogIter.toList
      println("before redis...." + pageLogList.size)

      val pageLogUniqMID = mutable.ListBuffer[PageLog]()
      val jedis = RedisUtil.getJedisFromPool()
      for (pageLog <- pageLogList) {
        val dateStr = new SimpleDateFormat("yyyy-MM-dd").format(new Date(pageLog.ts))
        val key = s"DAU:$dateStr"
        val isSucc = jedis.sadd(key, pageLog.mid)
        if (isSucc == 1L) {
          pageLogUniqMID.append(pageLog)
        }
      }
      jedis.close()
      println("after redis..." + pageLogUniqMID.size)
      pageLogUniqMID.iterator
    })
    //pageLogUniqMIDDStream.print()

    // convert pageLog stream to dauInfo stream
    val dauInfoDStream = pageLogUniqMIDDStream.mapPartitions(pageLogIter => {
      val dauList = mutable.ListBuffer[DauInfo]()
      val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val jedis = RedisUtil.getJedisFromPool()
      for (pageLog <- pageLogIter) {
        val dauInfo = new DauInfo()

        // gather info from pageLog
        BeanUtils.copyProperties(pageLog, dauInfo)

        // gather user info
        val redisUidKey = s"DIM:USER_INFO:${pageLog.user_id}"
        val userInfoJsonObj = JSON.parseObject(jedis.get(redisUidKey))

        val gender = userInfoJsonObj.getString("gender")
        dauInfo.user_gender = gender

        val birthDayLD = LocalDate.parse(userInfoJsonObj.getString("birthday"))
        val nowLD = LocalDate.now()
        val period = Period.between(birthDayLD, nowLD)
        val age = period.getYears
        dauInfo.user_age = age.toString

        // gather area info
        val redisKey = s"DIM:BASE_PROVINCE:${dauInfo.province_id}"
        val areaJsonObj = JSON.parseObject(jedis.get(redisKey))
        val areaName = areaJsonObj.getString("name")
        val isoCode = areaJsonObj.getString("iso_code")
        val area3166 = areaJsonObj.getString("iso_3166_2")
        val areaCode = areaJsonObj.getString("area_code")
        dauInfo.province_name = areaName
        dauInfo.province_iso_code = isoCode
        dauInfo.province_3166_2 = area3166
        dauInfo.province_area_code = areaCode

        // gather time info
        val dtHr = sdf.format(new Date(pageLog.ts))
        val dtHrArr = dtHr.split(" ")
        val dt = dtHrArr(0)
        val hr = dtHrArr(1).split(":")(0)
        dauInfo.dt = dt
        dauInfo.hr = hr

        dauList.append(dauInfo)
      }
      jedis.close()
      dauList.iterator
    })

    //dauInfoDStream.print(100)

    // write into OLAP (ElasticSearch)
    dauInfoDStream.foreachRDD(rdd => {
      rdd.foreachPartition(dauInfoIter => {
        val docs = dauInfoIter.map(dauInfo => (dauInfo.mid, dauInfo)).toList
        if (docs.nonEmpty) {
          val (hmid, hinfo) = docs.head
          val dateStr = new SimpleDateFormat("yyyy-MM-dd").format(new Date(hinfo.ts))
          val indexName = s"gmall_dau_info_$dateStr"
          EsUtils.bulkSave(indexName, docs)
        }
      })
      OffsetsUtils.saveOffsets(Config.DWD_PAGE_LOG_TOPIC, Config.DWD_DAU_GROUP, offsets)
    })

    ssc.start()
    ssc.awaitTermination()
  }

  /**
   * To prevent ES and Redis out-of-sync due to errors when writing to ES,
   * before (re)starting the stream, read ES to get all mid
   * and write them to redis to keep ES and redis in sync
   */

  def revertState(): Unit = {
    val date = LocalDate.now()
    val indexName = s"gmall_dau_info_$date"
    val fieldName = "mid"
    val mids = EsUtils.searchField(indexName, fieldName)

    val jedis = RedisUtil.getJedisFromPool()
    val redisKey = s"DAU:$date"
    jedis.del(redisKey)
    if (mids != null && mids.nonEmpty) {
      val pipeline = jedis.pipelined()
      for (mid <- mids) {
        pipeline.sadd(redisKey, mid)
      }
      pipeline.sync()
    }
    jedis.close()
  }


}
