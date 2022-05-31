package app

import com.alibaba.fastjson.JSON
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.{Config, KafkaTools, OffsetsUtils, RedisUtil}

object OdsBaseDbApp {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(Config.SPARK_CONF_MASTER).setAppName("ODS_BASE_DB")
    val ssc = new StreamingContext(conf, Seconds(5))

    val (kafkaDStream, offsetRanges) = KafkaTools.getKafkaDStreamWithOffsets(ssc, Config.MOCK_BASE_DB_TOPIC, Config.BASE_DB_CONSUMER_GROUP)

    val jsonObjDStream = kafkaDStream.map(
      consumerRecord => {
        val str = consumerRecord.value()
        JSON.parseObject(str)
      }) //.print(1000)

    jsonObjDStream.foreachRDD(rdd => {
      val redisFactKey = "FACT:TABLES"
      val redisDimKey = "DIM:TABLES"
      val jedis1 = RedisUtil.getJedisFromPool()
      // fact table list
      val factTables = jedis1.smembers(redisFactKey)
      println("factTables: " + factTables)
      val factTablesBC = ssc.sparkContext.broadcast(factTables)

      //dim table list
      val dimTables = jedis1.smembers(redisDimKey)
      println("dimTables: " + dimTables)
      val dimTablesBC = ssc.sparkContext.broadcast(dimTables)
      jedis1.close()

      rdd.foreachPartition(jsonIter => {
        // one redis connection per partition
        val jedis = RedisUtil.getJedisFromPool()

        for (jsonObj <- jsonIter) {

          // identify the operation type
          val opStr = jsonObj.getString("type")
          val opVal = opStr match {
            case "insert" | "bootstrap-insert" => "I" // add the bootstrap info for historic dim table
            case "update" => "U"
            case "delete" => "D"
            case _ => null
          }
          // filter out operations other than I/U/D
          if (opVal != null) {

            val tableName = jsonObj.getString("table")

            // fact table: write to Kafka topic
            if (factTablesBC.value.contains(tableName)) {
              println("fact tableName: " + tableName)
              val data = jsonObj.getString("data")
              val dwdTopicName: String = s"DWD_${tableName.toUpperCase}_$opVal"
              KafkaTools.send(dwdTopicName, data)
            }

            // dimension table: write to redis
            if (dimTablesBC.value.contains(tableName)) {
              println("dim tableName: " + tableName)
              val data = jsonObj.getJSONObject("data")
              val id = data.getString("id")
              val redisKey = s"DIM:${tableName.toUpperCase}:$id"
              jedis.set(redisKey, data.toJSONString)
            }
          } // if (opVal != null)
        }
        // close redis connection for each partition
        jedis.close()
        // flush kafka producer buffer memory to disk before saving offsets
        KafkaTools.flush()
      })
      // save offsets to redis
      OffsetsUtils.saveOffsets(Config.MOCK_BASE_DB_TOPIC, Config.BASE_DB_CONSUMER_GROUP, offsetRanges)
    })


    ssc.start()
    ssc.awaitTermination()
  }
}
