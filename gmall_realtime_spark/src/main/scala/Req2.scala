import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.JDBCUtil

import java.text.SimpleDateFormat
import java.util.Date

object Req2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Req2")
    val ssc = new StreamingContext(conf, Seconds(3))

    val kafkaParam = Map[String, Object](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.GROUP_ID_CONFIG -> "mockConsumer"
    )

    val kafkaDStream = KafkaUtils.createDirectStream[String, String](
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set("mock"), kafkaParam)
    )

    //kafkaDStream.map(_.value()).print()
    val adClickData = kafkaDStream.map(
      kafkaData => {
        val data = kafkaData.value()
        val fields = data.split(" ")
        AdClickData(fields(0), fields(1), fields(2), fields(3), fields(4))
      }
    )

    val reducedDS = adClickData.map(d => {
      val day = new SimpleDateFormat("yyyy-MM-dd").format(new Date(d.ts.toLong))
      ((day, d.area, d.city, d.ad), 1)
    }).reduceByKey(_ + _)

    reducedDS.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          iter => {
            val conn = JDBCUtil.getConnection
            val sql = "insert into area_city_ad_count (dt, area, city, adid, count) values (?, ?, ?, ?, ?) on duplicate key update count = count + ?"
            val pstat = conn.prepareStatement(sql)
            iter.foreach {
              case ((day, area, city, ad), cnt) => {
                pstat.setString(1, day)
                pstat.setString(2, area)
                pstat.setString(3, city)
                pstat.setString(4, ad)
                pstat.setInt(5, cnt)
                pstat.setInt(6, cnt)
                pstat.executeUpdate()
              }
            }
            pstat.close()
            conn.close()
          }
        )
      }
    )

    ssc.start()
    ssc.awaitTermination()
  }


  case class AdClickData(ts: String, area: String, city: String, user: String, ad: String)
}
