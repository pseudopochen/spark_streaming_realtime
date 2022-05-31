import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.JDBCUtil

import java.text.SimpleDateFormat
import java.util.Date
import scala.collection.mutable.ListBuffer

object Req1 {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[*]").setAppName("Req1")
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

    val ds = adClickData.transform(
      rdd => {
        val blackList = ListBuffer[String]()
        val conn = JDBCUtil.getConnection
        val pstat = conn.prepareStatement("select userid from black_list")
        val rs = pstat.executeQuery()
        while (rs.next()) {
          blackList.append(rs.getString(1))
        }
        rs.close()
        pstat.close()
        conn.close()

        val filteredRDD = rdd.filter(d => !blackList.contains(d.user))
        filteredRDD.map(
          data => {
            val day = new SimpleDateFormat("yyyy-MM-dd").format(new Date(data.ts.toLong))
            ((day, data.user, data.ad), 1)
          }
        ).reduceByKey(_ + _)
      }
    )

    ds.foreachRDD(
      rdd => {
        rdd.foreachPartition(
          iter => {
            val conn = JDBCUtil.getConnection
            iter.foreach {
              case ((day, user, ad), cnt) => {
                if (cnt > 30) {
                  val sql = "insert into black_list (userid) values (?) on duplicate key update userid = ?"
                  JDBCUtil.executeUpdate(conn, sql, Array(user, user))
                } else {
                  // check existence
                  val sql = "select * from user_ad_count where dt = ? and userid = ? and adid = ?"
                  val flg = JDBCUtil.isExist(conn, sql, Array(day, user, ad))
                  if (flg) { // if exists, update
                    val sql1 = "update user_ad_count set count = count + ? where dt = ? and userid = ? and adid = ?"
                    JDBCUtil.executeUpdate(conn, sql1, Array(cnt, day, user, ad))
                    // after update, check if count going over threshold of 30
                    val sql2 = "select * from user_ad_count where dt = ? and userid = ? and adid = ? and count > 30"
                    val flg1 = JDBCUtil.isExist(conn, sql2, Array(day, user, ad))
                    if (flg1) { // if over, add user to black_list
                      val sql3 = "insert into black_list (userid) values (?) on duplicate key update userid = ?"
                      JDBCUtil.executeUpdate(conn, sql3, Array(user, user))
                    }
                  } else { // if not exists, insert
                    val sql4 = "insert into user_ad_count (dt, userid, adid, count) values (?,?,?,?)"
                    JDBCUtil.executeUpdate(conn, sql4, Array(day, user, ad, cnt))
                  }
                }
              }
            }
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
