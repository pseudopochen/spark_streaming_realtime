package app

import bean.{OrderDetail, OrderInfo, OrderWide}
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializeConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import utils.{Config, EsUtils, KafkaTools, OffsetsUtils, RedisUtil}

import java.time.{LocalDate, Period}
import scala.collection.mutable.ListBuffer
import scala.collection.JavaConverters._

/**
 * Order wide table
 */
object DwdOrderApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster(Config.SPARK_CONF_MASTER).setAppName("DWD_ORDER_APP")
    val ssc = new StreamingContext(conf, Seconds(5))

    val orderInfoTopicName: String = "DWD_ORDER_INFO_I"
    val orderInfoGroup: String = "DWD_ORDER_INFO_GROUP"
    val (orderInfoDStream, orderInfoOffsets) = KafkaTools.getKafkaDStreamWithOffsets(ssc, orderInfoTopicName, orderInfoGroup)
    val orderInfo = orderInfoDStream.map(consumerRecord => JSON.parseObject(consumerRecord.value(), classOf[OrderInfo]))

    val orderDetailTopicName: String = "DWD_ORDER_DETAIL_I"
    val orderDetailGroup: String = "DWD_ORDER_DETAIL_GROUP"
    val (orderDetailDStream, orderDetailOffsets) = KafkaTools.getKafkaDStreamWithOffsets(ssc, orderDetailTopicName, orderDetailGroup)
    val orderDetail = orderDetailDStream.map(consumerRecord => JSON.parseObject(consumerRecord.value(), classOf[OrderDetail]))

    //orderInfo.print(100)
    //orderDetail.print(100)

    // correlate the dimensions of orderInfo
    val orderInfoDimDStream = orderInfo.mapPartitions(orderInfoIter => {
      val orderInfoList = orderInfoIter.toList
      val jedis = RedisUtil.getJedisFromPool()
      for (orderInfo <- orderInfoList) {
        // correlate user dimension
        val redisUserKey = s"DIM:USER_INFO:${orderInfo.user_id}"

        val userJson = JSON.parseObject(jedis.get(redisUserKey))
        val gender = userJson.getString("gender")
        orderInfo.user_gender = gender

        val birthday = userJson.getString("birthday")
        val age = Period.between(LocalDate.parse(birthday), LocalDate.now()).getYears
        orderInfo.user_age = age

        // correlate area dimension
        val redisProvinceKey = s"DIM:BASE_PROVINCE:${orderInfo.province_id}"
        val areaObj = JSON.parseObject(jedis.get(redisProvinceKey))
        orderInfo.province_name = areaObj.getString("name")
        orderInfo.province_3166_2_code = areaObj.getString("iso_3166_2")
        orderInfo.province_area_code = areaObj.getString("area_code")
        orderInfo.province_iso_code = areaObj.getString("iso_code")

        // process time info
        val strArr = orderInfo.create_time.split(" ")
        orderInfo.create_date = strArr(0)
        orderInfo.create_hour = strArr(1).split(":")(0)
      }
      jedis.close()
      orderInfoList.iterator
    })

    //orderInfoDimDStream.print(100)

    // join orderInfo and orderDetail streams
    // must convert streams into key-value streams before joining op
    val orderInfoKV = orderInfoDimDStream.map(orderInfo => (orderInfo.id, orderInfo))
    val orderDetailKV = orderDetail.map(od => (od.order_id, od))

    // use fullOuterJoin to deal with the situation that the two streams are out of sync
    val orderJoinDStream = orderInfoKV.fullOuterJoin(orderDetailKV)

    val orderWideDStream = orderJoinDStream.mapPartitions(orderJoinIter => {
      val orderWideList = ListBuffer[OrderWide]()
      val jedis = RedisUtil.getJedisFromPool()
      for ((orderId, (orderInfoOp, orderDetailOp)) <- orderJoinIter) {
        if (orderInfoOp.isDefined) { // orderInfo exists
          val orderInfo = orderInfoOp.get
          if (orderDetailOp.isDefined) { // orderDetail exists in the same rdd
            val orderDetail = orderDetailOp.get
            orderWideList.append(new OrderWide(orderInfo, orderDetail))
          }
          // write orderInfo into redis for matching with out-of-sync orderDetail
          val redisOrderInfoKey = s"ORDERJOIN:ORDER_INFO:$orderId"
          jedis.setex(redisOrderInfoKey, 24 * 3600, JSON.toJSONString(orderInfo, new SerializeConfig(true)))

          // read redis to match with out-of-sync orderDetail
          val redisOrderDetailKey = s"ORDERJOIN:ORDER_DETAIL:$orderId"
          val orderDetailSet = jedis.smembers(redisOrderDetailKey)
          if (orderDetailSet != null && orderDetailSet.size() > 0) {
            for (orderDetailJson <- orderDetailSet.asScala) {
              orderWideList.append(new OrderWide(orderInfo, JSON.parseObject(orderDetailJson, classOf[OrderDetail])))
            }
          }
        } else { // orderInfo does not exist, orderDetail exists
          // read redis to match with out-of-sync orderInfo
          val orderDetail = orderDetailOp.get
          val redisOrderInfoKey = s"ORDERJOIN:ORDER_INFO:$orderId"
          val orderInfoJson = jedis.get(redisOrderInfoKey)
          if (orderInfoJson != null && orderInfoJson.nonEmpty) {
            orderWideList.append(new OrderWide(JSON.parseObject(orderInfoJson, classOf[OrderInfo]), orderDetail))
          } else { // no matching orderInfo in redis, write orderDetail into redis
            val redisOrderDetailKey = s"ORDERJOIN:ORDER_DETAIL:$orderId"
            jedis.sadd(redisOrderDetailKey, JSON.toJSONString(orderDetail, new SerializeConfig(true)))
            jedis.expire(redisOrderDetailKey, 24 * 3600)
          }
        }
      }
      jedis.close()
      orderWideList.iterator
    })

    //    orderWideDStream.cache()
    //    orderWideDStream.print(1000)

    // write into OLAP (ElasticSearch)
    orderWideDStream.foreachRDD(rdd => {
      rdd.foreachPartition(orderWideIter => {
        val orderWideList = orderWideIter.map(orderWide => (orderWide.detail_id.toString, orderWide)).toList
        if (orderWideList.nonEmpty) {
          val (owId, owObj) = orderWideList.head
          val indexName = s"gmall_order_wide_${owObj.create_date}"
          println(indexName)
          EsUtils.bulkSave(indexName, orderWideList)
        } else {
          println("empty stream")
        }
      })
      OffsetsUtils.saveOffsets(orderInfoTopicName, orderInfoGroup, orderInfoOffsets)
      OffsetsUtils.saveOffsets(orderDetailTopicName, orderDetailGroup, orderDetailOffsets)
    })

    ssc.start()
    ssc.awaitTermination()
  }


}
