package utils

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange
import utils.RedisUtil.getJedisFromPool

import java.util
import scala.collection.mutable

/**
 * Control the commit of consumer offsets to Kafka
 * to implement "at-least-once"
 *
 * SparkStreaming has its own offset management:
 * (a) default is 5-s periodic, cannot guarantee "at-least-once"
 * (b) manual commit for only InputDstream[ConsumerRecord] type, not applicable to our own json obj stream
 */
object OffsetsUtils {

  /**
   * Save offsets into redis
   * @param topic
   * @param groupId
   * @param offsetRanges
   */
  def saveOffsets(topic: String, groupId: String, offsetRanges: Array[OffsetRange]): Unit = {
    if (offsetRanges != null && offsetRanges.length > 0) {
      val offsets = new util.HashMap[String, String]()
      for (offsetRange <- offsetRanges) {
        offsets.put(offsetRange.partition.toString, offsetRange.untilOffset.toString)
      }
      //println("save offsets: " + offsets)

      // write into redis
      val redisKey = s"offsets:$topic:$groupId"
      val jedis = getJedisFromPool
      jedis.hset(redisKey, offsets)
      jedis.close()
    }
  }

  /**
   * Read offsets from redis and return a map for getKafkaDStream
   * @param topic
   * @param groupId
   * @return
   */
  def readOffsets(topic:String, groupId:String) : Map[TopicPartition, Long] = {
    val redisKey = s"offsets:$topic:$groupId"
    val jedis = getJedisFromPool()
    val offsets = jedis.hgetAll(redisKey)
    jedis.close()
    //println("read offsets: " + offsets)

    val res = mutable.Map[TopicPartition, Long]()
    import scala.collection.JavaConverters._
    for((partition, offset) <- offsets.asScala) {
      val tp = new TopicPartition(topic, partition.toInt)
      res.put(tp, offset.toLong)
    }

    res.toMap
  }

}
