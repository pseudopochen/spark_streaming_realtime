package utils

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}

import java.util.Properties
import scala.collection.mutable

/**
 * Kafka tools for producing and consuming Kafka data
 */
object KafkaTools {

  /**
   * Consumer config
   */
  private val consumerConfigs = mutable.Map[String, Object](
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> PropsUtil(Config.KAFKA_BOOTSTRAP_SERVERS), //"localhost:9092",
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],

    // offset commit
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
    //ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG -> "5000"

    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest"
  )

  /**
   * Create consumer and get Kafka DStream using default offsets
   *
   * @param ssc
   * @param topic
   * @param groupID
   * @return
   */
  def getKafkaDStream(ssc: StreamingContext, topic: String, groupID: String) = {

    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupID)

    KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfigs)
    )
  }

  /**
   * Create consumer and get Kafka DStream with offset input
   *
   * @param ssc
   * @param topic
   * @param groupID
   * @param offsets
   * @return
   */
  def getKafkaDStream(ssc: StreamingContext, topic: String, groupID: String, offsets: Map[TopicPartition, Long]) = {

    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupID)

    KafkaUtils.createDirectStream(ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(topic), consumerConfigs, offsets)
    )
  }

  /**
   * Get Kafka DStream with offsets
   * @param ssc
   * @param topic
   * @param groupID
   * @return (offsetRangesDStream, offsetRanges)
   */

  def getKafkaDStreamWithOffsets(ssc:StreamingContext, topic:String, groupID:String) = {

    var kafkaDStream :  InputDStream[ConsumerRecord[String, String]] = null

    val offsets = OffsetsUtils.readOffsets(topic, groupID)
    if (offsets != null && offsets.nonEmpty) {
      kafkaDStream = getKafkaDStream(ssc, topic, groupID, offsets)
    } else {
      kafkaDStream = getKafkaDStream(ssc, topic, groupID)
    }

    var offsetRanges : Array[OffsetRange] = null
    val offsetRangesDStream = kafkaDStream.transform(rdd => {
      offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd
    })

    (offsetRangesDStream, offsetRanges)
  }


  val producer: KafkaProducer[String, String] = createProducer()

  /**
   * Create producer
   *
   * @return
   */
  def createProducer(): KafkaProducer[String, String] = {
    if (producer != null) {
      return producer
    }

    val prop = new Properties()
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, PropsUtil(Config.KAFKA_BOOTSTRAP_SERVERS)) //"localhost:9092")
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    prop.put(ProducerConfig.ACKS_CONFIG, "all")
    // batch.size
    // linger.ms
    // retries
    prop.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")

    new KafkaProducer[String, String](prop)
  }

  /**
   * Send message to the topic, using default (sticky) partitioner
   *
   * @param topic
   * @param msg
   * @return
   */
  def send(topic: String, msg: String): Unit = {
    producer.send(new ProducerRecord[String, String](topic, msg))
  }

  /**
   * Send message to the topic, using key for partitioner
   *
   * @param topic
   * @param key
   * @param msg
   */
  def send(topic: String, key: String, msg: String): Unit = {
    producer.send(new ProducerRecord[String, String](topic, key, msg))
  }

  /**
   * Flush data from producer memory buffer to disk
   */
  def flush() : Unit = {
    producer.flush()
  }

  /**
   * Close the producer
   */
  def close(): Unit = {
    if (producer != null)
      producer.close()
  }

}
