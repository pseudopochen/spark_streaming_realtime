import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Req3 {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Req3")
    val ssc = new StreamingContext(conf, Seconds(5))

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

    val reducedRDD = adClickData.map(
      d => {
        val newTS = d.ts.toLong / 10000 * 10000
        (newTS, 1)
      }
    ).reduceByKeyAndWindow((x: Int, y: Int) => {
      x + y
    }, Seconds(60), Seconds(10))

    reducedRDD.print()

    ssc.start()
    ssc.awaitTermination()
  }

  case class AdClickData(ts: String, area: String, city: String, user: String, ad: String)
}
