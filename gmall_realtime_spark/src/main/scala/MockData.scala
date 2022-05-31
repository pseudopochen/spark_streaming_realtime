import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import java.util.Properties
import scala.collection.mutable.ListBuffer
import scala.util.Random

object MockData {
  def main(args: Array[String]): Unit = {

    val prop = new Properties()
    prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])

    val producer = new KafkaProducer[String, String](prop)

    while (true) {
      mockData().foreach(
        data => {
          producer.send(new ProducerRecord[String, String]("mock", data))
          println(data)
        }
      )
      Thread.sleep(2000)
    }
  }

  def mockData(): ListBuffer[String] = {
    val list = ListBuffer[String]()
    val areaList = List[String]("East", "North", "South")
    val cityList = List[String]("Boston", "Seattle", "Houston")

    for (i <- 1 to 30) {
      val area = areaList(new Random().nextInt(areaList.length))
      val city = cityList(new Random().nextInt(cityList.length))
      val userid = new Random().nextInt(6) + 1
      val adid = new Random().nextInt(6) + 1
      list.append(s"${System.currentTimeMillis()} $area $city $userid $adid")
    }
    list
  }
}
