import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import java.time.Duration
import java.util.Properties
import scala.jdk.CollectionConverters._
object Main extends App {

  val props:Properties = new Properties()
  props.put(ConsumerConfig.GROUP_ID_CONFIG, "test")
  props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092")
  props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringDeserializer")
  props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
    "org.apache.kafka.common.serialization.StringDeserializer")

  val consumer = new KafkaConsumer[String, String](props)
  val topics = Seq("another_topic")

  def wordCounter(input: String): Map[String, Int] = {
    input.split("\\W+").toSeq.groupBy(identity).map(t => (t._1, t._2.length))
  }

  try {
    consumer.subscribe(topics.asJava)
    while (true) {
      val records = consumer.poll(Duration.ofMillis(100))
      for (record <- records.asScala) {
        wordCounter(record.value()).foreach {
          case (word -> count) => println((s"in a record number: '${record.key()}' '$word' occurs $count times."))
        }
      }
    }
  }catch{
    case e:Exception => e.printStackTrace()
  }finally {
    consumer.close()
  }
}
