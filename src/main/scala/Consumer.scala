object Consumer {

  import java.util.Properties
  import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
  import org.apache.kafka.common.serialization.StringDeserializer
  import scala.collection.JavaConverters._

  import org.apache.kafka.clients.consumer.KafkaConsumer

  def main(args: Array[String]): Unit = {

    // Initialize consumer properties
    val kafkaConsumerProps: Properties = {
      val props = new Properties()
      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
      props.put(ConsumerConfig.GROUP_ID_CONFIG, "myGroupId")
      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
      props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      props
    }

    val consumer = new KafkaConsumer[String, String](kafkaConsumerProps)

    consumer.subscribe(java.util.Collections.singletonList("myTopic"))
    // Poll for messages
    while (true) {
      val records = consumer.poll(java.time.Duration.ofMillis(100))

      for (record <- records.asScala) {
        val key = record.key()
        val value = record.value()
        println(s"Received message with key: $key and value: $value")
      }
    }
  }
}