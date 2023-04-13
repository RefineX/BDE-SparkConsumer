import org.apache.spark.sql.SparkSession
import java.util.Properties
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import scala.collection.JavaConverters._

object Main {
  def main(args: Array[String]): Unit = {
    println("Hello world!")

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Spark Word Count")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    val lines = spark.sparkContext.parallelize(
      Seq("Spark Intellij Idea Scala test one",
        "Spark Intellij Idea Scala test two",
        "Spark Intellij Idea Scala test three"))

    val counts = lines
      .flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    counts.foreach(println)

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