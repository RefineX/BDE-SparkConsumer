import org.apache.spark.sql.SparkSession

import java.util.Properties
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{StringType, StructType}

import scala.collection.JavaConverters._

object Main {
  def main(args: Array[String]): Unit = {

    // Initialize Spark Session
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Spark Word Count")
      .getOrCreate()
    // Create kafka dataframe
    var df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "myTopic")
      .option("startingOffsets", "earliest")
      .load()
    // Print dataframe schema
    df.printSchema()
    // Select value from dataframe
    df = df.selectExpr("CAST(value AS STRING)")
    // Define schema of value
    val json_schema = new StructType()
      .add("id", StringType)
      .add("first_name", StringType)
      .add("last_name", StringType)
      .add("email", StringType)
      .add("gender", StringType)
      .add("ip_address", StringType)
    // Apply schema to value
    df = df.select(from_json(col("value"), json_schema).as("data")).select("data.*")
    // Write dataframe to output
    df.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

  }
}