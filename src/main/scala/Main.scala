import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{IntegerType, StringType, TimestampType, DoubleType, StructType}

object Main {
  def main(args: Array[String]): Unit = {

    // Initialize Spark Session
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Bank Transaction Consumer")
      .getOrCreate()

    // Create Kafka DataFrame
    var df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "transactions")
      .option("startingOffsets", "earliest")
      .load()

    // Print DataFrame schema
    df.printSchema()

    // Select value from DataFrame
    df = df.selectExpr("CAST(value AS STRING)")

    // Define schema of value
    val json_schema = new StructType()
      .add("transaction_id", StringType)
      .add("account_id", IntegerType)
      .add("branch_id", IntegerType)
      .add("channel_id", IntegerType)
      .add("transaction_timestamp", TimestampType)
      .add("transaction_amount", DoubleType)

    // Apply schema to value
    df = df.select(from_json(col("value"), json_schema).as("data")).select("data.*")

    // Write DataFrame to output
    df.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

  }
}