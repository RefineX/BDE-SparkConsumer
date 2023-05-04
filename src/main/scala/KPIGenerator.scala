import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.time.LocalDateTime

object KPIGenerator {
  def main(args: Array[String]): Unit = {

    // Initialize Spark Session
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Bank Transaction Consumer")
      .getOrCreate()

    // Read dimension tables
    val accountDF = spark.read.option("header", "true").csv("DimensionTables/account_dimension.csv")
    val customerDF = spark.read.option("header", "true").csv("DimensionTables/customer_dimension.csv")
    val branchDF = spark.read.option("header", "true").csv("DimensionTables/branch_dimension.csv")
    val channelDF = spark.read.option("header", "true").csv("DimensionTables/channel_dimension.csv")

    // Create Kafka DataFrame
    var df = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "transactions")
      .option("startingOffsets", "earliest")
      .load()

    // Define schema of value
    val json_schema = new StructType()
      .add("transaction_id", StringType)
      .add("account_id", IntegerType)
      .add("branch_id", IntegerType)
      .add("channel_id", IntegerType)
      .add("transaction_timestamp", TimestampType)
      .add("transaction_amount", DoubleType)

    df = df.selectExpr("CAST(value AS STRING)")

    // Apply schema to value
    df = df.select(from_json(col("value"), json_schema).as("data")).select("data.*")

    // Add watermarking to accommodate for potentially late records
    df = df.withWatermark("transaction_timestamp", "1 second")

    // Join DataFrames
    df = df.join(accountDF, df("account_id") === accountDF("account_id"))
      .join(branchDF, df("branch_id") === branchDF("branch_id"))
      .join(channelDF, df("channel_id") === channelDF("channel_id"))
      .join(customerDF, accountDF("customer_id") === customerDF("customer_id"))
      .drop(accountDF("account_id"))
      .drop(branchDF("branch_id"))
      .drop(channelDF("channel_id"))
      .drop(customerDF("customer_id"))

    // Calculate KPIs
    val accountStats = df.groupBy(df("account_id"),df("account_type"),df("account_open_date"),window(df("transaction_timestamp"),"1 day","1 day").alias("time_window")).agg(
      count("*").alias("total_transactions"),
      sum("transaction_amount").alias("total_amount"),
      avg("transaction_amount").alias("avg_transaction_amount")
    ).withColumn("start_time", date_format(col("time_window.start"), "yyyy-MM-dd HH:mm:ss"))
     .withColumn("end_time", date_format(col("time_window.end"), "yyyy-MM-dd HH:mm:ss"))

    val branchStats = df.groupBy(df("branch_id"),df("branch_name"),df("branch_city"),window(df("transaction_timestamp"),"1 day","1 day").alias("time_window")).agg(
      count("*").alias("total_transactions"),
      sum("transaction_amount").alias("total_amount"),
      avg("transaction_amount").alias("avg_transaction_amount")
    ).withColumn("start_time", date_format(col("time_window.start"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("end_time", date_format(col("time_window.end"), "yyyy-MM-dd HH:mm:ss"))

    val channelStats = df.groupBy(df("channel_id"),df("channel_name"),window(df("transaction_timestamp"),"1 day","1 day").alias("time_window")).agg(
      count("*").alias("total_transactions"),
      sum("transaction_amount").alias("total_amount"),
      avg("transaction_amount").alias("avg_transaction_amount")
    ).withColumn("start_time", date_format(col("time_window.start"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("end_time", date_format(col("time_window.end"), "yyyy-MM-dd HH:mm:ss"))

    val customerStats = df.groupBy(df("customer_id"),df("customer_name"),df("customer_email"),window(df("transaction_timestamp"),"1 day","1 day").alias("time_window")).agg(
      count("*").alias("total_transactions"),
      sum("transaction_amount").alias("total_amount"),
      avg("transaction_amount").alias("avg_transaction_amount")
    ).withColumn("start_time", date_format(col("time_window.start"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("end_time", date_format(col("time_window.end"), "yyyy-MM-dd HH:mm:ss"))
    println(LocalDateTime.now()+" Stats initiated")

    // Start the queries
    accountStats
      .withColumn("time_window",col("time_window").cast("string"))
      .writeStream.format("memory")
      .option("header","true").outputMode("complete")
      .queryName("accountStats").start()

    branchStats
      .withColumn("time_window", col("time_window").cast("string"))
      .writeStream.format("memory")
      .option("header", "true").outputMode("complete")
      .queryName("branchStats").start()

    channelStats
      .withColumn("time_window", col("time_window").cast("string"))
      .writeStream.format("memory")
      .option("header", "true").outputMode("complete")
      .queryName("channelStats").start()

    customerStats
      .withColumn("time_window", col("time_window").cast("string"))
      .writeStream.format("memory")
      .option("header", "true").outputMode("complete")
      .queryName("customerStats").start()

    print("Waiting...")
    spark.streams.awaitAnyTermination(180000)
    println("Done.")

    // Write queries to CSVs
    spark.sql("select * from accountStats")
       .coalesce(1)
       .write.option("header", "true").csv("output/account_stats")
    println(LocalDateTime.now()+" Account Stats wrote to CSV")
    spark.sql("select * from branchStats")
      .coalesce(1)
      .write.option("header", "true").csv("output/branch_stats")
    println(LocalDateTime.now()+" Branch Stats wrote to CSV")
    spark.sql("select * from channelStats")
      .coalesce(1)
      .write.option("header", "true").csv("output/channel_stats")
    println(LocalDateTime.now()+" Channel Stats wrote to CSV")
    spark.sql("select * from customerStats")
      .coalesce(1)
      .write.option("header", "true").csv("output/customer_stats")
    println(LocalDateTime.now()+" Customer Stats wrote to CSV")
  }
}