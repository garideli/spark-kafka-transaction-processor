package com.example.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object TransactionProcessor extends App {
  val spark = SparkSession.builder
    .appName("TransactionProcessor")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("WARN")

  val kafkaBrokers = "localhost:9092"
  val inputTopic = "input-topic"
  val outputTopic = "output-topic"

  // Read from Kafka
  val transactionsDF = spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafkaBrokers)
    .option("subscribe", inputTopic)
    .option("startingOffsets", "earliest")
    .load()
    .selectExpr("CAST(value AS STRING) as json")
    .select(from_json(col("json"), schema = new org.apache.spark.sql.types.StructType()
      .add("transaction_id", "string")
      .add("user_id", "string")
      .add("amount", "double")
      .add("status", "string")
      .add("timestamp", "string")).alias("data"))
    .select("data.*")

  // Print input DataFrame
  println("Displaying the input DataFrame (transactionsDF):")
  transactionsDF.writeStream
    .outputMode("append")
    .format("console")
    .start()

  // Transformation: Calculate discounted amount for successful transactions
  val transformedDF = transactionsDF
    .filter(col("status") === "SUCCESS")
    .withColumn("discounted_amount", col("amount") * 0.9)

  // Print transformed DataFrame
  println("Displaying the transformed DataFrame (transformedDF):")
  transformedDF.writeStream
    .outputMode("append")
    .format("console")
    .start()

  // Write transformed data to output-topic
  transformedDF.selectExpr("to_json(struct(*)) AS value")
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafkaBrokers)
    .option("topic", outputTopic)
    .option("checkpointLocation", "/tmp/checkpoints")
    .start()

  // Commented out the shutdown hook
  /*
  sys.ShutdownHookThread {
    println("Stopping Spark Streaming gracefully...")
    spark.stop()
    println("Stopped Spark Streaming.")
  }
  */

  spark.streams.awaitAnyTermination()
}