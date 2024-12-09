# Streaming Data Pipeline

This project demonstrates a Spark Structured Streaming application for processing transactions from Kafka, applying transformations, and writing the results back to Kafka.

## Features
- Reads transactions from a Kafka topic.
- Filters successful transactions.
- Calculates discounted amounts.
- Outputs transformed data to another Kafka topic.

## Project Structure
.
├── src
│   └── main
│       └── scala
│           └── com.example.streaming
│               └── TransactionProcessor.scala
├── scripts
│   └── kafka-commands
├── build.sbt
└── README.md

## Prerequisites
- Apache Spark
- Apache Kafka
- Scala

## How to Run
1. Update Kafka configurations in `TransactionProcessor.scala`.
2. Compile the project:
   ```bash
   sbt compile
   
	3.	Run the application:
   sbt run
   