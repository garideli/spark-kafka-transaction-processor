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

## Sample Data

Below is an example of the input and transformed data:

### Input Data:
| transaction_id | user_id | amount | status  | timestamp           |
|----------------|---------|--------|---------|---------------------|
| TX12345        | U1001   | 250.75 | SUCCESS | 2024-12-05T10:00:00Z |
| TX12346        | U1002   | 150.5  | FAILED  | 2024-12-05T10:05:00Z |
| TX12347        | U1003   | 300.0  | SUCCESS | 2024-12-05T10:10:00Z |
| TX12348        | U1004   | 50.25  | PENDING | 2024-12-05T10:15:00Z |

### Transformed Data:
| transaction_id | user_id | amount | status  | timestamp           | discounted_amount |
|----------------|---------|--------|---------|---------------------|-------------------|
| TX12345        | U1001   | 250.75 | SUCCESS | 2024-12-05T10:00:00Z | 225.675           |
| TX12347        | U1003   | 300.0  | SUCCESS | 2024-12-05T10:10:00Z | 270.0             |
| TX12349        | U1005   | 450.0  | SUCCESS | 2024-12-05T10:20:00Z | 405.0             |

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
