"""Streaming source abstraction: offline file source or real Kafka.

Structured Streaming treats a directory of files and a Kafka topic almost
identically -- both are append-only, offset-tracked sources. This module exploits
that so the pipeline runs end to end with **no broker** by default (reading JSON
files that a producer drops into a watched directory), while the *real* Kafka
reader is one config flag away. Calling code never changes:

    df = read_stream(spark, cfg)   # a streaming DataFrame either way

Set ``SOURCE=kafka`` (plus ``KAFKA_BOOTSTRAP`` and ``KAFKA_TOPIC``) to switch to a
live broker; the default ``SOURCE=file`` keeps everything offline and reproducible.
"""
from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.types import (
    DoubleType, StringType, StructField, StructType, TimestampType,
)

from .logging_utils import get_logger

log = get_logger("source")

# The canonical transaction event schema. Declaring it explicitly (never
# inferring) is essential in streaming: schema inference is not allowed on a
# streaming file source, and a fixed schema prevents silent drift.
TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("currency", StringType(), True),
    StructField("status", StringType(), True),
    StructField("merchant_category", StringType(), True),
    StructField("channel", StringType(), True),
    StructField("country", StringType(), True),
    StructField("event_time", TimestampType(), False),
    StructField("is_fraud", DoubleType(), True),
])


def read_stream(spark: SparkSession, cfg) -> DataFrame:
    """Return a streaming DataFrame of transaction events.

    ``SOURCE=file`` (default): a streaming JSON file source over ``cfg.stream_dir``
    with the fixed schema and a bounded ``maxFilesPerTrigger`` so micro-batches
    resemble Kafka poll batches.

    ``SOURCE=kafka``: reads ``KAFKA_TOPIC`` from ``KAFKA_BOOTSTRAP`` and parses the
    JSON ``value`` payload with the same schema. Requires the spark-sql-kafka
    package at submit time; only attempted when explicitly selected.
    """
    source = os.environ.get("SOURCE", "file").lower()
    if source == "kafka":
        bootstrap = os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
        topic = os.environ.get("KAFKA_TOPIC", "transactions")
        log.info("reading from Kafka topic=%s bootstrap=%s", topic, bootstrap)
        raw = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load()
        )
        return (
            raw.select(F.from_json(F.col("value").cast("string"), TRANSACTION_SCHEMA).alias("j"))
            .select("j.*")
        )

    log.info("reading from offline file source dir=%s (no broker)", cfg.stream_dir)
    return (
        spark.readStream.schema(TRANSACTION_SCHEMA)
        .option("maxFilesPerTrigger", 4)
        .json(str(cfg.stream_dir))
    )
