"""PySpark Structured Streaming pipeline."""
from __future__ import annotations

import time
import os
import sys
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession, functions as F
from pyspark.sql.streaming import StreamingQuery

from .config import Config
from .logging_utils import get_logger
from .source import read_stream

log = get_logger("pipeline")


def create_spark_session(app_name: str = "TransactionProcessorPySpark", master: str = "local[*]") -> SparkSession:
    """Create a local Spark session matching the project constraints."""
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    spark = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def transform_transactions(df: DataFrame, cfg: Config) -> DataFrame:
    """Filter successful transactions and add discount and fraud features."""
    discount_lookup = F.create_map(
        *[
            item
            for category, discount in cfg.discount_rules.items()
            for item in (F.lit(category), F.lit(float(discount)))
        ]
    )
    discount_rate = F.coalesce(discount_lookup[F.col("merchant_category")], F.lit(0.0))
    high_amount = F.when(F.col("amount") >= F.lit(cfg.high_value_amount), F.lit(0.20)).when(
        F.col("amount") >= F.lit(cfg.high_value_amount * 0.55), F.lit(0.10)
    ).otherwise(F.lit(0.02))
    risky_category = F.when(F.col("merchant_category").isin("electronics", "luxury"), F.lit(0.11)).otherwise(F.lit(0.02))
    risky_channel = F.when(F.col("channel").isin("web", "api"), F.lit(0.09)).otherwise(F.lit(0.025))
    cross_border = F.when(F.col("country") != F.lit("US"), F.lit(0.07)).otherwise(F.lit(0.015))
    latent_signal = F.coalesce(F.col("is_fraud"), F.lit(0.0)) * F.lit(0.46)
    fraud_score = F.least(
        F.lit(1.0),
        F.greatest(
            F.lit(0.0),
            F.lit(0.03) + high_amount + risky_category + risky_channel + cross_border + latent_signal,
        ),
    )

    return (
        df.withColumn("status", F.upper(F.col("status")))
        .filter(F.col("status") == F.lit("SUCCESS"))
        .withColumn("gross_amount", F.col("amount").cast("double"))
        .withColumn("discount_rate", discount_rate.cast("double"))
        .withColumn("net_amount", F.col("gross_amount") * (F.lit(1.0) - F.col("discount_rate")))
        .withColumn("fraud_score", fraud_score.cast("double"))
        .withColumn("is_flagged", F.col("fraud_score") >= F.lit(float(cfg.fraud_threshold)))
        .select(
            "transaction_id",
            "user_id",
            "event_time",
            "currency",
            "country",
            "merchant_category",
            "channel",
            "status",
            "gross_amount",
            "discount_rate",
            "net_amount",
            "is_fraud",
            "fraud_score",
            "is_flagged",
        )
    )


def windowed_aggregates(df: DataFrame, cfg: Config) -> DataFrame:
    """Aggregate enriched transactions into event-time windows."""
    return (
        df.withWatermark("event_time", cfg.watermark_delay)
        .groupBy(F.window("event_time", cfg.window_duration))
        .agg(
            F.count("*").alias("event_count"),
            F.round(F.sum("gross_amount"), 2).alias("gross_value"),
            F.round(F.sum("net_amount"), 2).alias("net_value"),
            F.sum(F.col("is_flagged").cast("int")).cast("long").alias("flagged_count"),
            F.avg(F.col("is_flagged").cast("double")).alias("fraud_rate"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "event_count",
            "gross_value",
            "net_value",
            "flagged_count",
            "fraud_rate",
        )
    )


def run_streaming_pipeline(spark: SparkSession, cfg: Config) -> dict[str, Any]:
    """Run both availableNow streaming sinks and return measured run stats."""
    cfg.ensure()
    start = time.perf_counter()

    input_df = read_stream(spark, cfg)
    enriched_df = transform_transactions(input_df, cfg)

    log.info("starting enriched availableNow query")
    enriched_query = (
        enriched_df.writeStream.format("parquet")
        .option("path", str(cfg.enriched_dir))
        .option("checkpointLocation", str(cfg.checkpoint_dir / "enriched"))
        .outputMode("append")
        .trigger(availableNow=True)
        .start()
    )
    _await(enriched_query)

    log.info("starting windowed availableNow query")
    windowed_df = windowed_aggregates(transform_transactions(read_stream(spark, cfg), cfg), cfg)
    windowed_query = (
        windowed_df.writeStream.format("parquet")
        .option("path", str(cfg.windowed_dir))
        .option("checkpointLocation", str(cfg.checkpoint_dir / "windowed"))
        .outputMode("append")
        .trigger(availableNow=True)
        .start()
    )
    _await(windowed_query)

    duration = time.perf_counter() - start
    stats = collect_output_stats(spark, cfg)
    stats["pipeline_duration_seconds"] = round(duration, 3)
    stats["enriched_query_id"] = str(enriched_query.id)
    stats["windowed_query_id"] = str(windowed_query.id)
    return stats


def collect_output_stats(spark: SparkSession, cfg: Config) -> dict[str, Any]:
    """Read produced Parquet outputs and summarize them."""
    source_events = _source_event_count(spark, cfg)
    status_counts = _status_counts(spark, cfg)
    enriched_count = 0
    flagged_count = 0
    total_gross_value = 0.0
    total_net_value = 0.0
    min_event_time = None
    max_event_time = None
    if _has_parquet(cfg.enriched_dir):
        enriched = spark.read.parquet(str(cfg.enriched_dir))
        summary = enriched.agg(
            F.count("*").alias("rows"),
            F.sum(F.col("is_flagged").cast("int")).alias("flagged"),
            F.sum("gross_amount").alias("gross"),
            F.sum("net_amount").alias("net"),
            F.min("event_time").alias("min_ts"),
            F.max("event_time").alias("max_ts"),
        )
        row = summary.select(
            "rows",
            "flagged",
            "gross",
            "net",
            F.date_format("min_ts", "yyyy-MM-dd'T'HH:mm:ss").alias("min_event_time"),
            F.date_format("max_ts", "yyyy-MM-dd'T'HH:mm:ss").alias("max_event_time"),
        ).collect()[0]
        enriched_count = int(row["rows"] or 0)
        flagged_count = int(row["flagged"] or 0)
        total_gross_value = float(row["gross"] or 0.0)
        total_net_value = float(row["net"] or 0.0)
        min_event_time = row["min_event_time"]
        max_event_time = row["max_event_time"]

    window_count = 0
    max_window_fraud_rate = 0.0
    if _has_parquet(cfg.windowed_dir):
        windowed = spark.read.parquet(str(cfg.windowed_dir))
        row = windowed.agg(
            F.count("*").alias("rows"),
            F.max("fraud_rate").alias("max_fraud_rate"),
        ).collect()[0]
        window_count = int(row["rows"] or 0)
        max_window_fraud_rate = float(row["max_fraud_rate"] or 0.0)

    filtered_count = max(source_events - enriched_count, 0)
    return {
        "source_events": source_events,
        "enriched_events": enriched_count,
        "filtered_count": filtered_count,
        "filtered_pct": (filtered_count / source_events) if source_events else 0.0,
        "window_count": window_count,
        "flagged_count": flagged_count,
        "flagged_rate": (flagged_count / enriched_count) if enriched_count else 0.0,
        "total_gross_value": total_gross_value,
        "total_net_value": total_net_value,
        "discount_value": total_gross_value - total_net_value,
        "max_window_fraud_rate": max_window_fraud_rate,
        "status_counts": status_counts,
        "min_event_time": min_event_time,
        "max_event_time": max_event_time,
    }


def _await(query: StreamingQuery) -> None:
    query.awaitTermination()
    error = query.exception()
    if error is not None:
        raise error


def _source_event_count(spark: SparkSession, cfg: Config) -> int:
    if cfg.full_data_path.exists():
        return int(spark.read.parquet(str(cfg.full_data_path)).count())
    return int(cfg.n_events)


def _status_counts(spark: SparkSession, cfg: Config) -> dict[str, int]:
    if not cfg.full_data_path.exists():
        return {}
    rows = (
        spark.read.parquet(str(cfg.full_data_path))
        .groupBy("status")
        .count()
        .collect()
    )
    return {str(row["status"]): int(row["count"]) for row in rows}


def _has_parquet(path: Path) -> bool:
    return path.exists() and any(path.glob("*.parquet"))
