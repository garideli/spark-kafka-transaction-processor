from __future__ import annotations

from pyspark.sql import functions as F

from skproc.config import Config
from skproc.generate import generate_transactions
from skproc.pipeline import run_streaming_pipeline


def test_pipeline_end_to_end_available_now(spark, tmp_path):
    cfg = Config(root_dir=tmp_path, n_events=2_000, seed=11, batch_size=250)
    generated = generate_transactions(cfg)

    stats = run_streaming_pipeline(spark, cfg)

    assert stats["source_events"] == len(generated)
    assert cfg.enriched_dir.exists()
    assert cfg.windowed_dir.exists()

    enriched = spark.read.parquet(str(cfg.enriched_dir))
    windowed = spark.read.parquet(str(cfg.windowed_dir))
    enriched_count = enriched.count()
    window_count = windowed.count()

    assert 0 < enriched_count <= len(generated)
    assert stats["filtered_count"] == len(generated) - enriched_count
    assert window_count > 0
    bounds = windowed.agg(F.min("fraud_rate").alias("min"), F.max("fraud_rate").alias("max")).collect()[0]
    assert 0.0 <= bounds["min"] <= 1.0
    assert 0.0 <= bounds["max"] <= 1.0
