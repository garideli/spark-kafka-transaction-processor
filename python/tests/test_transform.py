from __future__ import annotations

from datetime import datetime

import pytest

from skproc.config import Config
from skproc.pipeline import transform_transactions
from skproc.source import TRANSACTION_SCHEMA


def test_transform_filters_success_and_computes_features(spark):
    cfg = Config(discount_rules={"grocery": 0.10, "luxury": 0.20}, fraud_threshold=0.65)
    rows = [
        ("txn_1", "user_1", 100.0, "USD", "SUCCESS", "grocery", "mobile", "US", datetime(2026, 1, 1, 9), 0.0),
        ("txn_2", "user_2", 250.0, "USD", "FAILED", "grocery", "web", "US", datetime(2026, 1, 1, 9, 1), 0.0),
        ("txn_3", "user_3", 900.0, "USD", "SUCCESS", "luxury", "api", "GB", datetime(2026, 1, 1, 9, 2), 1.0),
    ]
    df = spark.createDataFrame(rows, TRANSACTION_SCHEMA)

    result = transform_transactions(df, cfg).orderBy("transaction_id").collect()

    assert [row["transaction_id"] for row in result] == ["txn_1", "txn_3"]
    assert result[0]["net_amount"] == pytest.approx(90.0)
    assert result[1]["net_amount"] == pytest.approx(720.0)
    assert all(0.0 <= row["fraud_score"] <= 1.0 for row in result)
    assert result[1]["is_flagged"] is True
