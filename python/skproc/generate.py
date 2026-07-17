"""Synthetic transaction event generator for the offline stream source."""
from __future__ import annotations

import json
import shutil
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from .config import Config
from .logging_utils import get_logger

log = get_logger("generate")

MERCHANT_CATEGORIES = np.array(
    ["grocery", "travel", "electronics", "dining", "fuel", "entertainment", "pharmacy", "luxury"]
)
CHANNELS = np.array(["mobile", "web", "pos", "api"])
COUNTRIES = np.array(["US", "CA", "GB", "DE", "FR", "JP", "BR", "IN"])
CURRENCIES = np.array(["USD", "USD", "USD", "EUR", "GBP", "JPY", "CAD"])
STATUSES = np.array(["SUCCESS", "FAILED", "PENDING"])


def generate_transactions(cfg: Config) -> pd.DataFrame:
    """Create synthetic transactions, stream JSON files, and a full Parquet copy."""
    cfg.ensure()
    rng = np.random.default_rng(cfg.seed)
    n_events = int(cfg.n_events)
    if n_events <= 0:
        raise ValueError("n_events must be positive")

    users = np.array([f"user_{idx:05d}" for idx in range(1, cfg.n_users + 1)])
    event_offsets = np.sort(rng.integers(0, 3 * 60 * 60, size=n_events))
    base_time = datetime(2026, 1, 15, 9, 0, 0)
    event_times = [base_time + timedelta(seconds=int(offset)) for offset in event_offsets]

    categories = rng.choice(
        MERCHANT_CATEGORIES,
        size=n_events,
        p=np.array([0.18, 0.12, 0.13, 0.18, 0.10, 0.11, 0.10, 0.08]),
    )
    channels = rng.choice(CHANNELS, size=n_events, p=np.array([0.45, 0.28, 0.22, 0.05]))
    countries = rng.choice(COUNTRIES, size=n_events, p=np.array([0.72, 0.07, 0.05, 0.04, 0.03, 0.03, 0.03, 0.03]))
    statuses = rng.choice(STATUSES, size=n_events, p=np.array([0.88, 0.08, 0.04]))

    base_amounts = rng.lognormal(mean=4.15, sigma=0.78, size=n_events)
    category_multiplier = np.select(
        [
            categories == "travel",
            categories == "electronics",
            categories == "luxury",
            categories == "fuel",
            categories == "pharmacy",
        ],
        [2.0, 1.65, 2.8, 0.9, 0.7],
        default=1.0,
    )
    amounts = np.round(np.clip(base_amounts * category_multiplier, 4.0, 4_500.0), 2)

    fraud_probability = (
        0.012
        + (amounts > 300.0) * 0.035
        + (amounts > 800.0) * 0.095
        + np.isin(categories, ["electronics", "luxury"]) * 0.035
        + np.isin(channels, ["web", "api"]) * 0.025
        + (countries != "US") * 0.025
    )
    fraud_probability = np.clip(fraud_probability, 0.005, 0.45)
    is_fraud = rng.binomial(1, fraud_probability).astype(float)
    amounts = np.where(is_fraud == 1.0, np.round(amounts * rng.uniform(1.15, 2.2, n_events), 2), amounts)

    df = pd.DataFrame(
        {
            "transaction_id": [f"txn_{cfg.seed}_{idx:07d}" for idx in range(n_events)],
            "user_id": rng.choice(users, size=n_events),
            "amount": amounts.astype(float),
            "currency": rng.choice(CURRENCIES, size=n_events),
            "status": statuses,
            "merchant_category": categories,
            "channel": channels,
            "country": countries,
            "event_time": pd.to_datetime(event_times),
            "is_fraud": is_fraud.astype(float),
        }
    )

    _write_stream_files(df, cfg.stream_dir, cfg.batch_size)
    _write_full_parquet(df, cfg.full_data_path)
    log.info(
        "generated %s events into %s and %s",
        len(df),
        cfg.stream_dir,
        cfg.full_data_path,
    )
    return df


def _write_stream_files(df: pd.DataFrame, stream_dir: Path, batch_size: int) -> None:
    if stream_dir.exists():
        shutil.rmtree(stream_dir)
    stream_dir.mkdir(parents=True, exist_ok=True)

    batch_size = max(1, int(batch_size))
    for batch_idx, start in enumerate(range(0, len(df), batch_size)):
        batch = df.iloc[start : start + batch_size]
        path = stream_dir / f"transactions_{batch_idx:05d}.json"
        with path.open("w", encoding="utf-8") as handle:
            for record in batch.to_dict(orient="records"):
                record["event_time"] = record["event_time"].isoformat()
                handle.write(json.dumps(record, separators=(",", ":")) + "\n")


def _write_full_parquet(df: pd.DataFrame, full_data_path: Path) -> None:
    if full_data_path.exists():
        if full_data_path.is_dir():
            shutil.rmtree(full_data_path)
        else:
            full_data_path.unlink()
    full_data_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(
        full_data_path,
        index=False,
        engine="pyarrow",
        coerce_timestamps="us",
        allow_truncated_timestamps=True,
    )
