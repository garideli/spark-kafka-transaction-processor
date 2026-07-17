"""Runtime configuration for the offline Structured Streaming pipeline."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

DEFAULT_DISCOUNT_RULES = {
    "grocery": 0.03,
    "travel": 0.05,
    "electronics": 0.08,
    "dining": 0.10,
    "fuel": 0.00,
    "entertainment": 0.06,
    "pharmacy": 0.02,
    "luxury": 0.04,
}


@dataclass
class Config:
    """Application configuration with all paths rooted in the repository."""

    seed: int = 42
    n_events: int = 5_000
    n_users: int = 800
    batch_size: int = 250
    window_duration: str = "5 minutes"
    watermark_delay: str = "30 minutes"
    fraud_threshold: float = 0.65
    fraud_alert_rate: float = 0.08
    high_value_amount: float = 450.0
    discount_rules: dict[str, float] = field(
        default_factory=lambda: DEFAULT_DISCOUNT_RULES.copy()
    )
    source: str = field(default_factory=lambda: os.environ.get("SOURCE", "file").lower())
    kafka_bootstrap: str = field(
        default_factory=lambda: os.environ.get("KAFKA_BOOTSTRAP", "localhost:9092")
    )
    kafka_topic: str = field(
        default_factory=lambda: os.environ.get("KAFKA_TOPIC", "transactions")
    )
    root_dir: Path = field(default_factory=lambda: Path(__file__).resolve().parents[2])
    data_dir: Path | None = None
    stream_dir: Path | None = None
    full_data_path: Path | None = None
    checkpoint_dir: Path | None = None
    output_dir: Path | None = None
    enriched_dir: Path | None = None
    windowed_dir: Path | None = None
    reports_dir: Path | None = None
    figures_dir: Path | None = None

    def __post_init__(self) -> None:
        self.root_dir = Path(self.root_dir).resolve()
        self.data_dir = Path(self.data_dir or self.root_dir / "data")
        self.stream_dir = Path(self.stream_dir or self.data_dir / "stream")
        self.full_data_path = Path(
            self.full_data_path or self.data_dir / "transactions_full.parquet"
        )
        self.checkpoint_dir = Path(self.checkpoint_dir or self.root_dir / "checkpoints")
        self.output_dir = Path(self.output_dir or self.root_dir / "output")
        self.enriched_dir = Path(self.enriched_dir or self.output_dir / "enriched")
        self.windowed_dir = Path(self.windowed_dir or self.output_dir / "windowed")
        self.reports_dir = Path(self.reports_dir or self.root_dir / "reports")
        self.figures_dir = Path(self.figures_dir or self.reports_dir / "figures")

    def ensure(self) -> "Config":
        """Create runtime directories and return self for fluent setup."""
        for path in (
            self.data_dir,
            self.stream_dir,
            self.checkpoint_dir,
            self.output_dir,
            self.reports_dir,
            self.figures_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)
        return self

    def as_dict(self) -> dict[str, Any]:
        """Return a JSON-serializable configuration snapshot."""
        return {
            "seed": self.seed,
            "n_events": self.n_events,
            "n_users": self.n_users,
            "batch_size": self.batch_size,
            "window_duration": self.window_duration,
            "watermark_delay": self.watermark_delay,
            "fraud_threshold": self.fraud_threshold,
            "fraud_alert_rate": self.fraud_alert_rate,
            "high_value_amount": self.high_value_amount,
            "discount_rules": dict(self.discount_rules),
            "source": self.source,
            "kafka_bootstrap": self.kafka_bootstrap,
            "kafka_topic": self.kafka_topic,
            "paths": {
                "root_dir": str(self.root_dir),
                "data_dir": str(self.data_dir),
                "stream_dir": str(self.stream_dir),
                "full_data_path": str(self.full_data_path),
                "checkpoint_dir": str(self.checkpoint_dir),
                "output_dir": str(self.output_dir),
                "enriched_dir": str(self.enriched_dir),
                "windowed_dir": str(self.windowed_dir),
                "reports_dir": str(self.reports_dir),
                "figures_dir": str(self.figures_dir),
            },
        }
