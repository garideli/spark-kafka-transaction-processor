"""Matplotlib charts for the transaction processing outputs."""
from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from ..config import Config
from ..logging_utils import get_logger

log = get_logger("charts")

FIGURE_NAMES = [
    "throughput_over_time.png",
    "windowed_fraud_rate.png",
    "status_breakdown.png",
    "value_by_category.png",
    "discount_effect.png",
    "fraud_by_channel.png",
]


def create_charts(cfg: Config) -> list[Path]:
    """Create the six required PNG figures from generated Parquet artifacts."""
    cfg.ensure()
    _apply_style()
    enriched = read_parquet_dir(cfg.enriched_dir)
    windowed = read_parquet_dir(cfg.windowed_dir)
    full = pd.read_parquet(cfg.full_data_path) if cfg.full_data_path.exists() else pd.DataFrame()

    paths = [
        _throughput_over_time(windowed, cfg),
        _windowed_fraud_rate(windowed, cfg),
        _status_breakdown(full, enriched, cfg),
        _value_by_category(enriched, cfg),
        _discount_effect(enriched, cfg),
        _fraud_by_channel(enriched, cfg),
    ]
    log.info("wrote %s figures to %s", len(paths), cfg.figures_dir)
    return paths


def read_parquet_dir(path: Path) -> pd.DataFrame:
    """Read Spark part files from a directory into a pandas DataFrame."""
    files = sorted(path.glob("*.parquet")) if path.exists() else []
    if not files:
        return pd.DataFrame()
    return pd.concat((pd.read_parquet(file) for file in files), ignore_index=True)


def _apply_style() -> None:
    plt.rcParams.update(
        {
            "figure.figsize": (10, 5.5),
            "figure.dpi": 130,
            "axes.facecolor": "#f8fafc",
            "figure.facecolor": "white",
            "axes.edgecolor": "#334155",
            "axes.labelcolor": "#0f172a",
            "axes.titleweight": "bold",
            "axes.titlesize": 15,
            "axes.grid": True,
            "grid.color": "#d8dee9",
            "grid.linestyle": "-",
            "grid.linewidth": 0.7,
            "xtick.color": "#334155",
            "ytick.color": "#334155",
            "font.size": 10,
            "legend.frameon": False,
        }
    )


def _save(fig: plt.Figure, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(path, bbox_inches="tight")
    plt.close(fig)
    return path


def _throughput_over_time(windowed: pd.DataFrame, cfg: Config) -> Path:
    fig, ax = plt.subplots()
    path = cfg.figures_dir / "throughput_over_time.png"
    if windowed.empty:
        _empty(ax, "Throughput over time")
    else:
        df = windowed.sort_values("window_start")
        ax.plot(pd.to_datetime(df["window_start"]), df["event_count"], color="#2563eb", linewidth=2.2)
        ax.fill_between(pd.to_datetime(df["window_start"]), df["event_count"], alpha=0.18, color="#2563eb")
        ax.set_title("Events processed per event-time window")
        ax.set_xlabel("Window start")
        ax.set_ylabel("Successful events")
    return _save(fig, path)


def _windowed_fraud_rate(windowed: pd.DataFrame, cfg: Config) -> Path:
    fig, ax = plt.subplots()
    path = cfg.figures_dir / "windowed_fraud_rate.png"
    if windowed.empty:
        _empty(ax, "Windowed fraud rate")
    else:
        df = windowed.sort_values("window_start")
        ax.plot(pd.to_datetime(df["window_start"]), df["fraud_rate"], color="#dc2626", linewidth=2.2)
        ax.axhline(cfg.fraud_alert_rate, color="#111827", linestyle="--", linewidth=1.3, label="alert line")
        ax.set_ylim(0, max(float(df["fraud_rate"].max()) * 1.25, cfg.fraud_alert_rate * 1.8, 0.05))
        ax.set_title("Flagged fraud rate by window")
        ax.set_xlabel("Window start")
        ax.set_ylabel("Flagged rate")
        ax.legend()
    return _save(fig, path)


def _status_breakdown(full: pd.DataFrame, enriched: pd.DataFrame, cfg: Config) -> Path:
    fig, ax = plt.subplots()
    path = cfg.figures_dir / "status_breakdown.png"
    if full.empty:
        _empty(ax, "Status breakdown")
    else:
        counts = full["status"].value_counts().reindex(["SUCCESS", "FAILED", "PENDING"]).fillna(0)
        colors = ["#16a34a", "#ef4444", "#f59e0b"]
        ax.bar(counts.index, counts.values, color=colors)
        filtered = len(full) - len(enriched)
        filtered_share = filtered / len(full) if len(full) else 0.0
        ax.set_title(f"Input statuses and filtered share ({filtered_share:.1%})")
        ax.set_xlabel("Status")
        ax.set_ylabel("Events")
        for idx, value in enumerate(counts.values):
            ax.text(idx, value, f"{int(value):,}", ha="center", va="bottom", color="#0f172a")
    return _save(fig, path)


def _value_by_category(enriched: pd.DataFrame, cfg: Config) -> Path:
    fig, ax = plt.subplots()
    path = cfg.figures_dir / "value_by_category.png"
    if enriched.empty:
        _empty(ax, "Value by category")
    else:
        totals = enriched.groupby("merchant_category")["net_amount"].sum().sort_values(ascending=True)
        ax.barh(totals.index, totals.values, color="#0891b2")
        ax.set_title("Net transaction value by merchant category")
        ax.set_xlabel("Net value")
        ax.set_ylabel("")
    return _save(fig, path)


def _discount_effect(enriched: pd.DataFrame, cfg: Config) -> Path:
    fig, ax = plt.subplots()
    path = cfg.figures_dir / "discount_effect.png"
    if enriched.empty:
        _empty(ax, "Discount effect")
    else:
        ax.hist(enriched["gross_amount"], bins=34, alpha=0.58, color="#64748b", label="gross")
        ax.hist(enriched["net_amount"], bins=34, alpha=0.58, color="#22c55e", label="net")
        ax.set_title("Gross vs net amount distribution")
        ax.set_xlabel("Amount")
        ax.set_ylabel("Events")
        ax.legend()
    return _save(fig, path)


def _fraud_by_channel(enriched: pd.DataFrame, cfg: Config) -> Path:
    fig, ax = plt.subplots()
    path = cfg.figures_dir / "fraud_by_channel.png"
    if enriched.empty:
        _empty(ax, "Fraud by channel")
    else:
        rates = enriched.groupby("channel")["is_flagged"].mean().sort_values(ascending=False)
        ax.bar(rates.index, rates.values, color="#7c3aed")
        ax.axhline(cfg.fraud_alert_rate, color="#111827", linestyle="--", linewidth=1.3, label="alert line")
        ax.set_title("Flagged rate by channel")
        ax.set_xlabel("Channel")
        ax.set_ylabel("Flagged rate")
        ax.set_ylim(0, max(float(rates.max()) * 1.25, cfg.fraud_alert_rate * 1.8, 0.05))
        ax.legend()
    return _save(fig, path)


def _empty(ax: plt.Axes, title: str) -> None:
    ax.set_title(title)
    ax.text(0.5, 0.5, "No data", transform=ax.transAxes, ha="center", va="center")
    ax.set_xticks([])
    ax.set_yticks([])
