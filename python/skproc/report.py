"""Markdown and JSON reporting for pipeline runs."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

from .config import Config
from .source import TRANSACTION_SCHEMA
from .viz.charts import read_parquet_dir


def create_report(cfg: Config, run_stats: dict[str, Any]) -> dict[str, Path]:
    """Write pipeline_report.md and metrics.json."""
    cfg.ensure()
    enriched = read_parquet_dir(cfg.enriched_dir)
    windowed = read_parquet_dir(cfg.windowed_dir)
    full = pd.read_parquet(cfg.full_data_path) if cfg.full_data_path.exists() else pd.DataFrame()
    metrics = build_metrics(cfg, run_stats, enriched, windowed, full)

    metrics_path = cfg.reports_dir / "metrics.json"
    metrics_path.write_text(json.dumps(metrics, indent=2, sort_keys=True), encoding="utf-8")

    report_path = cfg.reports_dir / "pipeline_report.md"
    report_path.write_text(_render_report(cfg, metrics, enriched, windowed), encoding="utf-8")
    return {"metrics": metrics_path, "report": report_path}


def build_metrics(
    cfg: Config,
    run_stats: dict[str, Any],
    enriched: pd.DataFrame,
    windowed: pd.DataFrame,
    full: pd.DataFrame,
) -> dict[str, Any]:
    """Build the metrics JSON payload from observed artifacts."""
    source_events = int(len(full)) if not full.empty else int(run_stats.get("source_events", cfg.n_events))
    enriched_events = int(len(enriched))
    filtered_count = max(source_events - enriched_events, 0)
    flagged_count = int(enriched["is_flagged"].sum()) if "is_flagged" in enriched else 0
    duration = float(run_stats.get("pipeline_duration_seconds", 0.0))
    status_counts = (
        {str(k): int(v) for k, v in full["status"].value_counts().to_dict().items()}
        if "status" in full
        else run_stats.get("status_counts", {})
    )
    total_gross = float(enriched["gross_amount"].sum()) if "gross_amount" in enriched else 0.0
    total_net = float(enriched["net_amount"].sum()) if "net_amount" in enriched else 0.0

    return {
        "source": cfg.source,
        "events_generated": source_events,
        "events_enriched": enriched_events,
        "events_filtered": filtered_count,
        "filtered_pct": filtered_count / source_events if source_events else 0.0,
        "windows": int(len(windowed)),
        "flagged_count": flagged_count,
        "flagged_rate": flagged_count / enriched_events if enriched_events else 0.0,
        "max_window_fraud_rate": float(windowed["fraud_rate"].max()) if "fraud_rate" in windowed and not windowed.empty else 0.0,
        "total_gross_value": total_gross,
        "total_net_value": total_net,
        "discount_value": total_gross - total_net,
        "pipeline_duration_seconds": duration,
        "throughput_events_per_second": enriched_events / duration if duration > 0 else 0.0,
        "status_counts": status_counts,
        "window_duration": cfg.window_duration,
        "watermark_delay": cfg.watermark_delay,
        "fraud_threshold": cfg.fraud_threshold,
        "fraud_alert_rate": cfg.fraud_alert_rate,
        "min_event_time": run_stats.get("min_event_time"),
        "max_event_time": run_stats.get("max_event_time"),
    }


def dataframe_to_markdown(df: pd.DataFrame, max_rows: int = 8) -> str:
    """Render a compact GitHub-flavored Markdown table without tabulate."""
    if df.empty:
        return "_No rows produced._"
    display = df.head(max_rows).copy()
    for col in display.columns:
        if pd.api.types.is_datetime64_any_dtype(display[col]):
            display[col] = display[col].dt.strftime("%Y-%m-%d %H:%M:%S")
    rows = [[_format_value(value) for value in row] for row in display.to_numpy()]
    headers = [str(col) for col in display.columns]
    widths = [
        max(len(headers[idx]), *(len(row[idx]) for row in rows))
        for idx in range(len(headers))
    ]
    header_line = "| " + " | ".join(headers[idx].ljust(widths[idx]) for idx in range(len(headers))) + " |"
    divider = "| " + " | ".join("-" * widths[idx] for idx in range(len(headers))) + " |"
    body = [
        "| " + " | ".join(row[idx].ljust(widths[idx]) for idx in range(len(headers))) + " |"
        for row in rows
    ]
    return "\n".join([header_line, divider, *body])


def sample_enriched_for_markdown(cfg: Config, rows: int = 6) -> str:
    enriched = read_parquet_dir(cfg.enriched_dir)
    cols = [
        "transaction_id",
        "user_id",
        "event_time",
        "merchant_category",
        "channel",
        "gross_amount",
        "discount_rate",
        "net_amount",
        "fraud_score",
        "is_flagged",
    ]
    existing = [col for col in cols if col in enriched.columns]
    return dataframe_to_markdown(enriched.sort_values("event_time")[existing], max_rows=rows) if existing else "_No rows produced._"


def sample_windowed_for_markdown(cfg: Config, rows: int = 8) -> str:
    windowed = read_parquet_dir(cfg.windowed_dir)
    if not windowed.empty:
        windowed = windowed.sort_values("window_start")
    return dataframe_to_markdown(windowed, max_rows=rows)


def _render_report(
    cfg: Config,
    metrics: dict[str, Any],
    enriched: pd.DataFrame,
    windowed: pd.DataFrame,
) -> str:
    schema_rows = pd.DataFrame(
        [{"field": field.name, "type": field.dataType.simpleString(), "nullable": field.nullable} for field in TRANSACTION_SCHEMA]
    )
    transform_rules = pd.DataFrame(
        [
            {"rule": "status filter", "detail": "keep SUCCESS transactions only"},
            {"rule": "discount", "detail": "net_amount = amount * (1 - category discount_rate)"},
            {"rule": "fraud score", "detail": "bounded 0..1 score from amount, category, channel, country, and latent signal"},
            {"rule": "window", "detail": f"{cfg.window_duration} event-time windows with {cfg.watermark_delay} watermark"},
            {"rule": "trigger", "detail": "availableNow=True; every query awaits termination"},
        ]
    )
    enriched_cols = [
        col
        for col in [
            "transaction_id",
            "event_time",
            "merchant_category",
            "channel",
            "gross_amount",
            "discount_rate",
            "net_amount",
            "fraud_score",
            "is_flagged",
        ]
        if col in enriched.columns
    ]
    enriched_sample = enriched.sort_values("event_time")[enriched_cols] if enriched_cols else pd.DataFrame()
    windowed_sample = windowed.sort_values("window_start") if not windowed.empty else windowed

    return f"""# Pipeline Report

## Run stats

{dataframe_to_markdown(pd.DataFrame([metrics]))}

## Input schema

{dataframe_to_markdown(schema_rows, max_rows=20)}

## Transform rules

{dataframe_to_markdown(transform_rules, max_rows=10)}

## Sample enriched events

{dataframe_to_markdown(enriched_sample, max_rows=8)}

## Windowed aggregates

{dataframe_to_markdown(windowed_sample, max_rows=12)}
"""


def _format_value(value: Any) -> str:
    if pd.isna(value):
        return ""
    if isinstance(value, float):
        return f"{value:,.4f}" if abs(value) < 1 else f"{value:,.2f}"
    return str(value)
