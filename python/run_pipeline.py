"""Generate offline events, run availableNow streaming, and publish artifacts."""
from __future__ import annotations

import argparse
import json
import shutil
import time
from pathlib import Path

from skproc.config import Config
from skproc.generate import generate_transactions
from skproc.logging_utils import get_logger
from skproc.pipeline import create_spark_session, run_streaming_pipeline
from skproc.report import create_report
from skproc.viz.charts import create_charts

log = get_logger("run")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the offline PySpark transaction pipeline.")
    parser.add_argument("--events", type=int, default=5_000, help="number of synthetic events")
    parser.add_argument("--seed", type=int, default=42, help="deterministic generator seed")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    cfg = Config(n_events=args.events, seed=args.seed)
    _reset_run_outputs(cfg)
    cfg.ensure()

    started = time.perf_counter()
    generated = generate_transactions(cfg)
    spark = create_spark_session()
    try:
        run_stats = run_streaming_pipeline(spark, cfg)
    finally:
        spark.stop()

    figures = create_charts(cfg)
    report_paths = create_report(cfg, run_stats)
    manifest = {
        "config": cfg.as_dict(),
        "generated_events": int(len(generated)),
        "artifacts": {
            "enriched": str(cfg.enriched_dir),
            "windowed": str(cfg.windowed_dir),
            "figures": [str(path) for path in figures],
            "report": str(report_paths["report"]),
            "metrics": str(report_paths["metrics"]),
        },
        "stats": run_stats,
        "total_duration_seconds": round(time.perf_counter() - started, 3),
    }
    manifest_path = cfg.reports_dir / "run_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True), encoding="utf-8")

    print(
        "Pipeline complete: "
        f"{run_stats['source_events']:,} generated, "
        f"{run_stats['enriched_events']:,} enriched, "
        f"{run_stats['filtered_count']:,} filtered, "
        f"{run_stats['window_count']:,} windows, "
        f"{run_stats['flagged_count']:,} flagged."
    )
    print(f"Artifacts: {cfg.output_dir} and {cfg.reports_dir}")
    return 0


def _reset_run_outputs(cfg: Config) -> None:
    for path in (
        cfg.stream_dir,
        cfg.checkpoint_dir,
        cfg.output_dir,
        cfg.figures_dir,
    ):
        if Path(path).exists():
            shutil.rmtree(path)
    if cfg.full_data_path.exists():
        if cfg.full_data_path.is_dir():
            shutil.rmtree(cfg.full_data_path)
        else:
            cfg.full_data_path.unlink()


if __name__ == "__main__":
    raise SystemExit(main())
