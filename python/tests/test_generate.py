from __future__ import annotations

import json

from skproc.config import Config
from skproc.generate import generate_transactions
from skproc.source import TRANSACTION_SCHEMA


def test_generate_writes_events_and_schema_fields(tmp_path):
    cfg = Config(root_dir=tmp_path, n_events=240, seed=7, batch_size=50)
    df = generate_transactions(cfg)

    files = sorted(cfg.stream_dir.glob("*.json"))
    assert files
    assert cfg.full_data_path.exists()
    assert len(df) == 240
    assert {"FAILED", "PENDING"}.intersection(set(df["status"]))

    first = json.loads(files[0].read_text(encoding="utf-8").splitlines()[0])
    expected_fields = {field.name for field in TRANSACTION_SCHEMA}
    assert expected_fields.issubset(first.keys())
