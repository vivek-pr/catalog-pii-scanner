from __future__ import annotations

import json
import logging
from typing import Any, cast

import pytest

from catalog_pii_scanner.datasets import generate_synthetic
from catalog_pii_scanner.embeddings import EmbedModel
from catalog_pii_scanner.ensemble import Calibrator, Ensemble
from catalog_pii_scanner.logging_utils import (
    JsonFormatter,
    correlation_context,
    get_logger,
)
from catalog_pii_scanner.rules import propose_candidates


def _format_record(rec: logging.LogRecord) -> dict[str, Any] | None:
    try:
        s = JsonFormatter().format(rec)
        obj = json.loads(s)
        if isinstance(obj, dict):
            return cast(dict[str, Any], obj)
        return None
    except Exception:
        return None


def test_logs_are_json_and_redacted(caplog: pytest.LogCaptureFixture) -> None:
    # Ensure our logger is configured and captured
    logger = get_logger()
    caplog.set_level(logging.DEBUG, logger=logger.name)

    ds = generate_synthetic(n=2, seed=7)

    raw_pii: set[str] = set()
    with correlation_context("test-corr-123"):
        for ex in ds:
            for span, _ in ex.labels:
                raw_pii.add(span.text)
            cands = propose_candidates(ex.text)
            ens = Ensemble(embed=EmbedModel(), calibrator=Calibrator.identity())
            _ = ens.predict(ex.text, cands)

    # All log records from our logger are JSON parseable via formatter
    objs = [o for o in (_format_record(r) for r in caplog.records if r.name == logger.name) if o]
    assert objs, "Expected JSON structured logs from safe logger"

    # No raw PII substrings appear in captured logs
    for pii in raw_pii:
        assert pii not in caplog.text

    # Snapshot-like assertions: check structure of scan_contexts entries
    scans = [o for o in objs if o.get("event") == "scan_contexts"]
    assert len(scans) >= 2
    for obj in scans:
        # Required keys
        assert obj.get("time") and obj.get("level") in {"DEBUG", "INFO"} and obj.get("logger")
        assert obj.get("correlation_id") == "test-corr-123"
        assert isinstance(obj.get("n_candidates"), int)
        assert isinstance(obj.get("examples"), list)
        assert isinstance(obj.get("redacted_text"), str)
        # No raw PII inside any structured field
        flat = json.dumps(obj)
        for pii in raw_pii:
            assert pii not in flat
