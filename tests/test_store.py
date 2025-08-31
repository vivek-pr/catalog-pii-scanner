from __future__ import annotations

import json
from pathlib import Path

from typer.testing import CliRunner

from catalog_pii_scanner.cli import app
from catalog_pii_scanner.db import (
    Base,
    Finding,
    init_db,
    session_scope,
    upsert_column,
)


def _sqlite_url(tmp_path: Path) -> str:
    return f"sqlite:///{tmp_path}/cps.db"


def test_crud_and_cli_export(tmp_path: Path) -> None:
    # Setup SQLite DB
    db_url = _sqlite_url(tmp_path)
    Session = init_db(db_url)
    # CRUD via repository helpers
    with session_scope(Session) as s:
        col = upsert_column(s, "cat1", "sch1", "tbl1", "col1", data_type="string")
        assert col.id > 0 and col.ref == "cat1.sch1.tbl1.col1"

        # Add a finding
        from catalog_pii_scanner.db import add_finding

        f = add_finding(
            s,
            col,
            types=["EMAIL", "PHONE"],
            confidence=0.88,
            hit_rate=0.42,
            model_version="test-v1",
            source="unit",
        )
        assert f.id > 0 and f.column_ref.endswith("col1")

        # Update confidence
        f.confidence = 0.91
        s.flush()

    # Validate read
    with session_scope(Session) as s:
        f2 = s.get(Finding, f.id)
        assert f2 is not None and abs(f2.confidence - 0.91) < 1e-6

    # CLI: scan --dry-run writes another finding
    runner = CliRunner()
    r = runner.invoke(
        app,
        [
            "scan",
            "--dry-run",
            "--db",
            db_url,
            "--catalog",
            "cat1",
            "--schema",
            "sch1",
            "--table",
            "tbl1",
            "--column",
            "col1",
            "--type",
            "EMAIL",
            "--model-version",
            "cli-v1",
        ],
    )
    assert r.exit_code == 0

    # Export JSON and CSV
    out_json = tmp_path / "findings.json"
    out_csv = tmp_path / "findings.csv"
    rj = runner.invoke(app, ["export", "--format", "json", "--db", db_url, "--out", str(out_json)])
    rc = runner.invoke(app, ["export", "--format", "csv", "--db", db_url, "--out", str(out_csv)])
    assert rj.exit_code == 0 and rc.exit_code == 0
    assert out_json.exists() and out_csv.exists()
    data = json.loads(out_json.read_text())
    assert isinstance(data, list) and len(data) >= 2
    assert set(data[0].keys()) >= {
        "id",
        "column_ref",
        "types",
        "confidence",
        "hit_rate",
        "model_version",
        "scanned_at",
        "source",
    }


def test_postgres_ddl_smoke() -> None:
    # Ensure metadata compiles for PostgreSQL (no live DB required)
    from sqlalchemy.dialects import postgresql
    from sqlalchemy.schema import CreateTable

    dialect = postgresql.dialect()
    # Exercise all tables
    for table in Base.metadata.sorted_tables:
        sql = str(CreateTable(table).compile(dialect=dialect))
        # Basic sanity: CREATE TABLE and table name appear
        assert sql.startswith("\nCREATE TABLE") and table.name in sql
