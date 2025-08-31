from __future__ import annotations

import json
from time import time
from typing import Any, cast

import pytest
from typer.testing import CliRunner

from catalog_pii_scanner.cli import app

try:
    from hmsclient.genthrift.hive_metastore import ttypes  # type: ignore

    from catalog_pii_scanner.connectors.hms import HiveMetastoreClient
except Exception:  # pragma: no cover - optional dependency not installed
    ttypes = None  # type: ignore
    HiveMetastoreClient = None  # type: ignore


pytestmark = pytest.mark.skipif(ttypes is None, reason="hmsclient not installed")


class _FakeHMS:
    def __init__(self) -> None:
        self._dbs: dict[str, dict[str, Any]] = {}

    # Minimal API used by HiveMetastoreClient
    def get_all_databases(self) -> list[str]:
        return list(self._dbs.keys())

    def get_all_tables(self, db: str) -> list[str]:
        return list(self._dbs.get(db, {}))

    def get_table(self, db: str, name: str) -> Any:
        return cast(Any, self._dbs[db][name])

    def alter_table(self, db: str, name: str, new_table: Any) -> None:
        self._dbs.setdefault(db, {})[name] = new_table

    # Helpers for setup
    def create_database(self, name: str) -> None:
        self._dbs.setdefault(name, {})

    def create_simple_table(self, db: str, name: str) -> None:
        cols = [
            ttypes.FieldSchema(name="id", type="int", comment=None),  # type: ignore[attr-defined]
            ttypes.FieldSchema(name="email", type="string", comment="user email"),  # type: ignore[attr-defined]
        ]
        sdi = ttypes.SerDeInfo(  # type: ignore[attr-defined]
            name="serde",
            serializationLib="org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
            parameters={},
        )
        sd = ttypes.StorageDescriptor(  # type: ignore[attr-defined]
            cols=cols,
            location="file:/tmp",
            inputFormat="org.apache.hadoop.mapred.TextInputFormat",
            outputFormat="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
            compressed=False,
            numBuckets=0,
            serdeInfo=sdi,
            bucketCols=[],
            sortCols=[],
            parameters={},
            skewedInfo=None,
            storedAsSubDirectories=False,
        )
        tbl = ttypes.Table(  # type: ignore[attr-defined]
            tableName=name,
            dbName=db,
            owner="owner",
            createTime=int(time()),
            lastAccessTime=0,
            retention=0,
            sd=sd,
            partitionKeys=[],
            parameters={},
            tableType="EXTERNAL_TABLE",
        )
        self._dbs.setdefault(db, {})[name] = tbl


def test_hms_enumerate_and_writeback_fake(monkeypatch: Any) -> None:
    fake = _FakeHMS()
    fake.create_database("demo")
    fake.create_simple_table("demo", "users")

    # Patch CLI to use our client
    client = HiveMetastoreClient(raw_client=fake)  # type: ignore[arg-type]
    monkeypatch.setattr(
        "catalog_pii_scanner.cli.HiveMetastoreClient",
        lambda: client,
        raising=True,
    )

    runner = CliRunner()
    res = runner.invoke(
        app,
        [
            "scan",
            "--target",
            "hms://*",
            "--apply",
            "--type",
            "EMAIL",
            "--append-comment",
            "PII detected",
        ],
    )
    assert res.exit_code == 0, res.output
    payload = json.loads(res.stdout)
    assert payload["count"] >= 1

    # Verify writeback idempotency and properties
    tbl = fake.get_table("demo", "users")
    props = getattr(tbl, "parameters", {})
    assert props.get("cps.pii.col.email") == "true"
    assert props.get("cps.pii_types.col.email") == "EMAIL"
    col = next(c for c in getattr(tbl.sd, "cols", []) if c.name == "email")
    assert "PII detected" in (col.comment or "")

    # Run again; comment should not duplicate
    res2 = runner.invoke(
        app,
        [
            "scan",
            "--target",
            "hms://*",
            "--apply",
            "--type",
            "EMAIL",
            "--append-comment",
            "PII detected",
        ],
    )
    assert res2.exit_code == 0
    tbl2 = fake.get_table("demo", "users")
    col2 = next(c for c in getattr(tbl2.sd, "cols", []) if c.name == "email")
    assert (col2.comment or "").count("PII detected") == 1
