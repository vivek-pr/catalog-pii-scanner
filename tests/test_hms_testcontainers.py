from __future__ import annotations

import json
import socket
import time
from typing import Any

import pytest
from typer.testing import CliRunner

from catalog_pii_scanner.cli import app

try:  # Optional runtime deps for this test
    from hmsclient import hmsclient  # type: ignore
    from hmsclient.genthrift.hive_metastore import ttypes  # type: ignore
    from testcontainers.core.container import DockerContainer  # type: ignore
except Exception:  # pragma: no cover - optional dependency not installed
    DockerContainer = None  # type: ignore
    hmsclient = None  # type: ignore
    ttypes = None  # type: ignore


pytestmark = pytest.mark.skipif(
    any(x is None for x in [DockerContainer, hmsclient, ttypes]),
    reason="testcontainers/hmsclient not installed",
)


def _wait_port(host: str, port: int, timeout: float = 30.0) -> None:
    deadline = time.time() + timeout
    last_err: Exception | None = None
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=1.0):
                return
        except Exception as e:  # noqa: BLE001
            last_err = e
            time.sleep(0.5)
    if last_err:
        raise last_err


def test_hms_enumerate_and_writeback_with_container(monkeypatch: Any) -> None:
    # Use Iceberg's lightweight HMS image (derby-backed)
    with DockerContainer("tabulario/hive-metastore:3.1.2").with_exposed_ports(9083) as c:  # type: ignore[operator]
        port = int(c.get_exposed_port(9083))  # type: ignore[arg-type]
        _wait_port("127.0.0.1", port, timeout=60.0)

        # Create demo DB and table via Thrift
        with hmsclient.HMSClient(host="127.0.0.1", port=port) as cli:  # type: ignore[misc]
            try:
                cli.create_database(  # type: ignore[attr-defined]
                    ttypes.Database(name="demo", description=None, locationUri=None, parameters={})  # type: ignore[attr-defined]
                )
            except Exception:
                pass

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
            from time import time as _now

            tbl = ttypes.Table(  # type: ignore[attr-defined]
                tableName="users",
                dbName="demo",
                owner="owner",
                createTime=int(_now()),
                lastAccessTime=0,
                retention=0,
                sd=sd,
                partitionKeys=[],
                parameters={},
                tableType="EXTERNAL_TABLE",
            )
            try:
                cli.create_table(tbl)  # type: ignore[attr-defined]
            except Exception:
                pass

        # Run CLI scan against HMS and apply tags
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
        data = json.loads(res.stdout)
        assert data["count"] >= 1

        # Verify writeback: properties + comment
        with hmsclient.HMSClient(host="127.0.0.1", port=port) as cli:  # type: ignore[misc]
            t = cli.get_table("demo", "users")  # type: ignore[attr-defined]
            props = getattr(t, "parameters", {})
            assert props.get("cps.pii.col.email") == "true"
            assert props.get("cps.pii_types.col.email") == "EMAIL"
            col = next(c for c in getattr(t.sd, "cols", []) if c.name == "email")
            assert "PII detected" in (col.comment or "")

        # Idempotent second run
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
        with hmsclient.HMSClient(host="127.0.0.1", port=port) as cli:  # type: ignore[misc]
            t2 = cli.get_table("demo", "users")  # type: ignore[attr-defined]
            col2 = next(c for c in getattr(t2.sd, "cols", []) if c.name == "email")
            assert (col2.comment or "").count("PII detected") == 1
