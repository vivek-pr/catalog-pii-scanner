from __future__ import annotations

import json
import os
import socket
from urllib.parse import urlparse

import pytest
from typer.testing import CliRunner

from catalog_pii_scanner.cli import app


def _has_localstack() -> bool:
    # Check that the endpoint is reachable on TCP
    url = os.getenv("AWS_ENDPOINT_URL") or os.getenv("GLUE_ENDPOINT_URL") or "http://localhost:4566"
    try:
        p = urlparse(url)
        host = p.hostname or "localhost"
        port = p.port or (443 if p.scheme == "https" else 80)
        with socket.create_connection((host, port), timeout=0.2):
            return True
    except Exception:
        return False


def test_glue_enumerate_and_writeback_localstack(monkeypatch: pytest.MonkeyPatch) -> None:
    if not _has_localstack():
        pytest.skip("Localstack endpoint not configured")

    try:
        import boto3  # type: ignore
    except Exception:
        pytest.skip("boto3 not installed in this env")

    endpoint = os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566")
    region = os.getenv("AWS_REGION", "us-east-1")
    # Minimal creds for localstack
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", os.getenv("AWS_ACCESS_KEY_ID", "test"))
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", os.getenv("AWS_SECRET_ACCESS_KEY", "test"))
    monkeypatch.setenv("AWS_REGION", region)
    monkeypatch.setenv("AWS_ENDPOINT_URL", endpoint)

    glue = boto3.client("glue", region_name=region, endpoint_url=endpoint)

    # Create demo catalog if not exists
    db_name = "demo"
    try:
        glue.create_database(DatabaseInput={"Name": db_name})
    except Exception:
        pass

    tbl_name = "users"
    cols = [
        {"Name": "id", "Type": "int"},
        {"Name": "email", "Type": "string", "Comment": "user email"},
        {"Name": "age", "Type": "int"},
    ]
    try:
        glue.create_table(
            DatabaseName=db_name,
            TableInput={
                "Name": tbl_name,
                "StorageDescriptor": {
                    "Columns": cols,
                    "Location": "s3://dummy",
                },
                "TableType": "EXTERNAL_TABLE",
                "Parameters": {},
            },
        )
    except Exception:
        pass

    runner = CliRunner()
    result = runner.invoke(
        app,
        [
            "scan",
            "--target",
            "glue://*",
            "--apply",
            "--type",
            "EMAIL",
            "--append-comment",
            "PII detected",
        ],
    )
    assert result.exit_code == 0, result.output
    data = json.loads(result.stdout)
    assert data["count"] >= 1

    # Verify writeback on the email column
    t = glue.get_table(DatabaseName=db_name, Name=tbl_name)["Table"]
    c = next(cc for cc in t["StorageDescriptor"]["Columns"] if cc["Name"] == "email")
    params = c.get("Parameters") or {}
    assert params.get("pii") == "true"
    assert params.get("pii_types") == "EMAIL"
    assert "PII detected" in (c.get("Comment") or "")

    # Idempotent: run again and ensure comment not duplicated
    result2 = runner.invoke(
        app,
        [
            "scan",
            "--target",
            "glue://*",
            "--apply",
            "--type",
            "EMAIL",
            "--append-comment",
            "PII detected",
        ],
    )
    assert result2.exit_code == 0
    t2 = glue.get_table(DatabaseName=db_name, Name=tbl_name)["Table"]
    c2 = next(cc for cc in t2["StorageDescriptor"]["Columns"] if cc["Name"] == "email")
    assert (c2.get("Comment") or "").count("PII detected") == 1
