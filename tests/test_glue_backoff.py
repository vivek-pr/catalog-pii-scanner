from __future__ import annotations

from typing import Any

import pytest

from catalog_pii_scanner.connectors.glue import GlueCatalogClient

try:
    from botocore.exceptions import ClientError  # type: ignore
except Exception:  # pragma: no cover - environment without boto3
    ClientError = Exception  # type: ignore


class _FlakyGlue:
    def __init__(self, fail_times: int) -> None:
        self.calls = 0
        self.fail_times = fail_times

    def get_databases(self, **_: Any) -> dict:
        self.calls += 1
        if self.calls <= self.fail_times:
            raise ClientError(
                {"Error": {"Code": "ThrottlingException", "Message": "Rate exceeded"}},
                "GetDatabases",
            )
        return {"DatabaseList": [{"Name": "demo"}]}


def test_backoff_succeeds_after_throttle_retries(monkeypatch: pytest.MonkeyPatch) -> None:
    # Skip if boto3 not available in env
    try:
        import boto3  # type: ignore  # noqa: F401
    except Exception:
        pytest.skip("boto3 not installed in this env")

    flaky = _FlakyGlue(fail_times=2)
    cli = GlueCatalogClient(boto3_client=flaky, max_retries=3, base_delay=0.01)
    dbs = cli.list_databases()
    assert dbs == ["demo"]
