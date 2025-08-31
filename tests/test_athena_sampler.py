from __future__ import annotations

import os
from typing import Any

import boto3
import pytest
from botocore.stub import ANY, Stubber

from catalog_pii_scanner.connectors.athena import AthenaConfig, AthenaSampler


@pytest.fixture(autouse=True)
def _aws_env() -> None:  # ensure region/creds exist for boto3 clients
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "test")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "test")


def _mk_status(state: str) -> dict[str, Any]:
    return {
        "QueryExecution": {
            "Status": {"State": state, "StateChangeReason": ""},
            "Statistics": {"EngineExecutionTimeInMillis": 10, "DataScannedInBytes": 1234},
        }
    }


def test_athena_sampler_samples_with_not_ready_then_success() -> None:
    ath = boto3.client("athena")
    glu = boto3.client("glue")
    s_ath = Stubber(ath)
    s_glu = Stubber(glu)

    # 1) Create workgroup with cutoff and s3 output
    cfg = AthenaConfig(
        s3_output="s3://bucket/results/",
        workgroup=None,
        create_workgroup=True,
        workgroup_bytes_scanned_cutoff=50 * 1024 * 1024,  # 50 MiB
        max_wait_seconds=5.0,
        initial_backoff=0.01,
        max_backoff=0.05,
        max_retries=2,
        create_temp_database=False,
    )
    s_ath.add_response(
        "create_work_group",
        {},
        expected_params={
            "Name": ANY,
            "Configuration": {
                "ResultConfiguration": {"OutputLocation": cfg.s3_output},
                "EnforceWorkGroupConfiguration": True,
                "PublishCloudWatchMetricsEnabled": False,
                "RequesterPaysEnabled": False,
                "BytesScannedCutoffPerQuery": cfg.workgroup_bytes_scanned_cutoff,
            },
            "Description": ANY,
        },
    )

    # 2) Start query
    s_ath.add_response(
        "start_query_execution",
        {"QueryExecutionId": "q-123"},
        expected_params={
            "QueryString": (
                "SELECT email FROM users WHERE email IS NOT NULL ORDER BY rand() LIMIT 2"
            ),
            "WorkGroup": ANY,
            "QueryExecutionContext": {"Database": "demo"},
        },
    )

    # 3) Poll: QUEUED -> RUNNING -> SUCCEEDED
    s_ath.add_response(
        "get_query_execution", _mk_status("QUEUED"), expected_params={"QueryExecutionId": "q-123"}
    )
    s_ath.add_response(
        "get_query_execution", _mk_status("RUNNING"), expected_params={"QueryExecutionId": "q-123"}
    )
    s_ath.add_response(
        "get_query_execution",
        _mk_status("SUCCEEDED"),
        expected_params={"QueryExecutionId": "q-123"},
    )

    # 4) Results: first call returns not-ready, then success with header + 2 rows
    s_ath.add_client_error(
        "get_query_results",
        service_error_code="InvalidRequestException",
        service_message="Query has not yet finished",
        http_status_code=400,
        expected_params={"QueryExecutionId": "q-123"},
    )

    s_ath.add_response(
        "get_query_results",
        {
            "ResultSet": {
                "Rows": [
                    {"Data": [{"VarCharValue": "email"}]},
                    {"Data": [{"VarCharValue": "a@example.com"}]},
                    {"Data": [{"VarCharValue": "b@test.org"}]},
                ]
            }
        },
        expected_params={"QueryExecutionId": "q-123"},
    )

    # 5) Cleanup: delete temporary workgroup when closing sampler
    s_ath.add_response(
        "delete_work_group",
        {},
        expected_params={
            "WorkGroup": ANY,
            "RecursiveDeleteOption": True,
        },
    )

    s_glu.activate()
    s_ath.activate()
    try:
        sampler = AthenaSampler(config=cfg, athena_client=ath, glue_client=glu)
        vals = sampler.sample_column(database="demo", table="users", column="email", n=2)
        assert sorted(vals) == ["a@example.com", "b@test.org"]
    finally:
        sampler.close()
        s_ath.deactivate()
        s_glu.deactivate()
