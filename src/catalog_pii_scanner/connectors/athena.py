from __future__ import annotations

import random
import time
import uuid
from dataclasses import dataclass
from typing import Any

import boto3
from botocore.client import BaseClient
from botocore.exceptions import BotoCoreError, ClientError


@dataclass
class AthenaConfig:
    s3_output: str
    workgroup: str | None = None
    create_workgroup: bool = True
    workgroup_bytes_scanned_cutoff: int = 100 * 1024 * 1024  # 100 MiB
    enforce_workgroup_configuration: bool = True
    # Polling/timeout
    max_wait_seconds: float = 60.0
    initial_backoff: float = 0.5
    max_backoff: float = 5.0
    max_retries: int = 3
    # Optional temp DB (not required for simple SELECT sampling)
    create_temp_database: bool = False
    temp_db_prefix: str = "cps_tmp"


class AthenaSampler:
    """Sample values from Athena tables with bounded cost and retries.

    - Creates a temporary workgroup (optional) with BytesScanned cutoff
    - Polls query execution with exponential backoff
    - Handles eventual consistency where results aren't immediately ready

    Only SELECT queries are used; a temp database is optional and currently
    unused except for future CTAS-based strategies.
    """

    def __init__(
        self,
        *,
        config: AthenaConfig,
        athena_client: BaseClient | None = None,
        glue_client: BaseClient | None = None,
        region_name: str | None = None,
    ) -> None:
        if not config.s3_output or not config.s3_output.startswith("s3://"):
            raise ValueError("config.s3_output must be an s3:// URL")
        self.cfg = config
        self._athena: BaseClient = athena_client or boto3.client("athena", region_name=region_name)
        self._glue: BaseClient = glue_client or boto3.client("glue", region_name=region_name)
        self._wg_name: str | None = None
        self._wg_created: bool = False
        self._temp_db: str | None = None
        self._temp_db_created: bool = False

        # Ensure a workgroup is available
        if self.cfg.workgroup:
            self._wg_name = self.cfg.workgroup
        elif self.cfg.create_workgroup:
            self._wg_name = self._create_temp_workgroup()
            self._wg_created = True
        else:
            # Fall back to primary workgroup
            self._wg_name = "primary"

        # Optionally create temporary database in Glue
        if self.cfg.create_temp_database:
            self._temp_db = self._create_temp_db()
            self._temp_db_created = True

    # -------------------- Public API --------------------
    def close(self) -> None:
        # Best effort cleanup
        if self._temp_db_created and self._temp_db:
            try:
                self._glue.delete_database(Name=self._temp_db)
            except Exception:
                pass
            self._temp_db_created = False
        if self._wg_created and self._wg_name:
            try:
                self._athena.delete_work_group(WorkGroup=self._wg_name, RecursiveDeleteOption=True)
            except Exception:
                pass
            self._wg_created = False

    def __del__(self) -> None:  # pragma: no cover - best-effort destructor
        try:
            self.close()
        except Exception:
            pass

    def sample_column(
        self,
        *,
        database: str,
        table: str,
        column: str,
        n: int,
        where: str | None = None,
    ) -> list[str]:
        n = max(1, int(n))
        where_clause = ""
        parts: list[str] = []
        if where and where.strip():
            parts.append(f"({where})")
        parts.append(f"{column} IS NOT NULL")
        if parts:
            where_clause = " WHERE " + " AND ".join(parts)

        # Order randomly to get samples; LIMIT caps the output size
        sql = f"SELECT {column} FROM {table}{where_clause} " f"ORDER BY rand() LIMIT {n}"
        qid = self._start_query(sql, database)
        self._wait(qid)
        rows = self._collect_results(qid, max_rows=n)

        values: list[str] = []
        seen: set[str] = set()
        for r in rows:
            if not r:
                continue
            v = r[0]
            if v is None:
                continue
            if v in seen:
                continue
            values.append(v)
            seen.add(v)
            if len(values) >= n:
                break
        return values

    # -------------------- Internals --------------------
    def _create_temp_workgroup(self) -> str:
        name = f"cps_tmp_{uuid.uuid4().hex[:8]}"
        cfg = {
            "ResultConfiguration": {
                "OutputLocation": self.cfg.s3_output,
            },
            "EnforceWorkGroupConfiguration": self.cfg.enforce_workgroup_configuration,
            "PublishCloudWatchMetricsEnabled": False,
            "RequesterPaysEnabled": False,
            "BytesScannedCutoffPerQuery": int(self.cfg.workgroup_bytes_scanned_cutoff),
        }
        self._athena.create_work_group(
            Name=name,
            Configuration=cfg,
            Description="cps temporary workgroup",
        )
        return name

    def _create_temp_db(self) -> str:
        name = f"{self.cfg.temp_db_prefix}_{uuid.uuid4().hex[:8]}"
        # Minimal database input; LocationUri is optional but helpful
        try:
            self._glue.create_database(
                DatabaseInput={
                    "Name": name,
                    "Description": "cps temporary athena database",
                    "LocationUri": self.cfg.s3_output.rstrip("/") + "/_temp_db/",
                    "Parameters": {"cps": "true"},
                }
            )
        except ClientError as e:  # pragma: no cover - unlikely, ignored
            code = e.response.get("Error", {}).get("Code")
            if code != "AlreadyExistsException":
                raise
        return name

    def _start_query(self, sql: str, database: str | None) -> str:
        # StartQueryExecution with workgroup and query context
        for attempt in range(self.cfg.max_retries):
            try:
                params: dict[str, Any] = {
                    "QueryString": sql,
                    "WorkGroup": self._wg_name or "primary",
                }
                if database:
                    params["QueryExecutionContext"] = {"Database": database}
                res = self._athena.start_query_execution(**params)  # type: ignore[arg-type]
                return str(res["QueryExecutionId"])  # type: ignore[no-any-return]
            except (ClientError, BotoCoreError):
                if attempt >= self.cfg.max_retries - 1:
                    raise
                self._sleep_backoff(attempt)
        raise RuntimeError("unreachable: start_query_execution retries exhausted")

    def _wait(self, qid: str) -> None:
        start = time.time()
        delay = self.cfg.initial_backoff
        last_state = "QUEUED"
        while True:
            if time.time() - start > self.cfg.max_wait_seconds:
                raise TimeoutError(
                    f"Athena query {qid} timed out waiting for results; last_state={last_state}"
                )
            try:
                info = self._athena.get_query_execution(QueryExecutionId=qid)
                q = info.get("QueryExecution", {})
                status = (q.get("Status") or {}).get("State")
                last_state = str(status)
                if status in {"SUCCEEDED"}:
                    return
                if status in {"FAILED", "CANCELLED"}:
                    reason = (q.get("Status") or {}).get("StateChangeReason")
                    raise RuntimeError(f"Athena query failed: {reason}")
            except (ClientError, BotoCoreError):
                # Treat as transient and continue
                pass
            self._sleep(delay)
            delay = min(self.cfg.max_backoff, delay * 1.7 + random.uniform(0, 0.2))

    def _collect_results(self, qid: str, *, max_rows: int | None = None) -> list[list[str | None]]:
        out: list[list[str | None]] = []
        token: str | None = None
        header_skipped = False
        while True:
            try:
                params = {"QueryExecutionId": qid}
                if token:
                    params["NextToken"] = token
                res = self._athena.get_query_results(**params)  # type: ignore[arg-type]
            except ClientError as e:
                code = e.response.get("Error", {}).get("Code")
                # Handle eventual consistency: results may not be immediately readable
                if code in {
                    "InvalidRequestException",
                    "ThrottlingException",
                    "TooManyRequestsException",
                }:
                    self._sleep(self.cfg.initial_backoff)
                    continue
                raise

            rs = res.get("ResultSet", {})
            rows = rs.get("Rows") or []
            for row in rows:
                data = row.get("Data") or []
                vals = [cell.get("VarCharValue") for cell in data]
                # Skip header line once: heuristic — if first page and first row
                if not header_skipped:
                    header_skipped = True
                    continue
                out.append(vals)
                if max_rows is not None and len(out) >= max_rows:
                    return out
            token = res.get("NextToken")
            if not token:
                break
        return out

    # -------------------- Utilities --------------------
    @staticmethod
    def _sleep(secs: float) -> None:
        time.sleep(max(0.0, float(secs)))

    def _sleep_backoff(self, attempt: int) -> None:
        base = self.cfg.initial_backoff * (1.5**attempt)
        jitter = random.uniform(0, 0.25)
        self._sleep(min(self.cfg.max_backoff, base + jitter))


__all__ = ["AthenaConfig", "AthenaSampler"]
