from __future__ import annotations

import copy
import os
import random
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any, cast

try:
    import boto3  # type: ignore
    from botocore.exceptions import ClientError  # type: ignore
except Exception:  # pragma: no cover - optional dependency in some envs
    boto3 = None  # type: ignore
    ClientError = Exception  # type: ignore


# --------- Retry / backoff helpers ---------


def _is_throttle_error(err: Exception) -> bool:
    try:
        if isinstance(err, ClientError):  # type: ignore[misc]
            code = err.response.get("Error", {}).get("Code")
            return code in {
                "ThrottlingException",
                "TooManyRequestsException",
                "RequestLimitExceeded",
            }
    except Exception:  # pragma: no cover - defensive
        return False
    return False


def _with_retries(fn: Callable[[], Any], *, max_retries: int = 5, base_delay: float = 0.5) -> Any:
    """Call `fn` with exponential backoff on throttling-like errors."""
    attempt = 0
    while True:
        try:
            return fn()
        except Exception as e:  # noqa: BLE001
            if attempt >= max_retries or not _is_throttle_error(e):
                raise
            # Exponential backoff with jitter
            delay = base_delay * (2**attempt) * (0.5 + random.random())
            time.sleep(min(delay, 8.0))
            attempt += 1


# --------- Data structures ---------


@dataclass
class GlueColumn:
    database: str
    table: str
    name: str
    type: str | None
    comment: str | None
    parameters: dict[str, str]

    @property
    def ref(self) -> str:
        return f"glue://{self.database}/{self.table}/{self.name}"


# --------- Client wrapper ---------


class GlueCatalogClient:
    """Thin wrapper over boto3 Glue client with safe defaults and retries."""

    def __init__(
        self,
        *,
        region_name: str | None = None,
        endpoint_url: str | None = None,
        boto3_client: Any | None = None,
        max_retries: int = 5,
        base_delay: float = 0.5,
    ) -> None:
        if boto3_client is not None:
            self._client = boto3_client
        else:
            if boto3 is None:
                raise RuntimeError("boto3 is required for GlueCatalogClient but is not installed")
            # Allow overriding via env (useful for Localstack)
            region_name = region_name or os.getenv("AWS_REGION") or "us-east-1"
            endpoint_url = (
                endpoint_url or os.getenv("AWS_ENDPOINT_URL") or os.getenv("GLUE_ENDPOINT_URL")
            )
            self._client = boto3.client("glue", region_name=region_name, endpoint_url=endpoint_url)

        self._max_retries = max_retries
        self._base_delay = base_delay

    # ----- Enumeration -----

    def list_databases(self) -> list[str]:
        next_token: str | None = None
        names: list[str] = []
        while True:

            def _call(token: str | None = next_token) -> dict[str, Any]:  # bind loop var
                if token:
                    return cast(dict[str, Any], self._client.get_databases(NextToken=token))
                return cast(dict[str, Any], self._client.get_databases())

            resp = cast(
                dict[str, Any],
                _with_retries(_call, max_retries=self._max_retries, base_delay=self._base_delay),
            )
            for db in resp.get("DatabaseList", []) or []:
                if db.get("Name"):
                    names.append(db["Name"])  # type: ignore[index]
            next_token = resp.get("NextToken")
            if not next_token:
                break
        return names

    def list_tables(self, database: str) -> list[dict[str, Any]]:
        next_token: str | None = None
        out: list[dict[str, Any]] = []
        while True:

            def _call(token: str | None = next_token) -> dict[str, Any]:  # bind loop var
                if token:
                    return cast(
                        dict[str, Any],
                        self._client.get_tables(DatabaseName=database, NextToken=token),
                    )
                return cast(dict[str, Any], self._client.get_tables(DatabaseName=database))

            resp = _with_retries(_call, max_retries=self._max_retries, base_delay=self._base_delay)
            out.extend(resp.get("TableList", []) or [])
            next_token = resp.get("NextToken")
            if not next_token:
                break
        return out

    def iter_columns(
        self,
        db_patterns: Iterable[str] | None = None,
        table_patterns: Iterable[str] | None = None,
    ) -> Iterable[GlueColumn]:
        import fnmatch

        db_pats = list(db_patterns or ["*"])
        tbl_pats = list(table_patterns or ["*"])

        for db in self.list_databases():
            if not any(fnmatch.fnmatch(db, p) for p in db_pats):
                continue
            for tbl in self.list_tables(db):
                name = tbl.get("Name")
                if not name:
                    continue
                if not any(fnmatch.fnmatch(name, p) for p in tbl_pats):
                    continue
                sd = tbl.get("StorageDescriptor") or {}
                cols = sd.get("Columns") or []
                for c in cols:
                    yield GlueColumn(
                        database=db,
                        table=name,  # type: ignore[arg-type]
                        name=c.get("Name"),  # type: ignore[arg-type]
                        type=c.get("Type"),  # type: ignore[arg-type]
                        comment=c.get("Comment"),  # type: ignore[arg-type]
                        parameters=c.get("Parameters") or {},
                    )

    # ----- Writeback (idempotent) -----

    def get_table(self, database: str, table: str) -> dict[str, Any]:
        def _call() -> dict[str, Any]:
            return cast(dict[str, Any], self._client.get_table(DatabaseName=database, Name=table))

        return cast(
            dict[str, Any],
            _with_retries(_call, max_retries=self._max_retries, base_delay=self._base_delay),
        )

    def update_column_tags(
        self,
        *,
        database: str,
        table: str,
        column: str,
        pii: bool,
        pii_types: list[str] | None = None,
        append_comment: str | None = None,
    ) -> bool:
        """Update column parameters and optionally append comment.

        Returns True if an update was applied; False if no changes needed.
        """
        tbl: dict[str, Any] = self.get_table(database, table).get("Table", {})
        tbl_input = _table_to_input(tbl)

        sd = tbl_input.get("StorageDescriptor") or {}
        cols = sd.get("Columns") or []
        changed = False
        for c in cols:
            if c.get("Name") != column:
                continue
            params = c.get("Parameters") or {}
            new_params = dict(params)
            # idempotent parameter updates
            if str(new_params.get("pii")).lower() != str(bool(pii)).lower():
                new_params["pii"] = str(bool(pii)).lower()
            if pii_types is not None:
                desired_list = [t.strip() for t in pii_types if t.strip()]
                desired = ",".join(sorted(desired_list))
                if new_params.get("pii_types") != desired:
                    new_params["pii_types"] = desired
            if new_params != params:
                c["Parameters"] = new_params
                changed = True

            if append_comment:
                existing: str = c.get("Comment") or ""
                if append_comment not in (existing or ""):
                    c["Comment"] = (existing + (" " if existing else "") + append_comment)[:255]
                    changed = True
            break

        if not changed:
            return False

        def _call() -> Any:
            return self._client.update_table(DatabaseName=database, TableInput=tbl_input)

        _with_retries(_call, max_retries=self._max_retries, base_delay=self._base_delay)
        return True


# --------- Helpers ---------


def _table_to_input(tbl: dict[str, Any]) -> dict[str, Any]:
    """Convert Glue GetTable output to a valid TableInput for UpdateTable.

    Strictly whitelist allowed TableInput fields and sanitize nested shapes to
    avoid InvalidInputException from read-only/unknown fields in GetTable output.
    """
    allowed_table_keys = {
        # Required
        "Name",
        # Optional
        "Description",
        "Owner",
        "Retention",
        "StorageDescriptor",
        "PartitionKeys",
        "ViewOriginalText",
        "ViewExpandedText",
        "TableType",
        "Parameters",
        "TargetTable",
    }

    def _sanitize_column(col: dict[str, Any]) -> dict[str, Any]:
        allowed = {"Name", "Type", "Comment", "Parameters"}
        return {k: v for k, v in col.items() if k in allowed}

    def _sanitize_serde(info: dict[str, Any]) -> dict[str, Any]:
        allowed = {"Name", "SerializationLibrary", "Parameters"}
        return {k: v for k, v in info.items() if k in allowed}

    def _sanitize_order(ord_: dict[str, Any]) -> dict[str, Any]:
        allowed = {"Column", "SortOrder"}
        return {k: v for k, v in ord_.items() if k in allowed}

    def _sanitize_skewed(info: dict[str, Any]) -> dict[str, Any]:
        allowed = {
            "SkewedColumnNames",
            "SkewedColumnValues",
            "SkewedColumnValueLocationMaps",
        }
        return {k: v for k, v in info.items() if k in allowed}

    def _sanitize_schema_ref(ref: dict[str, Any]) -> dict[str, Any]:
        allowed = {"SchemaId", "SchemaVersionId", "SchemaVersionNumber"}
        out = {k: v for k, v in ref.items() if k in allowed}
        if "SchemaId" in out and isinstance(out["SchemaId"], dict):
            sid_allowed = {"SchemaArn", "SchemaName", "RegistryName"}
            out["SchemaId"] = {k: v for k, v in out["SchemaId"].items() if k in sid_allowed}
        return out

    def _sanitize_storage_descriptor(sd: dict[str, Any]) -> dict[str, Any]:
        allowed = {
            "Columns",
            "Location",
            "AdditionalLocations",
            "InputFormat",
            "OutputFormat",
            "Compressed",
            "NumberOfBuckets",
            "SerdeInfo",
            "BucketColumns",
            "SortColumns",
            "Parameters",
            "SkewedInfo",
            "StoredAsSubDirectories",
            "SchemaReference",
        }
        sdo = {k: v for k, v in sd.items() if k in allowed}
        if "Columns" in sdo and isinstance(sdo["Columns"], list):
            sdo["Columns"] = [_sanitize_column(c) for c in sdo["Columns"] if isinstance(c, dict)]
        if "SerdeInfo" in sdo and isinstance(sdo["SerdeInfo"], dict):
            sdo["SerdeInfo"] = _sanitize_serde(sdo["SerdeInfo"])  # type: ignore[assignment]
        if "SortColumns" in sdo and isinstance(sdo["SortColumns"], list):
            sdo["SortColumns"] = [
                _sanitize_order(o) for o in sdo["SortColumns"] if isinstance(o, dict)
            ]
        if "SkewedInfo" in sdo and isinstance(sdo["SkewedInfo"], dict):
            sdo["SkewedInfo"] = _sanitize_skewed(sdo["SkewedInfo"])  # type: ignore[assignment]
        if "SchemaReference" in sdo and isinstance(sdo["SchemaReference"], dict):
            sdo["SchemaReference"] = _sanitize_schema_ref(sdo["SchemaReference"])  # type: ignore[assignment]
        return sdo

    def _sanitize_partition_keys(pks: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return [_sanitize_column(c) for c in pks if isinstance(c, dict)]

    def _sanitize_target_table(tt: dict[str, Any]) -> dict[str, Any]:
        allowed = {"CatalogId", "DatabaseName", "Name"}
        return {k: v for k, v in tt.items() if k in allowed}

    # Build whitelisted table
    ti: dict[str, Any] = {}
    for k in allowed_table_keys:
        if k not in tbl:
            continue
        v = tbl[k]
        if k == "StorageDescriptor" and isinstance(v, dict):
            ti[k] = _sanitize_storage_descriptor(v)
        elif k == "PartitionKeys" and isinstance(v, list):
            ti[k] = _sanitize_partition_keys(v)
        elif k == "TargetTable" and isinstance(v, dict):
            ti[k] = _sanitize_target_table(v)
        else:
            ti[k] = copy.deepcopy(v)

    # Ensure minimal required defaults exist
    ti.setdefault("Name", tbl.get("Name"))
    ti.setdefault(
        "StorageDescriptor",
        _sanitize_storage_descriptor(tbl.get("StorageDescriptor", {}) or {}),
    )
    ti.setdefault("Parameters", tbl.get("Parameters") or {})
    ti.setdefault("TableType", tbl.get("TableType") or "EXTERNAL_TABLE")
    return ti


__all__ = [
    "GlueCatalogClient",
    "GlueColumn",
]
