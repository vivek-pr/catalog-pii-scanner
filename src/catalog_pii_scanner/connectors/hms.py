from __future__ import annotations

import fnmatch
import os
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Any, cast

try:  # Optional dependency; tests may mock
    from hmsclient import hmsclient as _hms
    from hmsclient.genthrift.hive_metastore import ttypes as _ttypes
except Exception:  # pragma: no cover - optional dependency guard
    _hms = None  # type: ignore
    _ttypes = None  # type: ignore


@dataclass
class HMSColumn:
    database: str
    table: str
    name: str
    type: str | None
    comment: str | None
    properties: dict[str, str]

    @property
    def ref(self) -> str:
        return f"hms://{self.database}/{self.table}/{self.name}"


class HiveMetastoreClient:
    """Hive Metastore (Thrift) client wrapper.

    - Enumerates columns via Thrift `get_table`
    - Writes back by altering table column comments and table parameters

    Env defaults:
      - `HMS_HOST` (default: localhost)
      - `HMS_PORT` (default: 9083)
    """

    def __init__(
        self,
        *,
        host: str | None = None,
        port: int | None = None,
        raw_client: Any | None = None,
    ) -> None:
        if raw_client is not None:
            self._client = raw_client
        else:
            if _hms is None:  # pragma: no cover - import guard
                raise RuntimeError(
                    "hmsclient is required for HiveMetastoreClient but is not installed"
                )
            host = host or os.getenv("HMS_HOST") or "localhost"
            port = int(port or int(os.getenv("HMS_PORT", "9083")))
            # hmsclient is a context manager; open a long-lived connection
            cli = _hms.HMSClient(host=host, port=port)
            cli.open()
            self._client = cli

    # ------------- Enumeration -------------

    def list_databases(self) -> list[str]:
        dbs = cast(list[str], self._client.get_all_databases())
        return sorted(dbs)

    def list_tables(self, database: str) -> list[str]:
        tbls = cast(list[str], self._client.get_all_tables(database))
        return sorted(tbls)

    def get_table(self, database: str, table: str) -> Any:
        return self._client.get_table(database, table)

    def iter_columns(
        self,
        db_patterns: Iterable[str] | None = None,
        table_patterns: Iterable[str] | None = None,
    ) -> Iterator[HMSColumn]:
        db_pats = list(db_patterns or ["*"])
        tbl_pats = list(table_patterns or ["*"])
        for db in self.list_databases():
            if not any(fnmatch.fnmatch(db, p) for p in db_pats):
                continue
            for tname in self.list_tables(db):
                if not any(fnmatch.fnmatch(tname, p) for p in tbl_pats):
                    continue
                t = self.get_table(db, tname)
                sd = getattr(t, "sd", None)
                cols = getattr(sd, "cols", []) or []
                props = cast(dict[str, str], getattr(t, "parameters", {}) or {})
                for c in cols:
                    name = getattr(c, "name", None)
                    dtype = getattr(c, "type", None)
                    comment = getattr(c, "comment", None)
                    if not name:
                        continue
                    yield HMSColumn(
                        database=db,
                        table=tname,
                        name=name,
                        type=dtype,
                        comment=comment,
                        properties=props,
                    )

    # ------------- Writeback -------------

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
        """Idempotently update table parameters and column comment via alter_table.

        Returns True if any change was applied.
        """
        t = self.get_table(database, table)
        changed = False

        # Update table-level parameters to reflect column tagging
        params = cast(dict[str, str], getattr(t, "parameters", {}) or {})
        new_params = dict(params)
        key_enabled = f"cps.pii.col.{column}"
        if str(new_params.get(key_enabled)).lower() != str(bool(pii)).lower():
            new_params[key_enabled] = str(bool(pii)).lower()
            changed = True
        if pii_types is not None:
            desired = ",".join(sorted(t.strip() for t in pii_types if t.strip()))
            key_types = f"cps.pii_types.col.{column}"
            if new_params.get(key_types) != desired:
                new_params[key_types] = desired
                changed = True
        if new_params != params:
            t.parameters = new_params  # type: ignore[attr-defined]

        # Update column comment if needed
        sd = getattr(t, "sd", None)
        cols = getattr(sd, "cols", []) or []
        for c in cols:
            if getattr(c, "name", None) != column:
                continue
            if append_comment:
                existing = cast(str, getattr(c, "comment", None) or "")
                if append_comment not in existing:
                    new_comment = (existing + (" " if existing else "") + append_comment)[:255]
                    c.comment = new_comment  # type: ignore[attr-defined]
                    changed = True
            break

        if not changed:
            return False

        # Apply via alter_table
        self._client.alter_table(database, table, t)
        return True


__all__ = [
    "HiveMetastoreClient",
    "HMSColumn",
]
