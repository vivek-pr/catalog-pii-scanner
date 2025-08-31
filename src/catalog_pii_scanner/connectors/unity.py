from __future__ import annotations

import fnmatch
import os
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Any, cast

try:  # Optional at runtime; tests may mock
    import requests  # type: ignore
except Exception:  # pragma: no cover - optional dependency in some envs
    requests = None  # type: ignore


@dataclass
class UnityColumn:
    catalog: str
    schema: str
    table: str
    name: str
    type: str | None
    comment: str | None
    properties: dict[str, str]

    @property
    def ref(self) -> str:
        return f"unity://{self.catalog}/{self.schema}/{self.table}/{self.name}"


class UnityCatalogClient:
    """Databricks Unity Catalog client.

    - Enumerates columns using REST or JDBC (system.information_schema)
    - Writes back via SQL (preferred) or REST fallback (best-effort)

    Auth via env:
      - REST:  DATABRICKS_HOST, DATABRICKS_TOKEN
      - JDBC:  DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH
    """

    def __init__(
        self,
        *,
        host: str | None = None,
        token: str | None = None,
        http_path: str | None = None,
        session: Any | None = None,
        sql_conn: Any | None = None,
        fetch_size: int = 1000,
    ) -> None:
        self.host = host or os.getenv("DATABRICKS_HOST") or ""
        self.token = token or os.getenv("DATABRICKS_TOKEN") or ""
        self.http_path = http_path or os.getenv("DATABRICKS_HTTP_PATH") or ""
        self.fetch_size = max(1, int(fetch_size))

        self._session = session
        self._sql_conn = sql_conn

        # Lazily create a requests session if not provided
        if self._session is None and self.host:
            if requests is None:  # pragma: no cover - import guard
                raise RuntimeError(
                    "requests is required for UnityCatalogClient REST usage but is not installed"
                )
            s = requests.Session()
            s.headers.update({"Authorization": f"Bearer {self.token}"})
            self._session = s

    # ------------- Enumeration -------------

    def iter_columns(
        self,
        catalog_patterns: Iterable[str] | None = None,
        schema_patterns: Iterable[str] | None = None,
        table_patterns: Iterable[str] | None = None,
    ) -> Iterator[UnityColumn]:
        """Yield UnityColumn via JDBC if available, else REST traversal.

        Patterns use fnmatch semantics (e.g., '*', 'demo*').
        """
        cat_pats = list(catalog_patterns or ["*"])
        sch_pats = list(schema_patterns or ["*"])
        tbl_pats = list(table_patterns or ["*"])

        if self._sql_conn is not None:
            yield from self._iter_columns_sql(cat_pats, sch_pats, tbl_pats)
            return
        yield from self._iter_columns_rest(cat_pats, sch_pats, tbl_pats)

    # ----- JDBC path (system.information_schema) -----

    def _iter_columns_sql(
        self, cat_pats: list[str], sch_pats: list[str], tbl_pats: list[str]
    ) -> Iterator[UnityColumn]:
        q = (
            "SELECT table_catalog, table_schema, table_name, column_name, data_type, comment "
            "FROM system.information_schema.columns"
        )
        assert self._sql_conn is not None
        conn = cast(Any, self._sql_conn)
        cur = conn.cursor()
        cur.arraysize = self.fetch_size
        cur.execute(q)
        while True:
            rows = cur.fetchmany(self.fetch_size)
            if not rows:
                break
            for r in rows:
                catalog, schema, table, col, dtype, comment = r
                if not any(fnmatch.fnmatch(catalog, p) for p in cat_pats):
                    continue
                if not any(fnmatch.fnmatch(schema, p) for p in sch_pats):
                    continue
                if not any(fnmatch.fnmatch(table, p) for p in tbl_pats):
                    continue
                yield UnityColumn(
                    catalog=catalog,
                    schema=schema,
                    table=table,
                    name=col,
                    type=dtype,
                    comment=comment,
                    properties={},
                )

    # ----- REST path -----

    def _rest_get(self, path: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        if not self._session:
            raise RuntimeError("REST session not configured; provide host/token or session")
        url = self._join(self.host, path)
        resp = self._session.get(url, params=params or {})
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]

    def _rest_patch(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        if not self._session:
            raise RuntimeError("REST session not configured; provide host/token or session")
        url = self._join(self.host, path)
        resp = self._session.patch(url, json=body)
        resp.raise_for_status()
        return resp.json()  # type: ignore[no-any-return]

    @staticmethod
    def _join(host: str, path: str) -> str:
        host = host.rstrip("/")
        path = path.lstrip("/")
        return f"{host}/{path}"

    def list_catalogs(self) -> list[str]:
        out: list[str] = []
        token: str | None = None
        while True:
            params: dict[str, Any] = {"max_results": 1000}
            if token:
                params["page_token"] = token
            resp = self._rest_get("/api/2.1/unity-catalog/catalogs", params)
            for c in resp.get("catalogs", []) or []:
                name = c.get("name")
                if name:
                    out.append(name)  # type: ignore[arg-type]
            token = resp.get("next_page_token")
            if not token:
                break
        return out

    def list_schemas(self, catalog: str) -> list[str]:
        out: list[str] = []
        token: str | None = None
        while True:
            params: dict[str, Any] = {"catalog_name": catalog, "max_results": 1000}
            if token:
                params["page_token"] = token
            resp = self._rest_get("/api/2.1/unity-catalog/schemas", params)
            for s in resp.get("schemas", []) or []:
                name = s.get("name")
                if name:
                    out.append(name)  # type: ignore[arg-type]
            token = resp.get("next_page_token")
            if not token:
                break
        return out

    def list_tables(self, catalog: str, schema: str) -> list[str]:
        out: list[str] = []
        token: str | None = None
        while True:
            params: dict[str, Any] = {
                "catalog_name": catalog,
                "schema_name": schema,
                "max_results": 1000,
            }
            if token:
                params["page_token"] = token
            resp = self._rest_get("/api/2.1/unity-catalog/tables", params)
            for t in resp.get("tables", []) or []:
                name = t.get("name") or t.get("full_name")
                if name:
                    out.append(name)  # type: ignore[arg-type]
            token = resp.get("next_page_token")
            if not token:
                break
        return out

    def get_table(self, full_name: str) -> dict[str, Any]:
        # full_name is catalog.schema.table
        return self._rest_get(f"/api/2.1/unity-catalog/tables/{full_name}")

    def _iter_columns_rest(
        self, cat_pats: list[str], sch_pats: list[str], tbl_pats: list[str]
    ) -> Iterator[UnityColumn]:
        for cat in self.list_catalogs():
            if not any(fnmatch.fnmatch(cat, p) for p in cat_pats):
                continue
            for sch in self.list_schemas(cat):
                if not any(fnmatch.fnmatch(sch, p) for p in sch_pats):
                    continue
                for tname in self.list_tables(cat, sch):
                    # tname may be 'catalog.schema.table' or just table; normalize
                    full_name = tname
                    if full_name.count(".") == 0:
                        full_name = f"{cat}.{sch}.{tname}"
                    _, _, table = full_name.split(".", 2)
                    if not any(fnmatch.fnmatch(table, p) for p in tbl_pats):
                        continue
                    ti = self.get_table(full_name)
                    cols = ti.get("columns", []) or []
                    # Optional properties live at table-level
                    props: dict[str, str] = ti.get("properties") or {}
                    for c in cols:
                        name = c.get("name")
                        dtype = c.get("type_name") or c.get("type_text")
                        comment = c.get("comment")
                        yield UnityColumn(
                            catalog=cat,
                            schema=sch,
                            table=table,
                            name=name,  # type: ignore[arg-type]
                            type=dtype,  # type: ignore[arg-type]
                            comment=comment,  # type: ignore[arg-type]
                            properties=props,
                        )

    # ------------- Writeback -------------

    def update_column_tags(
        self,
        *,
        catalog: str,
        schema: str,
        table: str,
        column: str,
        pii: bool,
        pii_types: list[str] | None = None,
        append_comment: str | None = None,
    ) -> bool:
        """Idempotently set tags/comments using SQL if available, else REST patch.

        Returns True if any update is applied.
        """
        changed = False

        if self._sql_conn is not None:
            cur = self._sql_conn.cursor()
            # Table properties to hold tagging info (table-level key per column)
            desired_types = ",".join(sorted(t.strip() for t in (pii_types or []) if t.strip()))
            props: list[tuple[str, str]] = [
                (f"cps.pii.col.{column}", str(bool(pii)).lower()),
            ]
            if pii_types is not None:
                props.append((f"cps.pii_types.col.{column}", desired_types))

            if props:
                kv = ", ".join([f"'{k}'='{v}'" for k, v in props])
                cur.execute(f"ALTER TABLE {catalog}.{schema}.{table} SET TBLPROPERTIES ({kv})")
                changed = True

            if append_comment is not None:
                # Read existing
                get_q = (
                    "SELECT comment FROM system.information_schema.columns "
                    "WHERE table_catalog=? AND table_schema=? AND table_name=? AND column_name=?"
                )
                # Support both DB-API styles: qmark or format, try qmark first
                try:
                    cur.execute(get_q, (catalog, schema, table, column))
                except Exception:  # pragma: no cover - alt style fallback in tests
                    cur.execute(
                        get_q.replace("?", "%s"),
                        (catalog, schema, table, column),  # type: ignore[arg-type]
                    )
                row = cur.fetchone()
                existing_comment = (row[0] if row else None) or ""
                new_comment = existing_comment
                if append_comment and append_comment not in (existing_comment or ""):
                    new_comment = (
                        existing_comment + (" " if existing_comment else "") + append_comment
                    )[:1024]
                if new_comment != existing_comment:
                    cur.execute(
                        (
                            "COMMENT ON COLUMN "
                            f"{catalog}.{schema}.{table}.{column} IS ?".replace("?", "%s")
                            if "%s" in get_q
                            else f"COMMENT ON COLUMN {catalog}.{schema}.{table}.{column} IS ?"
                        ),
                        (new_comment,),
                    )
                    changed = True
            return changed

        # REST fallback: patch table with updated properties and column comment
        full_name = f"{catalog}.{schema}.{table}"
        ti = self.get_table(full_name)
        new_props = dict(ti.get("properties") or {})
        if str(new_props.get(f"cps.pii.col.{column}")).lower() != str(bool(pii)).lower():
            new_props[f"cps.pii.col.{column}"] = str(bool(pii)).lower()
            changed = True
        if pii_types is not None:
            desired_list = [t.strip() for t in pii_types if t.strip()]
            desired = ",".join(sorted(desired_list))
            if new_props.get(f"cps.pii_types.col.{column}") != desired:
                new_props[f"cps.pii_types.col.{column}"] = desired
                changed = True

        cols = list(ti.get("columns") or [])
        for c in cols:
            if c.get("name") != column:
                continue
            if append_comment:
                existing_comment2: str = c.get("comment") or ""
                if append_comment not in (existing_comment2 or ""):
                    c["comment"] = (
                        existing_comment2 + (" " if existing_comment2 else "") + append_comment
                    )[:1024]
                    changed = True
            break

        if not changed:
            return False
        body: dict[str, Any] = {"full_name": full_name, "properties": new_props, "columns": cols}
        # Some deployments require name in body; we include both defensively
        self._rest_patch(f"/api/2.1/unity-catalog/tables/{full_name}", body)
        return True


__all__ = [
    "UnityCatalogClient",
    "UnityColumn",
]
