from __future__ import annotations

import json
from typing import Any, cast

from typer.testing import CliRunner

from catalog_pii_scanner.cli import app
from catalog_pii_scanner.connectors.unity import UnityCatalogClient


class _FakeResp:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:  # no-op
        return None

    def json(self) -> dict[str, Any]:
        return self._payload


class _FakeSession:
    def __init__(self) -> None:
        self.headers: dict[str, str] = {}
        # Minimal in-memory table registry
        self._table: dict[str, Any] = {
            "full_name": "demo.public.users",
            "properties": {},
            "columns": [
                {"name": "id", "type_text": "int", "comment": None},
                {"name": "email", "type_text": "string", "comment": "user email"},
            ],
        }
        self._patches: list[dict[str, Any]] = []

    # Simple router
    def get(self, url: str, params: dict[str, Any] | None = None) -> _FakeResp:  # type: ignore[override]
        params = params or {}
        if url.endswith("/api/2.1/unity-catalog/catalogs"):
            # two-page pagination demo
            if "page_token" not in params:
                return _FakeResp({"catalogs": [{"name": "demo"}], "next_page_token": "p2"})
            return _FakeResp({"catalogs": [{"name": "demob"}]})
        if url.endswith("/api/2.1/unity-catalog/schemas"):
            return _FakeResp({"schemas": [{"name": "public"}], "next_page_token": None})
        if url.endswith("/api/2.1/unity-catalog/tables"):
            # tables list pagination
            if "page_token" not in params:
                return _FakeResp({"tables": [{"name": "users"}], "next_page_token": "t2"})
            return _FakeResp({"tables": [{"name": "users2"}], "next_page_token": None})
        if url.endswith("/api/2.1/unity-catalog/tables/demo.public.users"):
            return _FakeResp(self._table)
        if url.endswith("/api/2.1/unity-catalog/tables/demob.public.users"):
            return _FakeResp(self._table | {"full_name": "demob.public.users"})
        if url.endswith("/api/2.1/unity-catalog/tables/demo.public.users2"):
            # second table with no columns
            return _FakeResp({"full_name": "demo.public.users2", "columns": []})
        if url.endswith("/api/2.1/unity-catalog/tables/demob.public.users2"):
            return _FakeResp({"full_name": "demob.public.users2", "columns": []})
        raise AssertionError(f"unexpected GET {url} {params}")

    def patch(self, url: str, json: dict[str, Any]) -> _FakeResp:  # type: ignore[override]
        # apply minimal updates to in-memory registry
        assert url.endswith("/api/2.1/unity-catalog/tables/demo.public.users")
        self._patches.append({"url": url, "json": json})
        props = json.get("properties") or {}
        cols = json.get("columns") or []
        # merge properties
        props_map = cast(dict[str, Any], self._table["properties"])
        props_map.update(props)
        # update column comments when present
        col_list = cast(list[dict[str, Any]], self._table["columns"])
        for c in cols:
            for ec in col_list:
                if ec["name"] == c.get("name") and c.get("comment") is not None:
                    ec["comment"] = c.get("comment")
        return _FakeResp(self._table)


def test_unity_rest_pagination_and_writeback(monkeypatch: Any) -> None:
    fake = _FakeSession()

    # Force CLI to use our client with fake session
    client = UnityCatalogClient(host="https://example", token="t", session=fake)
    monkeypatch.setattr(
        "catalog_pii_scanner.cli.UnityCatalogClient",
        lambda: client,
        raising=True,
    )

    runner = CliRunner()
    res = runner.invoke(
        app,
        [
            "scan",
            "--target",
            "unity://*",
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

    # properties set and comment appended once
    props = fake._table["properties"]
    assert props.get("cps.pii.col.email") == "true"
    assert props.get("cps.pii_types.col.email") == "EMAIL"
    ec = next(c for c in fake._table["columns"] if c["name"] == "email")
    assert "PII detected" in (ec.get("comment") or "")

    # idempotent second run (no duplicate comment)
    res2 = runner.invoke(
        app,
        [
            "scan",
            "--target",
            "unity://*",
            "--apply",
            "--type",
            "EMAIL",
            "--append-comment",
            "PII detected",
        ],
    )
    assert res2.exit_code == 0
    ec2 = next(c for c in fake._table["columns"] if c["name"] == "email")
    assert (ec2.get("comment") or "").count("PII detected") == 1


class _FakeCursor:
    def __init__(self, rows: list[tuple[str, str, str, str, str, str | None]]) -> None:
        self._rows = rows
        self._pos = 0
        self.arraysize = 1000
        self._last_query = ""
        self._executed: list[tuple[str, tuple[Any, ...] | None]] = []
        # existing comments registry
        self._comments = {("demo", "public", "users", "email"): "user email"}

    def execute(self, q: str, params: tuple[Any, ...] | None = None) -> None:
        self._last_query = q
        self._executed.append((q, params))
        # Reset fetch position for a new listing query
        if "FROM system.information_schema.columns" in q and params is None:
            self._pos = 0

    def fetchmany(self, n: int) -> list[tuple[Any, ...]]:
        if "FROM system.information_schema.columns" not in self._last_query:
            return []
        start = self._pos
        end = min(len(self._rows), start + n)
        self._pos = end
        return self._rows[start:end]

    def fetchone(self) -> tuple[Any, ...] | None:
        # For comment lookup query
        if "SELECT comment FROM system.information_schema.columns" in self._last_query:
            # naive lookup by params in the last executed statement
            _, params = self._executed[-1]
            assert params is not None
            key = (params[0], params[1], params[2], params[3])
            return (self._comments.get(key),)
        return None


class _FakeSQLConn:
    def __init__(self, rows: list[tuple[str, str, str, str, str, str | None]]) -> None:
        self.cur = _FakeCursor(rows)

    def cursor(self) -> _FakeCursor:
        return self.cur


def test_unity_jdbc_pagination_and_writeback() -> None:
    rows: list[tuple[str, str, str, str, str, str | None]] = [
        ("demo", "public", "users", "id", "int", None),
        ("demo", "public", "users", "email", "string", "user email"),
        ("demo", "public", "orders", "order_id", "int", None),
    ]
    sql = _FakeSQLConn(rows)
    client = UnityCatalogClient(sql_conn=sql)

    cols = list(client.iter_columns(["demo"], ["public"], ["*"]))
    assert len(cols) == 3

    changed = client.update_column_tags(
        catalog="demo",
        schema="public",
        table="users",
        column="email",
        pii=True,
        pii_types=["EMAIL"],
        append_comment="PII detected",
    )
    assert changed is True

    # Validate SQL executed for properties and comment
    executed = [q for q, _ in sql.cur._executed]
    assert any(q.startswith("ALTER TABLE demo.public.users SET TBLPROPERTIES") for q in executed)
    assert any(q.startswith("COMMENT ON COLUMN demo.public.users.email IS") for q in executed)
