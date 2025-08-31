from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from catalog_pii_scanner.sampler import JDBCSampler


class _FakeCursor:
    def __init__(
        self,
        *,
        rows: list[Any],
        support_tablesample: bool,
        support_rand: bool,
        record: list[str],
    ) -> None:
        self._all_rows = rows
        self._results: list[tuple[Any]] = []
        self.arraysize = 1
        self._support_ts = support_tablesample
        self._support_rand = support_rand
        self._record = record

    def execute(self, sql: str, params: Iterable[Any] | None = None) -> None:  # noqa: ARG002
        self._record.append(sql)
        s = sql.lower()
        if "tablesample" in s and not self._support_ts:
            raise RuntimeError("TABLESAMPLE not supported")
        if "order by rand()" in s and not self._support_rand:
            raise RuntimeError("RAND not supported")
        if "order by rand(" in s and not self._support_rand:
            raise RuntimeError("rand not supported")

        # Naive LIMIT parser
        lim = 10
        if " limit " in s:
            try:
                lim = int(s.split(" limit ")[-1].split()[0])
            except Exception:
                lim = 10
        # Filter out Nones as the sampler would
        base = [r for r in self._all_rows if r is not None]
        self._results = [(v,) for v in base[:lim]]

    def fetchmany(self, n: int) -> list[tuple[Any, ...]]:
        out = self._results[:n]
        self._results = self._results[n:]
        return out

    def fetchall(self) -> list[tuple[Any, ...]]:
        out = self._results
        self._results = []
        return out

    def close(self) -> None:  # pragma: no cover - not essential for logic
        pass


class _FakeConn:
    def __init__(
        self,
        *,
        rows: list[Any],
        support_tablesample: bool = True,
        support_rand: bool = True,
        record: list[str] | None = None,
    ) -> None:
        self.rows = rows
        self.support_tablesample = support_tablesample
        self.support_rand = support_rand
        self.record = record if record is not None else []
        self.closed = False

    def cursor(self) -> _FakeCursor:
        return _FakeCursor(
            rows=self.rows,
            support_tablesample=self.support_tablesample,
            support_rand=self.support_rand,
            record=self.record,
        )

    def close(self) -> None:  # pragma: no cover - not essential for logic
        self.closed = True


def test_jdbc_sampler_tablesample_first() -> None:
    record: list[str] = []
    fake = _FakeConn(rows=["a", "b", None, "c", "d"], record=record)

    sampler = JDBCSampler(conn=fake, prefer_tablesample=True)
    out = sampler.sample_column(table="demo.public.users", column="email", n=3)

    assert out == ["a", "b", "c"]
    # First strategy should be TABLESAMPLE
    assert any("tablesample" in s.lower() for s in record)


def test_jdbc_sampler_fallback_to_rand() -> None:
    record: list[str] = []
    fake = _FakeConn(
        rows=[1, 2, 3, 4, 5], support_tablesample=False, support_rand=True, record=record
    )

    sampler = JDBCSampler(conn=fake, prefer_tablesample=True)
    out = sampler.sample_column(table="db.tbl", column="x", n=2)

    assert out == [1, 2]
    # Should have used ORDER BY RAND()
    assert any("order by rand" in s.lower() for s in record)


def test_jdbc_sampler_fallback_to_limit_only() -> None:
    record: list[str] = []
    fake = _FakeConn(
        rows=["u1", "u2", "u3"],
        support_tablesample=False,
        support_rand=False,
        record=record,
    )

    sampler = JDBCSampler(conn=fake)
    out = sampler.sample_column(table="db.tbl", column="user", n=2)
    assert out == ["u1", "u2"]
    # Should fall back to LIMIT
    assert any(" limit " in s.lower() and "order by" not in s.lower() for s in record)


def test_jdbc_sampler_connection_pooling_reuse() -> None:
    # Connect callable should be called only once due to pooling
    calls = {"n": 0}
    record1: list[str] = []
    record2: list[str] = []

    def _connect() -> _FakeConn:
        calls["n"] += 1
        # each connection gets its own record to ensure we reuse the same one
        rec = record1 if calls["n"] == 1 else record2
        return _FakeConn(rows=[10, 20, 30, 40], record=rec)

    sampler = JDBCSampler(connect=_connect, max_pool_size=1)

    out1 = sampler.sample_column(table="t", column="c", n=2)
    out2 = sampler.sample_column(table="t", column="c", n=2)

    assert out1 == [10, 20]
    assert out2 == [10, 20]
    assert calls["n"] == 1  # connection reused from pool
    # All queries should land on the same underlying connection's record
    assert record2 == []
