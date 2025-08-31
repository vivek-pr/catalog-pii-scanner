from __future__ import annotations

from collections.abc import Callable, Generator, Iterable
from contextlib import contextmanager
from typing import Any


class ConnectionPool:
    """Very small DB-API connection pool.

    - Accepts a `connect` callable that returns a DB-API connection.
    - Keeps up to `maxsize` idle connections for reuse.
    - Single-threaded friendly (no blocking/waiting semantics).
    """

    def __init__(self, connect: Callable[[], Any], maxsize: int = 4) -> None:
        if not callable(connect):
            raise TypeError("connect must be a callable returning a DB-API connection")
        self._connect = connect
        self._maxsize = max(1, int(maxsize))
        self._pool: list[Any] = []

    @contextmanager
    def acquire(self) -> Generator[Any, None, None]:
        # Reuse if possible; else create new
        conn = self._pool.pop() if self._pool else self._connect()
        try:
            yield conn
        finally:
            # Return to pool if not full; else close
            if len(self._pool) < self._maxsize:
                self._pool.append(conn)
            else:
                try:
                    conn.close()  # type: ignore[no-untyped-call]
                except Exception:
                    pass

    def closeall(self) -> None:
        while self._pool:
            conn = self._pool.pop()
            try:
                conn.close()  # type: ignore[no-untyped-call]
            except Exception:
                pass


class JDBCSampler:
    """Generic JDBC sampler for Hive/Spark/DBSQL-like engines.

    Features:
    - Simple connection pooling (or use a provided connection)
    - Randomized sampling via TABLESAMPLE when available
    - Fallback to ORDER BY RAND()/rand() LIMIT N, then plain LIMIT

    This class operates over DB-API connections and keeps SQL portable.
    """

    def __init__(
        self,
        *,
        conn: Any | None = None,
        connect: Callable[[], Any] | None = None,
        max_pool_size: int = 2,
        arraysize: int = 1000,
        prefer_tablesample: bool = True,
    ) -> None:
        if conn is None and connect is None:
            raise ValueError("Provide either an existing 'conn' or a 'connect' callable")
        if conn is not None and connect is not None:
            raise ValueError("Provide only one of 'conn' or 'connect', not both")

        self._conn = conn
        self._pool = ConnectionPool(connect, max_pool_size) if connect else None
        self.arraysize = max(1, int(arraysize))
        self.prefer_tablesample = prefer_tablesample

    def close(self) -> None:
        if self._pool is not None:
            self._pool.closeall()

    # ------------------------
    # Public API
    # ------------------------
    def sample_column(
        self,
        *,
        table: str,
        column: str,
        n: int,
        where: str | None = None,
    ) -> list[Any]:
        """Return up to N non-null sampled values from table.column.

        Attempts strategies in order until it collects N values:
          1) TABLESAMPLE with increasing percentage + LIMIT
          2) ORDER BY RAND()/rand() LIMIT
          3) Plain LIMIT
        """
        n = max(1, int(n))
        values: list[Any] = []
        seen: set[Any] = set()

        def add_rows(rows: list[tuple[Any, ...]] | Iterable[tuple[Any, ...]]) -> None:
            nonlocal values
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

        def _where_clause() -> str:
            parts: list[str] = []
            if where and where.strip():
                parts.append(f"({where})")
            parts.append(f"{column} IS NOT NULL")
            return (" WHERE " + " AND ".join(parts)) if parts else ""

        def _fetch(cur: Any, sql: str) -> None:
            cur.execute(sql)
            # Fetch at most 2n to allow filtering/uniques
            fetch = getattr(cur, "fetchmany", None)
            if callable(fetch):
                rows = cur.fetchmany(max(n * 2, self.arraysize))
            else:
                rows = cur.fetchall()
            add_rows(rows)

        def _run_sampling(conn: Any) -> None:
            # Helper to create/refresh cursor with arraysize
            def _new_cursor() -> Any:
                c = conn.cursor()
                try:
                    if hasattr(c, "arraysize"):
                        c.arraysize = self.arraysize
                except Exception:
                    pass
                return c

            cur = _new_cursor()

            def _on_error() -> None:
                nonlocal cur
                # Roll back aborted transaction if possible; recreate cursor
                try:
                    if hasattr(conn, "rollback"):
                        conn.rollback()
                except Exception:
                    pass
                try:
                    cur.close()
                except Exception:
                    pass
                cur = _new_cursor()

            # 1) TABLESAMPLE with ramping percentages
            if self.prefer_tablesample:
                for pct in (1, 2, 5, 10, 20, 50, 100):
                    if len(values) >= n:
                        break
                    try:
                        sql = (
                            f"SELECT {column} FROM {table} "
                            f"TABLESAMPLE ({pct} PERCENT)"
                            f"{_where_clause()} LIMIT {max(n * 2, 10)}"
                        )
                        _fetch(cur, sql)
                    except Exception:
                        # Likely unsupported; move to next strategy
                        _on_error()
                        break

            # 2) ORDER BY RAND()/rand() LIMIT
            if len(values) < n:
                for fn in ("RAND()", "rand()"):
                    if len(values) >= n:
                        break
                    try:
                        sql = (
                            f"SELECT {column} FROM {table}{_where_clause()} ORDER BY {fn} LIMIT {n}"
                        )
                        _fetch(cur, sql)
                        if len(values) >= n:
                            break
                    except Exception:
                        _on_error()
                        continue

            # 3) Plain LIMIT as last resort
            if len(values) < n:
                try:
                    sql = f"SELECT {column} FROM {table}{_where_clause()} LIMIT {max(n * 2, 10)}"
                    _fetch(cur, sql)
                except Exception:
                    # give up
                    _on_error()
                    pass

            # best-effort close
            try:
                cur.close()
            except Exception:
                pass

        # Acquire connection (pooled or direct)
        if self._pool is not None:
            with self._pool.acquire() as conn:
                _run_sampling(conn)
        else:
            assert self._conn is not None
            _run_sampling(self._conn)

        return values[:n]


__all__ = [
    "ConnectionPool",
    "JDBCSampler",
]
