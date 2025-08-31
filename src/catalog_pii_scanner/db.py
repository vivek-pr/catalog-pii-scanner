from __future__ import annotations

from collections.abc import Generator, Iterable
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import cast

from sqlalchemy import (
    DateTime,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    String,
    UniqueConstraint,
    create_engine,
    select,
)
from sqlalchemy.dialects.sqlite import JSON as SQLITE_JSON
from sqlalchemy.engine import Engine
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    Session,
    mapped_column,
    relationship,
    sessionmaker,
)

NAMING_CONVENTION = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


class Base(DeclarativeBase):
    metadata = MetaData(naming_convention=NAMING_CONVENTION)


class Catalog(Base):
    __tablename__ = "catalogs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), unique=True, nullable=False)

    schemas: Mapped[list[Schema]] = relationship(
        back_populates="catalog", cascade="all, delete-orphan"
    )  # type: ignore[name-defined]

    def __repr__(self) -> str:  # pragma: no cover - debug only
        return f"Catalog(id={self.id!r}, name={self.name!r})"


class Schema(Base):
    __tablename__ = "schemas"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    catalog_id: Mapped[int] = mapped_column(ForeignKey("catalogs.id", ondelete="CASCADE"))

    catalog: Mapped[Catalog] = relationship(back_populates="schemas")
    tables: Mapped[list[Table]] = relationship(  # type: ignore[name-defined]
        back_populates="schema", cascade="all, delete-orphan"
    )

    __table_args__ = (UniqueConstraint("catalog_id", "name"),)


class Table(Base):
    __tablename__ = "tables"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    schema_id: Mapped[int] = mapped_column(ForeignKey("schemas.id", ondelete="CASCADE"))

    schema: Mapped[Schema] = relationship(back_populates="tables")
    columns: Mapped[list[Column]] = relationship(  # type: ignore[name-defined]
        back_populates="table", cascade="all, delete-orphan"
    )

    __table_args__ = (UniqueConstraint("schema_id", "name"),)


class Column(Base):
    __tablename__ = "columns"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    data_type: Mapped[str | None] = mapped_column(String(128), nullable=True)
    description: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    table_id: Mapped[int] = mapped_column(ForeignKey("tables.id", ondelete="CASCADE"))

    table: Mapped[Table] = relationship(back_populates="columns")
    findings: Mapped[list[Finding]] = relationship(  # type: ignore[name-defined]
        back_populates="column", cascade="all, delete-orphan"
    )

    __table_args__ = (UniqueConstraint("table_id", "name"),)

    @property
    def ref(self) -> str:
        sch = self.table.schema
        cat = sch.catalog
        return f"{cat.name}.{sch.name}.{self.table.name}.{self.name}"


class Finding(Base):
    __tablename__ = "findings"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    column_id: Mapped[int] = mapped_column(ForeignKey("columns.id", ondelete="CASCADE"))
    # Store as JSON array for cross-DB compatibility (SQLite/PG)
    types: Mapped[list[str]] = mapped_column(SQLITE_JSON, nullable=False, default=list)
    confidence: Mapped[float] = mapped_column(Float, nullable=False)
    hit_rate: Mapped[float] = mapped_column(Float, nullable=False)
    model_version: Mapped[str] = mapped_column(String(64), nullable=False)
    scanned_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    source: Mapped[str] = mapped_column(String(64), nullable=False)
    # Denormalized reference for easy export/lookups
    column_ref: Mapped[str] = mapped_column(String(1024), nullable=False)

    column: Mapped[Column] = relationship(back_populates="findings")


def create_engine_for_url(url: str) -> Engine:
    # Allow SQLite multi-thread access for CLI/tests
    if url.startswith("sqlite"):  # sqlite:///file.db or sqlite:///:memory:
        engine = create_engine(url, connect_args={"check_same_thread": False})
    else:
        engine = create_engine(url)
    return engine


def init_db(url: str) -> sessionmaker[Session]:
    engine = create_engine_for_url(url)
    Base.metadata.create_all(engine)
    return sessionmaker(bind=engine, expire_on_commit=False)


@contextmanager
def session_scope(Session: sessionmaker[Session]) -> Generator[Session, None, None]:
    """Provide a transactional scope around a series of operations."""
    session = Session()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def upsert_column(
    session: Session,
    catalog: str,
    schema: str,
    table: str,
    column: str,
    *,
    data_type: str | None = None,
    description: str | None = None,
) -> Column:
    cat = cast(
        Catalog | None,
        session.execute(select(Catalog).where(Catalog.name == catalog)).scalar_one_or_none(),
    )
    if not cat:
        cat = Catalog(name=catalog)
        session.add(cat)
        session.flush()

    sch = cast(
        Schema | None,
        session.execute(
            select(Schema).where(Schema.catalog_id == cat.id, Schema.name == schema)
        ).scalar_one_or_none(),
    )
    if not sch:
        sch = Schema(name=schema, catalog_id=cat.id)
        session.add(sch)
        session.flush()

    tbl = cast(
        Table | None,
        session.execute(
            select(Table).where(Table.schema_id == sch.id, Table.name == table)
        ).scalar_one_or_none(),
    )
    if not tbl:
        tbl = Table(name=table, schema_id=sch.id)
        session.add(tbl)
        session.flush()

    col = cast(
        Column | None,
        session.execute(
            select(Column).where(Column.table_id == tbl.id, Column.name == column)
        ).scalar_one_or_none(),
    )
    if not col:
        col = Column(name=column, table_id=tbl.id, data_type=data_type, description=description)
        session.add(col)
    else:
        # update optional metadata if provided
        if data_type is not None:
            col.data_type = data_type
        if description is not None:
            col.description = description
    session.flush()
    return col


def add_finding(
    session: Session,
    column: Column,
    *,
    types: Iterable[str],
    confidence: float,
    hit_rate: float,
    model_version: str,
    source: str,
    scanned_at: datetime | None = None,
) -> Finding:
    ts = scanned_at or datetime.now(UTC)
    f = Finding(
        column_id=column.id,
        types=list(types),
        confidence=float(confidence),
        hit_rate=float(hit_rate),
        model_version=model_version,
        scanned_at=ts,
        source=source,
        column_ref=column.ref,
    )
    session.add(f)
    session.flush()
    return f
