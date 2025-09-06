from __future__ import annotations

import json
from pathlib import Path

import typer
import uvicorn

from . import __version__
from .config import validate_config_file
from .connectors.glue import GlueCatalogClient
from .connectors.hms import HiveMetastoreClient
from .connectors.unity import UnityCatalogClient
from .datasets import generate_synthetic, load_jsonl, save_jsonl
from .db import (
    Column as DBColumn,
)
from .db import (
    Finding,
    add_finding,
    init_db,
    session_scope,
    upsert_column,
)
from .embeddings import EmbedModel
from .ensemble import Calibrator, Ensemble
from .eval import calibrate_on_dataset, run_eval
from .pii_types import PIIType
from .redaction import token_for_label
from .rules import propose_candidates

app = typer.Typer(help="Catalog PII Scanner CLI")
config_app = typer.Typer(help="Config utilities")

# Define option defaults at module scope to satisfy Ruff B008
TYPE_OPT = typer.Option([], "--type", help="Detected PII type(s), e.g., EMAIL. Can repeat.")


def version_callback(value: bool) -> None:
    if value:
        typer.echo(__version__)
        raise typer.Exit()


@app.callback()
def main_callback(
    version: bool | None = typer.Option(
        None,
        "--version",
        callback=version_callback,
        is_eager=True,
        help="Show version and exit.",
    ),
) -> None:
    """Top-level callback for global options (e.g., --version)."""


@app.command()
def scan(
    path: str | None = typer.Argument(None, help="Path to scan for PII (placeholder)"),
    target: str | None = typer.Option(
        None, "--target", help="Target URI, e.g., glue://* or glue://db/*"
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run", help="Write findings to the results store (no tagging)"
    ),
    db: str = typer.Option(
        "sqlite:///cps.db", "--db", help="Database URL for results (SQLite or Postgres)"
    ),
    catalog: str = typer.Option("default", "--catalog", help="Catalog name"),
    schema: str = typer.Option("public", "--schema", help="Schema name"),
    table: str = typer.Option("files", "--table", help="Table name"),
    column: str = typer.Option("path", "--column", help="Column name"),
    type_: list[str] = TYPE_OPT,
    confidence: float = typer.Option(0.9, "--confidence", help="Confidence score [0-1]"),
    hit_rate: float = typer.Option(0.5, "--hit-rate", help="Hit rate [0-1]"),
    model_version: str = typer.Option("v0", "--model-version", help="Model version label"),
    source: str = typer.Option("cli", "--source", help="Source of the scan"),
    apply: bool = typer.Option(False, "--apply", help="Apply tags/comments back to the catalog"),
    append_comment: str | None = typer.Option(
        None, "--append-comment", help="Optional comment to append to column description"
    ),
) -> None:
    """Scan and persist results. With --dry-run, writes to SQLite/Postgres only."""
    # New: Targeted connector route
    if target and target.startswith("glue://"):
        # Enumerate AWS Glue Data Catalog
        pat = target[len("glue://") :].strip()
        parts = [p for p in pat.split("/") if p]
        db_pats = ["*"]
        tbl_pats = ["*"]
        if len(parts) >= 1 and parts[0] not in {"*", ""}:
            db_pats = [parts[0]]
        if len(parts) >= 2 and parts[1] not in {"*", ""}:
            tbl_pats = [parts[1]]

        glue_client = GlueCatalogClient()
        glue_cols = list(glue_client.iter_columns(db_patterns=db_pats, table_patterns=tbl_pats))
        # Print summary JSON to stdout
        out = [
            {
                "ref": gc.ref,
                "database": gc.database,
                "table": gc.table,
                "column": gc.name,
                "type": gc.type,
                "comment": gc.comment,
                "parameters": gc.parameters,
            }
            for gc in glue_cols
        ]
        typer.echo(json.dumps({"count": len(out), "columns": out}, indent=2))

        if apply:
            # Idempotent tag back per column
            for gc in glue_cols:
                glue_client.update_column_tags(
                    database=gc.database,
                    table=gc.table,
                    column=gc.name,
                    pii=True,
                    pii_types=type_ or ["PII"],
                    append_comment=append_comment,
                )
        return

    if target and target.startswith("unity://"):
        # Enumerate Databricks Unity Catalog
        pat = target[len("unity://") :].strip()
        parts = [p for p in pat.split("/") if p]
        cat_pats = ["*"]
        sch_pats = ["*"]
        tbl_pats = ["*"]
        if len(parts) >= 1 and parts[0] not in {"*", ""}:
            cat_pats = [parts[0]]
        if len(parts) >= 2 and parts[1] not in {"*", ""}:
            sch_pats = [parts[1]]
        if len(parts) >= 3 and parts[2] not in {"*", ""}:
            tbl_pats = [parts[2]]

        unity_client = UnityCatalogClient()
        unity_cols = list(
            unity_client.iter_columns(
                catalog_patterns=cat_pats, schema_patterns=sch_pats, table_patterns=tbl_pats
            )
        )
        out = [
            {
                "ref": uc.ref,
                "catalog": uc.catalog,
                "schema": uc.schema,
                "table": uc.table,
                "column": uc.name,
                "type": uc.type,
                "comment": uc.comment,
                "properties": uc.properties,
            }
            for uc in unity_cols
        ]
        typer.echo(json.dumps({"count": len(out), "columns": out}, indent=2))

        if apply:
            for uc in unity_cols:
                unity_client.update_column_tags(
                    catalog=uc.catalog,
                    schema=uc.schema,
                    table=uc.table,
                    column=uc.name,
                    pii=True,
                    pii_types=type_ or ["PII"],
                    append_comment=append_comment,
                )
        return

    if target and target.startswith("hms://"):
        # Enumerate Hive Metastore via Thrift
        pat = target[len("hms://") :].strip()
        parts = [p for p in pat.split("/") if p]
        db_pats = ["*"]
        tbl_pats = ["*"]
        if len(parts) >= 1 and parts[0] not in {"*", ""}:
            db_pats = [parts[0]]
        if len(parts) >= 2 and parts[1] not in {"*", ""}:
            tbl_pats = [parts[1]]

        hms_client = HiveMetastoreClient()
        hms_cols = list(hms_client.iter_columns(db_patterns=db_pats, table_patterns=tbl_pats))
        out = [
            {
                "ref": hc.ref,
                "database": hc.database,
                "table": hc.table,
                "column": hc.name,
                "type": hc.type,
                "comment": hc.comment,
                "properties": hc.properties,
            }
            for hc in hms_cols
        ]
        typer.echo(json.dumps({"count": len(out), "columns": out}, indent=2))

        if apply:
            for hc in hms_cols:
                hms_client.update_column_tags(
                    database=hc.database,
                    table=hc.table,
                    column=hc.name,
                    pii=True,
                    pii_types=type_ or ["PII"],
                    append_comment=append_comment,
                )
        return

    if path:
        typer.echo(f"Scanning path: {path}")
    if not dry_run:
        # For now, only dry-run writes results in this skeleton
        typer.echo("Hint: use --dry-run to write findings to the DB")
        return
    # Initialize DB and persist a single finding for the provided target
    Session = init_db(db)
    with session_scope(Session) as s:
        col: DBColumn = upsert_column(s, catalog=catalog, schema=schema, table=table, column=column)
        types = type_ or ["EMAIL"]
        f = add_finding(
            s,
            col,
            types=types,
            confidence=confidence,
            hit_rate=hit_rate,
            model_version=model_version,
            source=source,
        )
        typer.echo(
            json.dumps(
                {
                    "id": f.id,
                    "column_ref": f.column_ref,
                    "types": f.types,
                    "confidence": f.confidence,
                    "hit_rate": f.hit_rate,
                    "model_version": f.model_version,
                    "scanned_at": f.scanned_at.isoformat(),
                    "source": f.source,
                }
            )
        )


@app.command()
def scan_text(
    text: str = typer.Argument(..., help="Raw text to scan"),
    model_dir: str | None = typer.Option(None, "--model-dir", help="Directory for models"),
) -> None:
    """Scan a single text and output JSON with per-type probabilities per span."""
    # Build ensemble with identity calibrator if none saved
    embed = EmbedModel(clf_path=str(Path(model_dir or ".models") / "embed.joblib"))
    calib_path = str(Path(model_dir or ".models") / "calibrator.joblib")
    calibrator = Calibrator.load(calib_path)
    ens = Ensemble(embed=embed, calibrator=calibrator)
    cands = propose_candidates(text)
    preds = ens.predict(text, cands)
    out = []
    for p in preds:
        out.append(
            {
                "span": {"start": p.span.start, "end": p.span.end, "text": p.span.text},
                "label": p.label.value if p.label else None,
                "score": p.score,
                "probs": {t.value: float(v) for t, v in p.probs.items()},
            }
        )
    typer.echo(json.dumps(out, indent=2))


@app.command()
def serve(
    host: str = typer.Option("127.0.0.1", "--host", help="Host to bind"),
    port: int = typer.Option(8000, "--port", help="Port to listen on"),
    reload: bool = typer.Option(False, "--reload", help="Enable auto-reload (dev)"),
) -> None:
    """Run the FastAPI server."""
    # Import path keeps reload working reliably
    uvicorn.run(
        "catalog_pii_scanner.api:app",
        host=host,
        port=port,
        reload=reload,
    )


@app.command()
def gen_synth(
    out_path: str = typer.Argument("synth.jsonl", help="Output JSONL path"),
    n: int = typer.Option(500, "--n", help="Number of examples"),
    seed: int = typer.Option(1234, "--seed", help="Random seed"),
) -> None:
    """Generate a synthetic labeled dataset."""
    data = generate_synthetic(n=n, seed=seed)
    save_jsonl(out_path, data)
    typer.echo(f"Wrote {n} examples to {out_path}")


@app.command()
def train_embed(
    data_path: str = typer.Argument(..., help="JSONL dataset path"),
    model_dir: str = typer.Option(".models", "--model-dir", help="Directory to save models"),
) -> None:
    """Train the embeddings classifier on sanitized contexts only (no raw PII)."""
    ds = load_jsonl(data_path)
    embed = EmbedModel()
    texts: list[str] = []
    labels: list[PIIType] = []
    for ex in ds:
        # build candidate contexts from gold spans (supervised training)
        for span, lbl in ex.labels:
            start = max(0, span.start - 48)
            end = min(len(ex.text), span.end + 48)
            ctx = ex.text[start:end]
            # sanitize: replace the PII span with a bracket token like [EMAIL]
            tok = token_for_label(lbl)
            l_off = span.start - start
            r_off = span.end - start
            masked_ctx = ctx[:l_off] + tok + ctx[r_off:]
            texts.append(masked_ctx)
            labels.append(lbl)
    embed.fit(texts, labels)
    out_dir = Path(model_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    embed_path = str(out_dir / "embed.joblib")
    embed.save(embed_path)
    typer.echo(f"Saved embeddings classifier to {embed_path}")


@app.command()
def calibrate(
    data_path: str = typer.Argument(..., help="JSONL dataset path"),
    model_dir: str = typer.Option(".models", "--model-dir", help="Directory to save models"),
) -> None:
    """Fit calibration (Platt scaling) for ensemble outputs."""
    ds = load_jsonl(data_path)
    embed = EmbedModel(clf_path=str(Path(model_dir) / "embed.joblib"))
    calib = calibrate_on_dataset(ds, embed)
    out_dir = Path(model_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    calib_path = str(out_dir / "calibrator.joblib")
    calib.save(calib_path)
    typer.echo(f"Saved calibrator to {calib_path}")


@app.command()
def eval(
    data_path: str = typer.Argument(..., help="JSONL dataset path"),
    model_dir: str = typer.Option(".models", "--model-dir", help="Directory of models"),
) -> None:
    """Run evaluation and print precision/recall/F1 per type and micro/macro."""
    ds = load_jsonl(data_path)
    embed = EmbedModel(clf_path=str(Path(model_dir) / "embed.joblib"))
    calibrator = Calibrator.load(str(Path(model_dir) / "calibrator.joblib"))
    ens = Ensemble(embed=embed, calibrator=calibrator)
    rep = run_eval(ds, ens)
    # Render report
    lines = []
    lines.append("Per-type metrics:")
    for t, m in rep.per_type.items():
        lines.append(
            "  "
            f"{t.value:12s} "
            f"precision={m['precision']:.3f} "
            f"recall={m['recall']:.3f} "
            f"f1={m['f1']:.3f}"
        )
    lines.append(
        "Micro: "
        f"precision={rep.micro['precision']:.3f} "
        f"recall={rep.micro['recall']:.3f} "
        f"f1={rep.micro['f1']:.3f}"
    )
    lines.append(
        "Macro: "
        f"precision={rep.macro['precision']:.3f} "
        f"recall={rep.macro['recall']:.3f} "
        f"f1={rep.macro['f1']:.3f}"
    )
    typer.echo("\n".join(lines))


@config_app.command("validate")
def config_validate(
    file: str = typer.Option(..., "-f", "--file", help="Path to config YAML"),
    env_prefix: str = typer.Option("CPS_", "--env-prefix", help="ENV override prefix"),
) -> None:
    """Validate a configuration file, applying env overrides if present."""
    cfg, errors = validate_config_file(file, env_prefix=env_prefix)
    if errors:
        typer.echo("Config invalid:")
        for e in errors:
            typer.echo(f"  - {e}")
        raise typer.Exit(code=1)
    typer.echo("Config OK")


@app.command()
def export(
    format: str = typer.Option("json", "--format", help="Output format: json|csv"),
    out: str = typer.Option("-", "--out", help="Output file path or '-' for stdout"),
    db: str = typer.Option("sqlite:///cps.db", "--db", help="Database URL for results"),
) -> None:
    """Export findings from the results store as JSON or CSV."""
    fmt = format.lower()
    if fmt not in {"json", "csv"}:
        raise typer.BadParameter("--format must be 'json' or 'csv'")
    Session = init_db(db)
    from sqlalchemy import select

    rows: list[dict] = []
    with session_scope(Session) as s:
        res = s.execute(select(Finding))
        for f in res.scalars():
            rows.append(
                {
                    "id": f.id,
                    "column_ref": f.column_ref,
                    "types": f.types,
                    "confidence": f.confidence,
                    "hit_rate": f.hit_rate,
                    "model_version": f.model_version,
                    "scanned_at": f.scanned_at.isoformat(),
                    "source": f.source,
                }
            )

    if fmt == "json":
        data = json.dumps(rows, indent=2)
        if out == "-":
            typer.echo(data)
        else:
            Path(out).write_text(data, encoding="utf-8")
            typer.echo(f"Wrote {len(rows)} findings to {out}")
    else:
        # CSV
        import csv

        headers = [
            "id",
            "column_ref",
            "types",
            "confidence",
            "hit_rate",
            "model_version",
            "scanned_at",
            "source",
        ]
        if out == "-":
            # write to stdout
            import sys

            w = csv.DictWriter(sys.stdout, fieldnames=headers)
            w.writeheader()
            for r in rows:
                r = {
                    **r,
                    "types": (
                        ",".join(r["types"]) if isinstance(r.get("types"), list) else r.get("types")
                    ),
                }
                w.writerow(r)
        else:
            with open(out, "w", newline="", encoding="utf-8") as fh:
                w = csv.DictWriter(fh, fieldnames=headers)
                w.writeheader()
                for r in rows:
                    r = {
                        **r,
                        "types": (
                            ",".join(r["types"])
                            if isinstance(r.get("types"), list)
                            else r.get("types")
                        ),
                    }
                    w.writerow(r)
            typer.echo(f"Wrote {len(rows)} findings to {out}")


app.add_typer(config_app, name="config")


def main() -> None:
    """Console entry point for `cps`."""
    try:
        app()
    except KeyboardInterrupt:
        typer.echo("Aborted.")
        raise typer.Exit(code=130) from None


if __name__ == "__main__":
    main()
