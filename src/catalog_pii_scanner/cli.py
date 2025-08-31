from __future__ import annotations

import json
from pathlib import Path

import typer
import uvicorn

from . import __version__
from .config import validate_config_file
from .datasets import generate_synthetic, load_jsonl, save_jsonl
from .embeddings import EmbedModel
from .ensemble import Calibrator, Ensemble
from .eval import calibrate_on_dataset, run_eval
from .pii_types import PIIType
from .rules import propose_candidates

app = typer.Typer(help="Catalog PII Scanner CLI")
config_app = typer.Typer(help="Config utilities")


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
    path: str = typer.Argument(..., help="Path to scan for PII"),
) -> None:
    """Scan the given PATH for PII (stub)."""
    typer.echo(f"Scanning path: {path}")


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
            # sanitize: replace the PII span with shape-preserving mask
            masked_ctx = (
                ctx[: span.start - start]
                + ("0" * (span.end - span.start))
                + ctx[span.end - start :]
            )
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
