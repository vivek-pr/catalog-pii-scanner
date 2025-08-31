from __future__ import annotations

import typer
import uvicorn

from . import __version__

app = typer.Typer(help="Catalog PII Scanner CLI")


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


def main() -> None:
    """Console entry point for `cps`."""
    try:
        app()
    except KeyboardInterrupt:
        typer.echo("Aborted.")
        raise typer.Exit(code=130) from None


if __name__ == "__main__":
    main()
