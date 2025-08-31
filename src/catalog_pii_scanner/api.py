from __future__ import annotations

from fastapi import FastAPI

from . import __version__

app = FastAPI(title="Catalog PII Scanner API", version=__version__)


@app.get("/healthz")
def healthz() -> dict[str, str]:
    """Simple health check endpoint."""
    return {"status": "ok", "version": __version__}
