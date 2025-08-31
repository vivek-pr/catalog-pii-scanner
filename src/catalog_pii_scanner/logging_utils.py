from __future__ import annotations

import json
import logging
import sys
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, cast

from .pii_types import Span
from .redaction import mask_token, redact_text

# Correlation ID context
try:  # Python 3.11+
    import contextvars
except Exception:  # pragma: no cover - very old Python
    contextvars = None  # type: ignore[assignment]

if contextvars is not None:
    _corr_var: contextvars.ContextVar[str | None] = contextvars.ContextVar(
        "cps_correlation_id", default=None
    )
else:  # pragma: no cover - very old Python
    _corr_var = None  # type: ignore[assignment]


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def new_correlation_id() -> str:
    return str(uuid.uuid4())


@contextmanager
def correlation_context(correlation_id: str | None = None) -> Iterator[None]:
    token = None
    try:
        if _corr_var is not None:
            cid = correlation_id or new_correlation_id()
            token = _corr_var.set(cid)
        yield
    finally:  # pragma: no cover - trivial
        if token is not None and _corr_var is not None:
            _corr_var.reset(token)


def get_correlation_id() -> str | None:
    if _corr_var is None:
        return None
    try:
        return cast(str | None, _corr_var.get())
    except Exception:  # pragma: no cover
        return None


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:  # noqa: D401
        # Build a minimal JSON structure
        payload: dict[str, Any] = {
            "time": _now_iso(),
            "level": record.levelname,
            "logger": record.name,
        }
        # message may be literal or structured via extra
        if record.args and isinstance(record.msg, str):
            msg = record.msg % record.args
        else:
            msg = record.msg
        if isinstance(msg, dict):
            payload.update(msg)
        else:
            payload["message"] = str(msg)

        # Include correlation_id from context
        cid = get_correlation_id()
        if cid:
            payload["correlation_id"] = cid

        # Include extras that don't collide with defaults
        for k, v in record.__dict__.items():
            if k in {
                "name",
                "msg",
                "args",
                "levelname",
                "levelno",
                "pathname",
                "filename",
                "module",
                "exc_info",
                "exc_text",
                "stack_info",
                "lineno",
                "funcName",
                "created",
                "msecs",
                "relativeCreated",
                "thread",
                "threadName",
                "processName",
                "process",
            }:
                continue
            if k not in payload:
                try:
                    json.dumps(v)
                    payload[k] = v
                except Exception:
                    payload[k] = str(v)
        return json.dumps(payload, ensure_ascii=False)


_LOGGER_NAME = "catalog_pii_scanner"


class _CorrelationFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:  # pragma: no cover - trivial
        try:
            record.correlation_id = get_correlation_id()
        except Exception:
            record.correlation_id = None
        return True


def get_logger() -> logging.Logger:
    logger = logging.getLogger(_LOGGER_NAME)
    # Ensure single configured StreamHandler to stderr with JSON + correlation filter
    # Remove existing StreamHandlers to avoid stdout pollution in CLI outputs
    to_remove = [h for h in logger.handlers if isinstance(h, logging.StreamHandler)]
    for h in to_remove:
        logger.removeHandler(h)

    handler = logging.StreamHandler(stream=sys.stderr)
    handler.setFormatter(JsonFormatter())
    logger.addHandler(handler)
    # Add correlation filter so caplog records include id
    if not any(isinstance(f, _CorrelationFilter) for f in logger.filters):
        logger.addFilter(_CorrelationFilter())
    if logger.level == logging.NOTSET:
        logger.setLevel(logging.INFO)
    # Allow propagation so test capture (caplog) can see records
    logger.propagate = True
    return logger


def _dedupe_spans(spans: list[Span] | None) -> list[Span]:
    if not spans:
        return []
    seen: set[str] = set()
    out: list[Span] = []
    for s in spans:
        if s.text and s.text not in seen:
            out.append(s)
            seen.add(s.text)
    return out


def _scrub_string(s: str, spans: list[Span]) -> str:
    out = s
    for sp in spans:
        if sp.text:
            out = out.replace(sp.text, mask_token(sp.text))
    return out


def _scrub_obj(obj: Any, spans: list[Span]) -> Any:
    if obj is None:
        return None
    if isinstance(obj, str):
        return _scrub_string(obj, spans)
    if isinstance(obj, int | float | bool):
        return obj
    if isinstance(obj, list):
        return [_scrub_obj(x, spans) for x in obj]
    if isinstance(obj, tuple):  # pragma: no cover - rare
        return tuple(_scrub_obj(x, spans) for x in obj)
    if isinstance(obj, dict):
        return {k: _scrub_obj(v, spans) for k, v in obj.items()}
    try:  # pragma: no cover - fallback
        return json.loads(json.dumps(obj))
    except Exception:
        return str(obj)


def safe_log(
    *,
    event: str,
    details: dict[str, Any] | None = None,
    level: int = logging.INFO,
    text: str | None = None,
    pii_spans: list[Span] | None = None,
) -> None:
    """Emit a structured JSON log with redaction and correlation metadata.

    - Redacts all occurrences of provided spans across string fields.
    - If `text` is provided, logs only its redacted form as `redacted_text`.
    """
    logger = get_logger()
    spans = _dedupe_spans(pii_spans or [])

    payload: dict[str, Any] = {"event": event}
    if text is not None and spans:
        payload["redacted_text"] = redact_text(text, spans).redacted_text
    if details:
        payload.update(_scrub_obj(details, spans))

    logger.log(level, payload)
