from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from .pii_types import Candidate, Span


@dataclass
class Redaction:
    redacted_text: str
    # mapping from original offsets to redacted replacements
    replaced_spans: list[tuple[Span, str]]


def mask_token(token: str) -> str:
    # Preserve shape: digits->0, lowercase->x, uppercase->X, others unchanged
    out = []
    for ch in token:
        if ch.isdigit():
            out.append("0")
        elif ch.isalpha():
            out.append("X" if ch.isupper() else "x")
        else:
            out.append(ch)
    return "".join(out)


def redact_text(text: str, spans: Iterable[Span]) -> Redaction:
    # Replace spans with shape-preserving masks, keep length
    spans_sorted = sorted(spans, key=lambda s: s.start)
    out = []
    cursor = 0
    replaced: list[tuple[Span, str]] = []
    for s in spans_sorted:
        if s.start < cursor:
            # overlapping — skip or adjust
            continue
        out.append(text[cursor : s.start])
        masked = mask_token(s.text)
        out.append(masked)
        replaced.append((s, masked))
        cursor = s.end
    out.append(text[cursor:])
    return Redaction(redacted_text="".join(out), replaced_spans=replaced)


def assert_no_raw_pii_to_models(original: str, redacted: str, spans: Iterable[Span]) -> None:
    # Ensure that raw span texts don't appear in redacted payloads
    sset = {s.text for s in spans}
    for s in sset:
        if not s:
            continue
        assert s not in redacted, "Redaction guarantee violated: raw PII present in model input"


def contexts_for_candidates(
    text: str, candidates: list[Candidate], window: int = 32
) -> dict[int, str]:
    # Build small context windows around each candidate, redacting the candidate itself
    result: dict[int, str] = {}
    spans = [c.span for c in candidates]
    red = redact_text(text, spans)
    assert_no_raw_pii_to_models(text, red.redacted_text, spans)
    for idx, c in enumerate(candidates):
        s = c.span
        left = max(0, s.start - window)
        right = min(len(text), s.end + window)
        # Use the already-redacted text to avoid raw PII
        context = red.redacted_text[left:right]
        result[idx] = context
    return result
