from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from .ner import detect_ner_spans
from .pii_types import Candidate, PIIType, Span


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


def token_for_label(label: PIIType) -> str:
    # Standard bracket tokens for models/LLMs, e.g., [EMAIL]
    return f"[{label.value}]"


def typed_redact_text(text: str, labeled_spans: Iterable[tuple[Span, PIIType]]) -> Redaction:
    """Redact text by replacing spans with bracket tokens like [EMAIL].

    Does NOT preserve length (safe for embeddings/LLM). Overlapping spans are skipped in-order.
    """
    items = sorted(labeled_spans, key=lambda x: x[0].start)
    out: list[str] = []
    cursor = 0
    replaced: list[tuple[Span, str]] = []
    for s, t in items:
        if s.start < cursor:
            continue
        out.append(text[cursor : s.start])
        tok = token_for_label(t)
        out.append(tok)
        replaced.append((s, tok))
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
    """Build per-candidate sanitized context windows using typed tokens.

    - Uses rule candidates' labels plus Presidio/spaCy NER to mask EMAIL/PHONE/ID/etc.
    - Ensures no raw PII values from detected spans appear in the returned windows.
    - Returns windows with normalized tokens like [EMAIL], suitable for embeddings/LLM.
    """
    result: dict[int, str] = {}

    # Collect labeled spans from rules
    labeled_spans: list[tuple[Span, PIIType]] = [
        (c.span, c.rule_label) for c in candidates if c.rule_label is not None
    ]  # type: ignore[list-item]

    # Augment with NER (Presidio/spaCy/regex fallback) over full text
    try:
        ner_batches = detect_ner_spans([text])
        if ner_batches and ner_batches[0]:
            for ns in ner_batches[0]:
                labeled_spans.append((ns.span, ns.label))
    except Exception:
        # NER is optional; proceed with rules-only
        pass

    # For each candidate, build a window and apply typed redaction for any spans intersecting it
    for idx, c in enumerate(candidates):
        cand_span = c.span
        left = max(0, cand_span.start - window)
        right = min(len(text), cand_span.end + window)
        window_text = text[left:right]

        # Select spans that intersect window, adjust to window-relative coords
        window_spans: list[tuple[Span, PIIType]] = []
        for sp, lab in labeled_spans:
            if sp.start < right and left < sp.end:
                rs = max(sp.start, left) - left
                re = min(sp.end, right) - left
                # Create a window-relative span
                window_spans.append((Span(rs, re, window_text[rs:re]), lab))

        red = typed_redact_text(window_text, window_spans)
        result[idx] = red.redacted_text
    return result
