from __future__ import annotations

import re

from .pii_types import ALL_PII_TYPES, Candidate, PIIType, Span

# Regex patterns for common PII
EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")
# Looser start boundary to allow leading '(' or '+' etc.
PHONE_RE = re.compile(r"(?:\+?\d{1,3}[\s.-]?)?(?:\(\d{3}\)|\d{3})[\s.-]?\d{3}[\s.-]?\d{4}\b")
CC_RE = re.compile(r"\b(?:\d[ -]*?){13,19}\b")
SSN_RE = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
IP_RE = re.compile(r"\b(?:(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\b")
DATE_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}\b|\b\d{2}/\d{2}/\d{4}\b")

# Basic name detection (very weak, used only when context suggests)
PERSON_RE = re.compile(r"\b([A-Z][a-z]+\s[A-Z][a-z]+)\b")


def luhn_check(number: str) -> bool:
    digits = [int(ch) for ch in re.sub(r"\D", "", number)]
    if not (13 <= len(digits) <= 19):
        return False
    checksum = 0
    parity = len(digits) % 2
    for i, d in enumerate(digits):
        if i % 2 == parity:
            d *= 2
            if d > 9:
                d -= 9
        checksum += d
    return checksum % 10 == 0


def find_regex(text: str, regex: re.Pattern[str]) -> list[Span]:
    return [Span(m.start(), m.end(), m.group(0)) for m in regex.finditer(text)]


def propose_candidates(text: str) -> list[Candidate]:
    cands: list[Candidate] = []
    # Order matters a bit; add specific patterns first
    for span in find_regex(text, EMAIL_RE):
        cands.append(Candidate(span=span, rule_label=PIIType.EMAIL, rule_confidence=0.95))
    for span in find_regex(text, PHONE_RE):
        cands.append(Candidate(span=span, rule_label=PIIType.PHONE_NUMBER, rule_confidence=0.85))
    # Credit cards: validate with Luhn
    for span in find_regex(text, CC_RE):
        cc_ok = luhn_check(span.text)
        conf = 0.9 if cc_ok else 0.5
        cands.append(
            Candidate(
                span=span,
                rule_label=PIIType.CREDIT_CARD if cc_ok else None,
                rule_confidence=conf,
                validations={PIIType.CREDIT_CARD: cc_ok},
            )
        )
    for span in find_regex(text, SSN_RE):
        cands.append(Candidate(span=span, rule_label=PIIType.SSN, rule_confidence=0.9))
    for span in find_regex(text, IP_RE):
        cands.append(Candidate(span=span, rule_label=PIIType.IP_ADDRESS, rule_confidence=0.9))
    for span in find_regex(text, DATE_RE):
        cands.append(Candidate(span=span, rule_label=PIIType.DATE, rule_confidence=0.7))
    # Person names: low confidence; rely on context and ensemble
    for span in find_regex(text, PERSON_RE):
        cands.append(Candidate(span=span, rule_label=PIIType.PERSON, rule_confidence=0.4))
    return cands


def candidate_feature_vector(c: Candidate) -> dict[str, float | int | bool]:
    feats: dict[str, float | int | bool] = {}
    feats["len"] = len(c.span.text)
    feats["has_at"] = "@" in c.span.text
    feats["has_dot"] = "." in c.span.text
    feats["has_digits"] = any(ch.isdigit() for ch in c.span.text)
    feats["digits_ratio"] = sum(ch.isdigit() for ch in c.span.text) / max(1, len(c.span.text))
    feats["rule_conf"] = c.rule_confidence
    for t in ALL_PII_TYPES:
        feats[f"val_{t.value}"] = bool(c.validations.get(t, False) if c.validations else False)
        feats[f"rule_is_{t.value}"] = 1 if c.rule_label == t else 0
    return feats
