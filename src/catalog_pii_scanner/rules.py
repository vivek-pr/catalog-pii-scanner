from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass

from .pii_types import ALL_PII_TYPES, Candidate, PIIType, Span

# Regex patterns for common PII (precompiled)
EMAIL_RE = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")
# US-like phone numbers; allows country code and punctuation
PHONE_US_RE = re.compile(r"(?:\+?\d{1,3}[\s.-]?)?(?:\(\d{3}\)|\d{3})[\s.-]?\d{3}[\s.-]?\d{4}\b")
# Generic credit card digit sequences; validated by Luhn
CC_RE = re.compile(r"\b(?:\d[ -]*?){13,19}\b")
# US SSN
SSN_RE = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
# IPv4
IPV4_RE = re.compile(r"\b(?:(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\.){3}(?:25[0-5]|2[0-4]\d|[01]?\d\d?)\b")
# MAC address (colon or dash separated)
MAC_RE = re.compile(r"\b(?:[0-9A-Fa-f]{2}[:-]){5}[0-9A-Fa-f]{2}\b")
# Dates: ISO or common slashed formats (will be boosted if near DOB keywords)
DATE_RE = re.compile(r"\b(?:\d{4}-\d{2}-\d{2}|\d{2}/\d{2}/\d{4}|\d{2}-\d{2}-\d{4})\b")
# Aadhaar: 12 digits (first digit 2-9), optional spaces/hyphens; validated via Verhoeff
AADHAAR_RE = re.compile(r"\b([2-9][0-9]{3}[ -]?[0-9]{4}[ -]?[0-9]{4})\b")
# Indian PAN: 5 letters + 4 digits + 1 letter
PAN_RE = re.compile(r"\b([A-Z]{5}[0-9]{4}[A-Z])\b", flags=re.IGNORECASE)

# Basic person name (very weak)
PERSON_RE = re.compile(r"\b([A-Z][a-z]+\s[A-Z][a-z]+)\b")


# ---------------- Checksums/validators ----------------


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


# Verhoeff algorithm used by Aadhaar (base 10)
_VERHOEFF_D = [
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
    [1, 2, 3, 4, 0, 6, 7, 8, 9, 5],
    [2, 3, 4, 0, 1, 7, 8, 9, 5, 6],
    [3, 4, 0, 1, 2, 8, 9, 5, 6, 7],
    [4, 0, 1, 2, 3, 9, 5, 6, 7, 8],
    [5, 9, 8, 7, 6, 0, 4, 3, 2, 1],
    [6, 5, 9, 8, 7, 1, 0, 4, 3, 2],
    [7, 6, 5, 9, 8, 2, 1, 0, 4, 3],
    [8, 7, 6, 5, 9, 3, 2, 1, 0, 4],
    [9, 8, 7, 6, 5, 4, 3, 2, 1, 0],
]
_VERHOEFF_P = [
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9],
    [1, 5, 7, 6, 2, 8, 3, 0, 9, 4],
    [5, 8, 0, 3, 7, 9, 6, 1, 4, 2],
    [8, 9, 1, 6, 0, 4, 3, 5, 2, 7],
    [9, 4, 5, 3, 1, 2, 6, 8, 7, 0],
    [4, 2, 8, 6, 5, 7, 3, 9, 0, 1],
    [2, 7, 9, 3, 8, 0, 6, 4, 1, 5],
    [7, 0, 4, 6, 9, 1, 3, 2, 5, 8],
]


def verhoeff_check(number: str) -> bool:
    s = re.sub(r"\D", "", number)
    if len(s) != 12:
        return False
    # Aadhaar must not start with 0/1
    if s[0] in {"0", "1"}:
        return False
    c = 0
    # Process from right to left
    for i, ch in enumerate(reversed(s)):
        c = _VERHOEFF_D[c][_VERHOEFF_P[i % 8][int(ch)]]
    return c == 0


def find_regex(text: str, regex: re.Pattern[str]) -> list[Span]:
    return [Span(m.start(), m.end(), m.group(0)) for m in regex.finditer(text)]


@dataclass(frozen=True)
class RulesConfig:
    enabled_types: frozenset[PIIType] | None = None  # None -> all types
    locales: frozenset[str] = frozenset({"US", "IN"})

    def enabled(self, t: PIIType) -> bool:
        return (self.enabled_types is None) or (t in self.enabled_types)


def _enabled(t: PIIType, cfg: RulesConfig | None) -> bool:
    return True if cfg is None else cfg.enabled(t)


def propose_candidates(text: str, cfg: RulesConfig | None = None) -> list[Candidate]:
    cands: list[Candidate] = []
    # Order matters a bit; add specific patterns first
    if _enabled(PIIType.EMAIL, cfg):
        for span in find_regex(text, EMAIL_RE):
            cands.append(Candidate(span=span, rule_label=PIIType.EMAIL, rule_confidence=0.95))
    if _enabled(PIIType.PHONE_NUMBER, cfg):
        for span in find_regex(text, PHONE_US_RE):
            cands.append(
                Candidate(span=span, rule_label=PIIType.PHONE_NUMBER, rule_confidence=0.85)
            )
    # Credit cards: validate with Luhn
    if _enabled(PIIType.CREDIT_CARD, cfg):
        for span in find_regex(text, CC_RE):
            cc_ok = luhn_check(span.text)
            if cc_ok:
                cands.append(
                    Candidate(
                        span=span,
                        rule_label=PIIType.CREDIT_CARD,
                        rule_confidence=0.9,
                        validations={PIIType.CREDIT_CARD: True},
                    )
                )
    if _enabled(PIIType.SSN, cfg):
        for span in find_regex(text, SSN_RE):
            cands.append(Candidate(span=span, rule_label=PIIType.SSN, rule_confidence=0.9))
    if _enabled(PIIType.IP_ADDRESS, cfg):
        for span in find_regex(text, IPV4_RE):
            cands.append(Candidate(span=span, rule_label=PIIType.IP_ADDRESS, rule_confidence=0.9))
    if _enabled(PIIType.MAC_ADDRESS, cfg):
        for span in find_regex(text, MAC_RE):
            cands.append(Candidate(span=span, rule_label=PIIType.MAC_ADDRESS, rule_confidence=0.9))
    if _enabled(PIIType.AADHAAR, cfg):
        for span in find_regex(text, AADHAAR_RE):
            if verhoeff_check(span.text):
                cands.append(
                    Candidate(
                        span=span,
                        rule_label=PIIType.AADHAAR,
                        rule_confidence=0.9,
                        validations={PIIType.AADHAAR: True},
                    )
                )
    if _enabled(PIIType.PAN, cfg):
        for span in find_regex(text, PAN_RE):
            # Compiled regex is already strict; set label
            cands.append(Candidate(span=span, rule_label=PIIType.PAN, rule_confidence=0.9))
    if _enabled(PIIType.DATE, cfg):
        for span in find_regex(text, DATE_RE):
            # Boost if near DOB keywords
            left = max(0, span.start - 8)
            right = min(len(text), span.end + 8)
            ctx = text[left:right].lower()
            boost = 0.1 if ("dob" in ctx or "birth" in ctx) else 0.0
            cands.append(Candidate(span=span, rule_label=PIIType.DATE, rule_confidence=0.7 + boost))
    # Person names: low confidence; rely on context and ensemble
    if _enabled(PIIType.PERSON, cfg):
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


# ---------------- Metadata keyword heuristics ----------------
_KEYWORDS: dict[PIIType, tuple[str, ...]] = {
    PIIType.EMAIL: (
        "email",
        "e-mail",
        "mailid",
        "mail_id",
        "email_address",
        "primary_email",
    ),
    PIIType.PHONE_NUMBER: (
        "phone",
        "mobile",
        "cell",
        "contact",
        "telephone",
        "mobile_no",
        "phone_number",
    ),
    PIIType.SSN: ("ssn", "social_security"),
    PIIType.AADHAAR: ("aadhaar", "aadhar", "uidai", "uid"),
    PIIType.PAN: ("pan", "pan_no", "pan_number"),
    PIIType.CREDIT_CARD: ("card", "credit", "cc", "cc_number"),
    PIIType.IP_ADDRESS: ("ip", "ipv4", "ipv6"),
    PIIType.MAC_ADDRESS: ("mac", "mac_address"),
    PIIType.DATE: ("dob", "date_of_birth", "birthdate"),
    PIIType.PERSON: ("name", "first_name", "last_name", "full_name"),
}


def keyword_candidates_from_metadata(
    metadata: Mapping[str, str] | Iterable[tuple[str, str]],
    cfg: RulesConfig | None = None,
) -> list[Candidate]:
    pairs: list[tuple[str, str]]
    if isinstance(metadata, Mapping):
        pairs = list(metadata.items())
    else:
        pairs = list(metadata)
    out: list[Candidate] = []
    for _field, value in pairs:
        if not value:
            continue
        hay = value.lower()
        for t, kws in _KEYWORDS.items():
            if not _enabled(t, cfg):
                continue
            for kw in kws:
                idx = hay.find(kw)
                if idx != -1:
                    out.append(
                        Candidate(
                            span=Span(idx, idx + len(kw), value[idx : idx + len(kw)]),
                            rule_label=t,
                            rule_confidence=0.6,
                        )
                    )
                    break
    return out
