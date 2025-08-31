from __future__ import annotations

from catalog_pii_scanner.pii_types import PIIType
from catalog_pii_scanner.rules import (
    RulesConfig,
    keyword_candidates_from_metadata,
    luhn_check,
    propose_candidates,
    verhoeff_check,
)


def test_detect_mac_aadhaar_pan_and_dob() -> None:
    # Build a valid Aadhaar by brute-forcing the last digit for a fixed 11-digit prefix
    prefix = "23456789012"  # starts with 2, 11 digits
    aadhaar = None
    for d in range(10):
        cand = f"{prefix}{d}"
        if verhoeff_check(cand):
            aadhaar = cand
            break
    assert aadhaar is not None, "Failed to build a valid Aadhaar test number"

    text = f"Device MAC aa:bb:cc:dd:ee:ff, PAN ABCDE1234F, DOB: 31/12/1990, Aadhaar {aadhaar}."
    cands = propose_candidates(text)
    labels = {c.rule_label for c in cands if c.rule_label}
    assert PIIType.MAC_ADDRESS in labels
    assert PIIType.PAN in labels
    assert PIIType.DATE in labels
    assert PIIType.AADHAAR in labels


def test_rules_config_disable_types() -> None:
    text = "Reach me at john@example.com or (415) 555-0000"
    cfg = RulesConfig(enabled_types=frozenset({PIIType.EMAIL}))
    cands = propose_candidates(text, cfg)
    labels = [c.rule_label for c in cands if c.rule_label]
    assert PIIType.EMAIL in labels
    assert PIIType.PHONE_NUMBER not in labels


def test_metadata_keyword_heuristics() -> None:
    meta = {
        "name": "user_pan_number",
        "description": "primary email address for contact",
        "tags": "customer, pii",
    }
    hints = keyword_candidates_from_metadata(meta)
    types = {h.rule_label for h in hints if h.rule_label}
    assert PIIType.PAN in types
    assert PIIType.EMAIL in types


def test_false_positives_filtered() -> None:
    # PAN-like but invalid (missing last letter)
    txt1 = "PAN ABCDE12345 is invalid"
    labs1 = {c.rule_label for c in propose_candidates(txt1) if c.rule_label}
    assert PIIType.PAN not in labs1

    # Aadhaar-like but invalid (starts with 1 or fails verhoeff)
    txt2 = "Aadhaar 1234 5678 9012"
    labs2 = {c.rule_label for c in propose_candidates(txt2) if c.rule_label}
    assert PIIType.AADHAAR not in labs2

    # Credit card invalid Luhn should not be labelled as CC
    txt3 = "Card 4111 1111 1111 1112"
    assert not luhn_check("4111 1111 1111 1112")
    labs3 = {c.rule_label for c in propose_candidates(txt3) if c.rule_label}
    assert PIIType.CREDIT_CARD not in labs3
