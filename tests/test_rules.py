from catalog_pii_scanner.pii_types import PIIType
from catalog_pii_scanner.rules import luhn_check, propose_candidates


def test_propose_candidates_basic() -> None:
    text = (
        "Contact John Doe at john.doe@example.com or (415) 555-1212. "
        "Card 4111 1111 1111 1111 and SSN 123-45-6789."
    )
    cands = propose_candidates(text)
    labels = [c.rule_label for c in cands if c.rule_label]
    assert PIIType.EMAIL in labels
    assert PIIType.PHONE_NUMBER in labels
    assert PIIType.SSN in labels
    # Credit card validated via Luhn
    cc = [c for c in cands if c.validations and c.validations.get(PIIType.CREDIT_CARD)]
    assert cc, "Expected at least one Luhn-validated credit card candidate"


def test_luhn_check_valid_and_invalid() -> None:
    assert luhn_check("4111 1111 1111 1111")
    assert not luhn_check("4111 1111 1111 1112")
