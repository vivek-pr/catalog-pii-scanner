from catalog_pii_scanner.ensemble import fit_calibrator
from catalog_pii_scanner.pii_types import ALL_PII_TYPES, PIIType


def test_fit_calibrator_all_positive_class_guard() -> None:
    # Build a raw score list with any values; labels all EMAIL
    raw_scores = [{t: 0.5 for t in ALL_PII_TYPES} for _ in range(10)]
    labels: list[PIIType | None] = [PIIType.EMAIL for _ in range(10)]
    calib = fit_calibrator(raw_scores, labels)
    # For all-one labels, we should fall back to identity for EMAIL (no crash)
    assert PIIType.EMAIL in calib.models
    a, b = calib.models[PIIType.EMAIL]
    assert (a, b) == (1.0, 0.0)
