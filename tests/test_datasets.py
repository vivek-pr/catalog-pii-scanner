from catalog_pii_scanner.datasets import generate_synthetic


def test_generate_synthetic_labels_match_text() -> None:
    ds = generate_synthetic(n=5, seed=42)
    assert len(ds) == 5
    for ex in ds:
        for span, _ in ex.labels:
            assert ex.text[span.start : span.end] == span.text
