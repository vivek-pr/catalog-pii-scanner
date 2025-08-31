from __future__ import annotations

import os
from collections.abc import Sequence

import pytest

from catalog_pii_scanner.config import NERConfig
from catalog_pii_scanner.ner import NERProvider, NERSpan, detect_ner_spans, merge_with_rules
from catalog_pii_scanner.pii_types import PIIType, Span


class FakeProvider(NERProvider):
    def __init__(self, spans_for_text: list[NERSpan]):
        self._spans_for_text = spans_for_text

    def analyze_batch(self, texts: Sequence[str], _language: str = "en") -> list[list[NERSpan]]:
        # Return the same spans for each text for simplicity
        return [self._spans_for_text for _ in texts]


def test_ner_mock_provider_detects_and_combines() -> None:
    text = "Contact John Doe at john.doe@example.com or +1-415-555-1212."
    person_idx = text.find("John Doe")
    email_idx = text.find("john.doe@example.com")
    phone_idx = text.find("+1-415-555-1212")
    assert person_idx != -1 and email_idx != -1 and phone_idx != -1

    fake_spans = [
        NERSpan(
            span=Span(person_idx, person_idx + len("John Doe"), "John Doe"),
            label=PIIType.PERSON,
            score=0.90,
        ),
        NERSpan(
            span=Span(email_idx, email_idx + len("john.doe@example.com"), "john.doe@example.com"),
            label=PIIType.EMAIL,
            score=0.98,
        ),
        NERSpan(
            span=Span(phone_idx, phone_idx + len("+1-415-555-1212"), "+1-415-555-1212"),
            label=PIIType.PHONE_NUMBER,
            score=0.92,
        ),
    ]

    cfg = NERConfig(enabled=True, provider="presidio", confidence_min=0.60)
    res = detect_ner_spans([text], cfg=cfg, provider=FakeProvider(fake_spans))
    assert len(res) == 1 and len(res[0]) == 3
    labels = {s.label for s in res[0]}
    assert {PIIType.PERSON, PIIType.EMAIL, PIIType.PHONE_NUMBER} <= labels

    # Merge with rule priors and check combined max behavior
    combined = merge_with_rules(text, res[0], confidence_min=cfg.confidence_min)
    assert combined[PIIType.PERSON] >= 0.90
    # EMAIL: rule prior is 0.95, NER is 0.98 => combined 0.98
    assert combined[PIIType.EMAIL] >= 0.98 - 1e-6
    # PHONE: rule prior is 0.85, NER is 0.92 => combined 0.92
    assert combined[PIIType.PHONE_NUMBER] >= 0.92 - 1e-6

    # Increase threshold, only EMAIL remains after gating
    cfg2 = NERConfig(enabled=True, provider="presidio", confidence_min=0.95)
    res2 = detect_ner_spans([text], cfg=cfg2, provider=FakeProvider(fake_spans))
    labs2 = {s.label for s in res2[0]}
    assert labs2 == {PIIType.EMAIL}


@pytest.mark.skipif(
    not os.getenv("CPS_NER_SMOKE"), reason="NER smoke test disabled; set CPS_NER_SMOKE=1 to run"
)
def test_spacy_smoke_email_phone_detection() -> None:
    # Even without spaCy NER model, fallback regex detects EMAIL/PHONE
    os.environ.setdefault("CPS_OFFLINE", "1")
    text = "Call John Doe at john@example.com or (415) 555-1212"
    cfg = NERConfig(enabled=True, provider="spacy", confidence_min=0.6)
    res = detect_ner_spans([text], cfg=cfg)
    labs = {s.label for s in res[0]}
    assert PIIType.EMAIL in labs
    assert PIIType.PHONE_NUMBER in labs
    # PERSON may be present if spaCy model available; don't assert
