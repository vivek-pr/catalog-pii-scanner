from __future__ import annotations

from catalog_pii_scanner.embeddings import EmbedModel
from catalog_pii_scanner.ensemble import Calibrator, Ensemble
from catalog_pii_scanner.pii_types import PIIType
from catalog_pii_scanner.redaction import contexts_for_candidates
from catalog_pii_scanner.rules import propose_candidates


def test_contexts_use_bracket_tokens() -> None:
    text = "Contact John Doe at john.doe@example.com or +1 (415) 555-1234."
    cands = propose_candidates(text)
    ctxs = contexts_for_candidates(text, cands, window=64)
    # Ensure tokens appear and raw values do not
    any_email = any("[EMAIL]" in v for v in ctxs.values())
    any_phone = any("[PHONE_NUMBER]" in v for v in ctxs.values())
    assert any_email, "Expected [EMAIL] token in contexts"
    assert any_phone, "Expected [PHONE_NUMBER] token in contexts"
    for v in ctxs.values():
        assert "john.doe@example.com" not in v
        assert "+1 (415) 555-1234" not in v


def test_contexts_cover_multiple_types() -> None:
    text = (
        "Name: Jane Roe, Email: jane.roe@example.org, Phone: 212-555-9876, "
        "CC: 4111 1111 1111 1111, SSN: 123-45-6789"
    )
    cands = propose_candidates(text)
    ctxs = contexts_for_candidates(text, cands, window=80)
    joined = "\n".join(ctxs.values())
    # Expect tokens for various types detected by rules
    assert "[EMAIL]" in joined
    assert "[PHONE_NUMBER]" in joined
    assert "[CREDIT_CARD]" in joined
    assert "[SSN]" in joined
    # No raw values must leak
    assert "jane.roe@example.org" not in joined
    assert "212-555-9876" not in joined
    assert "4111 1111 1111 1111" not in joined
    assert "123-45-6789" not in joined


class _CapturingEmbed(EmbedModel):
    captured: list[str]

    def __init__(self) -> None:  # type: ignore[no-untyped-def]
        super().__init__()
        self.captured = []

    def predict_proba(self, texts: list[str]) -> dict[int, dict[PIIType, float]]:  # type: ignore[override]
        self.captured = list(texts)
        # Neutral predictions
        return {i: {t: 0.0 for t in PIIType} for i in range(len(texts))}  # type: ignore[arg-type]


def test_no_raw_pii_reaches_embeddings() -> None:
    text = "Reach me at bob.smith@company.com and 650-555-0000."
    cands = propose_candidates(text)
    embed = _CapturingEmbed()
    ens = Ensemble(embed=embed, calibrator=Calibrator.identity())
    _ = ens.predict(text, cands)
    # All inputs sent to embeddings must be tokenized and free of raw PII
    assert embed.captured, "Embeddings received no inputs"
    joined = "\n".join(embed.captured)
    assert "[EMAIL]" in joined and "[PHONE_NUMBER]" in joined
    assert "bob.smith@company.com" not in joined
    assert "650-555-0000" not in joined
