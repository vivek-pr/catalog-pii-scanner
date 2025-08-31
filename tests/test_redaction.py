from catalog_pii_scanner.redaction import contexts_for_candidates, redact_text
from catalog_pii_scanner.rules import propose_candidates


def test_redaction_masks_spans_and_preserves_length() -> None:
    text = "Email me at john.doe@example.com please."
    cands = propose_candidates(text)
    spans = [c.span for c in cands]
    red = redact_text(text, spans)
    # No raw span text should appear in redacted output
    for s in spans:
        assert s.text not in red.redacted_text
        assert (
            (s.end - s.start)
            == len(s.text)
            == len(red.replaced_spans[[x[0] for x in red.replaced_spans].index(s)][1])
        )


def test_contexts_are_sanitized() -> None:
    text = "Contact at john.doe@example.com now."
    cands = propose_candidates(text)
    ctxs = contexts_for_candidates(text, cands)
    for i, c in enumerate(cands):
        assert c.span.text not in ctxs[i]
