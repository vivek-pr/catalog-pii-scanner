import os

from catalog_pii_scanner.embeddings import EmbedModel
from catalog_pii_scanner.ensemble import Calibrator, Ensemble
from catalog_pii_scanner.rules import propose_candidates


def test_ensemble_predict_per_type_probs_and_labels() -> None:
    os.environ["CPS_OFFLINE"] = "1"
    text = "Call me at (415) 555-1212 or email john.doe@example.com"
    cands = propose_candidates(text)
    ens = Ensemble(embed=EmbedModel(), calibrator=Calibrator.identity())
    preds = ens.predict(text, cands)
    assert preds, "Expected predictions"
    for p, c in zip(preds, cands, strict=False):
        # per-type probs present and sum to ~1
        assert p.probs and abs(sum(p.probs.values()) - 1.0) < 1e-6
        assert p.label is not None
        # If rule_label is given, it should dominate in this simple case
        if c.rule_label is not None:
            assert p.label == c.rule_label
