from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from .datasets import LabeledExample
from .embeddings import EmbedModel
from .ensemble import Calibrator, Ensemble, fit_calibrator
from .pii_types import ALL_PII_TYPES, PIIType, Prediction, Span
from .rules import propose_candidates


@dataclass
class EvalReport:
    per_type: dict[PIIType, dict[str, float]]
    micro: dict[str, float]
    macro: dict[str, float]


def _match(
    preds: list[Prediction], gold: list[tuple[Span, PIIType]]
) -> tuple[int, int, int, dict[PIIType, tuple[int, int, int]]]:
    # Simple overlap matching: a prediction matches a gold if spans overlap any char and types equal
    tp = 0
    fp = 0
    fn = 0
    per_type: dict[PIIType, list[int]] = {t: [0, 0, 0] for t in ALL_PII_TYPES}
    used_gold = [False] * len(gold)
    for p in preds:
        matched = False
        for j, (gs, gt) in enumerate(gold):
            if used_gold[j]:
                continue
            if p.span.start < gs.end and gs.start < p.span.end:
                # overlap
                if p.label == gt:
                    tp += 1
                    per_type[gt][0] += 1
                    used_gold[j] = True
                    matched = True
                    break
        if not matched:
            fp += 1
            per_type[p.label or ALL_PII_TYPES[0]][1] += 1
    for j, (_gs, gt) in enumerate(gold):
        if not used_gold[j]:
            fn += 1
            per_type[gt][2] += 1
    per_type_tuple = {t: (v[0], v[1], v[2]) for t, v in per_type.items()}
    return tp, fp, fn, per_type_tuple


def _prf(tp: int, fp: int, fn: int) -> dict[str, float]:
    prec = tp / (tp + fp) if (tp + fp) else 0.0
    rec = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * prec * rec / (prec + rec) if (prec + rec) else 0.0
    return {"precision": prec, "recall": rec, "f1": f1}


def run_eval(examples: Iterable[LabeledExample], ensemble: Ensemble) -> EvalReport:
    all_preds: list[Prediction] = []
    all_gold: list[tuple[Span, PIIType]] = []
    for ex in examples:
        cands = propose_candidates(ex.text)
        preds = ensemble.predict(ex.text, cands)
        all_preds.extend(preds)
        all_gold.extend(ex.labels)
    tp, fp, fn, per_type = _match(all_preds, all_gold)
    per_type_scores: dict[PIIType, dict[str, float]] = {}
    for t, (tpi, fpi, fni) in per_type.items():
        per_type_scores[t] = _prf(tpi, fpi, fni)
    micro = _prf(tp, fp, fn)
    macro = {
        k: sum(v[k] for v in per_type_scores.values()) / len(per_type_scores)
        for k in ["precision", "recall", "f1"]
    }
    return EvalReport(per_type=per_type_scores, micro=micro, macro=macro)


def calibrate_on_dataset(examples: list[LabeledExample], embed: EmbedModel) -> Calibrator:
    # Compute raw scores (pre-calibration) using rule + ner + embed
    ensemble = Ensemble(embed=embed, calibrator=Calibrator.identity())
    raw_scores: list[dict[PIIType, float]] = []
    labels: list[PIIType | None] = []
    for ex in examples:
        cands = propose_candidates(ex.text)
        scores, _, _ = ensemble.raw_scores(ex.text, cands)
        # For calibration we need true label per candidate: pick exact match if exists
        gold = ex.labels
        for c, sc in zip(cands, scores, strict=False):
            lbl: PIIType | None = None
            for gs, gt in gold:
                if c.span.start < gs.end and gs.start < c.span.end:
                    lbl = gt
                    break
            raw_scores.append(sc)
            labels.append(lbl)
    return fit_calibrator(raw_scores, labels)
