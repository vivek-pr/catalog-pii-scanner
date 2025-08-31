from __future__ import annotations

import math
from dataclasses import dataclass

try:
    import joblib  # type: ignore
    from sklearn.linear_model import LogisticRegression  # type: ignore
except Exception:  # pragma: no cover
    joblib = None  # type: ignore
    LogisticRegression = None  # type: ignore

from .embeddings import EmbedModel
from .logging_utils import safe_log
from .ner import ner_context_signals
from .pii_types import ALL_PII_TYPES, Candidate, PIIType, Prediction
from .redaction import contexts_for_candidates


@dataclass
class Calibrator:
    # per-type platt scalers (logistic regression on scalar score)
    models: dict[PIIType, tuple[float, float]]  # (a, b) for sigmoid(a*x + b)

    @staticmethod
    def identity() -> Calibrator:
        return Calibrator(models={t: (1.0, 0.0) for t in ALL_PII_TYPES})

    def save(self, path: str) -> None:
        if joblib is None:
            return
        joblib.dump(self.models, path)

    @staticmethod
    def load(path: str) -> Calibrator:
        if joblib is None:
            return Calibrator.identity()
        try:
            m = joblib.load(path)
            return Calibrator(models=m)
        except Exception:
            return Calibrator.identity()

    def __call__(self, per_type_scores: dict[PIIType, float]) -> dict[PIIType, float]:
        out: dict[PIIType, float] = {}
        for t, s in per_type_scores.items():
            a, b = self.models.get(t, (1.0, 0.0))
            z = a * s + b
            # numerically stable sigmoid
            if z >= 0:
                ez = math.exp(-z)
                p = 1.0 / (1.0 + ez)
            else:
                ez = math.exp(z)
                p = ez / (1.0 + ez)
            out[t] = float(p)
        return out


@dataclass
class Ensemble:
    embed: EmbedModel
    calibrator: Calibrator
    # weights for signals
    w_rule: float = 0.6
    w_ner: float = 0.2
    w_embed: float = 0.4

    def predict(self, text: str, candidates: list[Candidate]) -> list[Prediction]:
        # Build sanitized contexts
        contexts = contexts_for_candidates(text, candidates, window=48)
        # Safe structured log about sanitized inputs
        try:
            import logging

            safe_log(
                event="scan_contexts",
                details={
                    "n_candidates": len(candidates),
                    "examples": [contexts[i] for i in range(min(3, len(candidates)))],
                },
                level=logging.DEBUG,
                text=text,
                pii_spans=[c.span for c in candidates],
            )
        except Exception:
            # Logging must never break prediction
            pass
        # NER context signals (sanitized)
        ner_sig = ner_context_signals(contexts)
        # Embedding predictions on sanitized snippets (candidate masked in context)
        embed_inputs = [contexts[i] for i in range(len(candidates))]
        embed_probs = self.embed.predict_proba(embed_inputs)

        preds: list[Prediction] = []
        for i, c in enumerate(candidates):
            # Start with rule prior
            per_type_score: dict[PIIType, float] = {t: 0.0 for t in ALL_PII_TYPES}
            if c.rule_label is not None:
                per_type_score[c.rule_label] += self.w_rule * c.rule_confidence
            # Validation boosts (e.g., Luhn for CC)
            for t, ok in (c.validations or {}).items():
                if ok:
                    per_type_score[t] += 0.2
            # NER context mapping
            for t in ALL_PII_TYPES:
                per_type_score[t] += self.w_ner * float(ner_sig.get(i, {}).get(t.value, 0.0))
            # Embedding classifier
            for t in ALL_PII_TYPES:
                per_type_score[t] += self.w_embed * float(embed_probs.get(i, {}).get(t, 0.0))

            # Calibrate into probabilities
            probs = self.calibrator(per_type_score)
            # Normalize to avoid all-zeros
            ssum = sum(probs.values()) or 1.0
            probs = {t: v / ssum for t, v in probs.items()}
            label = max(probs.keys(), key=lambda k: probs[k])
            score = float(probs[label])
            preds.append(
                Prediction(
                    span=c.span,
                    probs=probs,
                    label=label,
                    score=score,
                    signals={
                        "rule_label": c.rule_label.value if c.rule_label else None,
                        "rule_conf": c.rule_confidence,
                        "validations": {k.value: v for k, v in (c.validations or {}).items()},
                        "ner": ner_sig.get(i, {}),
                        "embed": {
                            k.value: float(v) for k, v in (embed_probs.get(i, {}) or {}).items()
                        },
                    },
                )
            )
        return preds

    def raw_scores(
        self, text: str, candidates: list[Candidate]
    ) -> tuple[
        list[dict[PIIType, float]], dict[int, dict[str, float]], dict[int, dict[PIIType, float]]
    ]:
        contexts = contexts_for_candidates(text, candidates, window=48)
        ner_sig = ner_context_signals(contexts)
        embed_inputs = [contexts[i] for i in range(len(candidates))]
        embed_probs = self.embed.predict_proba(embed_inputs)
        scores: list[dict[PIIType, float]] = []
        for i, c in enumerate(candidates):
            per_type_score: dict[PIIType, float] = {t: 0.0 for t in ALL_PII_TYPES}
            if c.rule_label is not None:
                per_type_score[c.rule_label] += self.w_rule * c.rule_confidence
            for t, ok in (c.validations or {}).items():
                if ok:
                    per_type_score[t] += 0.2
            for t in ALL_PII_TYPES:
                per_type_score[t] += self.w_ner * float(ner_sig.get(i, {}).get(t.value, 0.0))
            for t in ALL_PII_TYPES:
                per_type_score[t] += self.w_embed * float(embed_probs.get(i, {}).get(t, 0.0))
            scores.append(per_type_score)
        return (
            scores,
            ner_sig,
            {
                i: {t: float(embed_probs.get(i, {}).get(t, 0.0)) for t in ALL_PII_TYPES}
                for i in range(len(candidates))
            },
        )


def fit_calibrator(
    raw_scores: list[dict[PIIType, float]],
    true_labels: list[PIIType | None],
) -> Calibrator:
    # Fit per-type Platt scalers a*x + b
    models: dict[PIIType, tuple[float, float]] = {}
    for t in ALL_PII_TYPES:
        # Build dataset: x=raw_score_t, y=1 if label==t else 0
        X = [[rs.get(t, 0.0)] for rs in raw_scores]
        y = [1 if (lbl == t) else 0 for lbl in true_labels]
        # Guard both extremes: no positives OR all positives -> fallback to identity
        if sum(y) == 0 or sum(y) == len(y) or LogisticRegression is None:
            models[t] = (1.0, 0.0)
            continue
        lr = LogisticRegression(solver="liblinear")
        lr.fit(X, y)
        a = float(lr.coef_[0][0])
        b = float(lr.intercept_[0])
        models[t] = (a, b)
    return Calibrator(models=models)
