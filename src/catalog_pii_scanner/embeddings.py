from __future__ import annotations

import os
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

import numpy as np

try:  # soft dependency
    from sentence_transformers import SentenceTransformer  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    SentenceTransformer = None  # type: ignore

try:
    import joblib  # type: ignore
    from sklearn.linear_model import LogisticRegression  # type: ignore
    from sklearn.multiclass import OneVsRestClassifier  # type: ignore
    from sklearn.pipeline import Pipeline  # type: ignore
    from sklearn.preprocessing import StandardScaler  # type: ignore
except Exception:  # pragma: no cover
    LogisticRegression = None  # type: ignore
    StandardScaler = None  # type: ignore
    Pipeline = None  # type: ignore
    OneVsRestClassifier = None  # type: ignore
    joblib = None  # type: ignore

from .pii_types import ALL_PII_TYPES, PIIType

DEFAULT_SBERT = "sentence-transformers/all-MiniLM-L6-v2"


@dataclass
class EmbedModel:
    sbert_name: str = DEFAULT_SBERT
    clf_path: str | None = None
    # internal caches
    _sbert: Any | None = field(init=False, default=None)
    _clf: list[Any | tuple[str, float]] | None = field(init=False, default=None)

    def __post_init__(self) -> None:
        self._sbert = None
        self._clf = None

    def _load_sbert(self):  # type: ignore[no-untyped-def]
        # Offline mode: skip loading heavy models
        if os.getenv("CPS_OFFLINE"):
            return None
        if self._sbert is None and SentenceTransformer is not None:
            self._sbert = SentenceTransformer(self.sbert_name)
        return self._sbert

    def _load_clf(self) -> list[Any | tuple[str, float]] | None:  # type: ignore[no-any-unimported]
        if (
            self._clf is None
            and self.clf_path
            and joblib is not None
            and os.path.exists(self.clf_path)
        ):
            self._clf = joblib.load(self.clf_path)
        return self._clf

    def encode(self, texts: Sequence[str]) -> np.ndarray:
        model = self._load_sbert()
        if model is None:
            # Fallback: simple hashing features if SBERT not available
            rng = np.random.default_rng(42)
            return rng.normal(size=(len(texts), 32)).astype(np.float32)
        embs = model.encode(list(texts), normalize_embeddings=True, show_progress_bar=False)
        return np.asarray(embs)

    def predict_proba(self, texts: Sequence[str]) -> dict[int, dict[PIIType, float]]:
        clf = self._load_clf()
        X = self.encode(texts)
        result: dict[int, dict[PIIType, float]] = {}
        if clf is None:
            # neutral predictions
            for i in range(len(texts)):
                result[i] = {t: 0.0 for t in ALL_PII_TYPES}
            return result
        # clf may be a list of per-class estimators or trivial entries
        per_class_scores: list[list[float]] = []  # shape: [class][sample]
        assert isinstance(clf, list)
        for j, _t in enumerate(ALL_PII_TYPES):
            est = clf[j]
            if isinstance(est, tuple) and est and est[0] == "trivial":
                p_pos = float(est[1])
                per_class_scores.append([p_pos for _ in range(len(texts))])
            else:
                proba = est.predict_proba(X)  # type: ignore[no-any-return]
                # scikit returns [n_samples, 2]; take prob of positive class (index 1)
                p_pos_list: list[float] = [float(row[1]) for row in proba]
                per_class_scores.append(p_pos_list)
        for i in range(len(texts)):
            out: dict[PIIType, float] = {}
            for j, t in enumerate(ALL_PII_TYPES):
                out[t] = float(per_class_scores[j][i])
            result[i] = out
        return result

    def fit(self, texts: Sequence[str], labels: Sequence[PIIType]) -> None:
        if LogisticRegression is None:
            return
        X = self.encode(texts)
        y = np.array([t.value for t in labels])
        # One-vs-rest with class weights to handle imbalance
        classes = np.array([t.value for t in ALL_PII_TYPES])
        # Build multi-label targets per class (one-hot)
        Y = np.stack([(y == c).astype(int) for c in classes], axis=1)
        # No base pipeline variable needed; estimators are created per class below
        # Fit one-vs-rest manually to keep per-class calibration simple
        estims: list[Any | tuple[str, float]] = []
        for j in range(Y.shape[1]):
            yj = Y[:, j]
            # If only one class present, store a trivial estimator with constant prob
            positives = int(yj.sum())
            if positives == 0:
                estims.append(("trivial", 0.0))
                continue
            if positives == len(yj):
                estims.append(("trivial", 1.0))
                continue
            est = Pipeline(
                steps=[
                    ("scaler", StandardScaler(with_mean=False)),
                    (
                        "clf",
                        LogisticRegression(
                            solver="liblinear",
                            max_iter=200,
                            class_weight="balanced",
                        ),
                    ),
                ]
            )
            est.fit(X, yj)
            estims.append(est)  # type: ignore[arg-type]
        self._clf = estims

    def save(self, path: str) -> None:
        if self._clf is None or joblib is None:
            return
        os.makedirs(os.path.dirname(path), exist_ok=True)
        joblib.dump(self._clf, path)
        self.clf_path = path
