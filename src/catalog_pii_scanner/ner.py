from __future__ import annotations

from functools import lru_cache

try:
    import spacy  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    spacy = None  # type: ignore

from .pii_types import PIIType


@lru_cache(maxsize=1)
def _load_spacy() -> spacy.Language | None:
    if spacy is None:
        return None
    try:
        # Try small English model; if missing, fall back to blank English
        return spacy.load("en_core_web_sm")  # type: ignore[arg-type]
    except Exception:
        return spacy.blank("en")  # type: ignore[no-any-return]


def ner_context_signals(context_texts: dict[int, str]) -> dict[int, dict[str, float]]:
    """Run spaCy NER on sanitized context windows only.

    Returns a per-candidate dict of soft signals derived from context entity labels.
    This function never sees raw PII; contexts are expected to be redacted upstream.
    """
    nlp = _load_spacy()
    signals: dict[int, dict[str, float]] = {}
    if nlp is None:
        for k in context_texts:
            signals[k] = {}
        return signals

    docs = list(nlp.pipe(context_texts.values(), disable=["tagger", "lemmatizer"]))
    for (idx, _ctx), doc in zip(context_texts.items(), docs, strict=False):
        # Basic counts of entity labels in context
        label_counts: dict[str, int] = {}
        for ent in getattr(doc, "ents", []) or []:
            label_counts[ent.label_] = label_counts.get(ent.label_, 0) + 1
        # Map some context labels/keywords to PIIType hints
        mapped: dict[str, float] = {}
        if label_counts:
            total = sum(label_counts.values())
            # PERSON context may support PERSON label
            mapped[PIIType.PERSON.value] = label_counts.get("PERSON", 0) / total
            # DATE context may support DATE
            mapped[PIIType.DATE.value] = label_counts.get("DATE", 0) / total
            # ORG context near emails often occurs, weak signal
            mapped[PIIType.EMAIL.value] = label_counts.get("ORG", 0) / total * 0.5
        signals[idx] = mapped
    return signals
