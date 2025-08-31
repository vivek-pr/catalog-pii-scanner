from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from functools import lru_cache

try:
    import spacy  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    spacy = None  # type: ignore

from .config import NERConfig
from .pii_types import PIIType, Span
from .rules import EMAIL_RE, PHONE_US_RE

# ---------------- spaCy loading helpers ----------------


@lru_cache(maxsize=8)
def _load_spacy(model: str | None, language: str = "en") -> spacy.Language | None:
    if spacy is None:
        return None
    try:
        if model:
            return spacy.load(model)  # type: ignore[arg-type]
        # Try small English model; if missing, fall back to blank
        return spacy.load("en_core_web_sm")  # type: ignore[arg-type]
    except Exception:
        try:
            return spacy.blank(language)
        except Exception:
            return None


# ---------------- Public datatypes ----------------


@dataclass(frozen=True)
class NERSpan:
    span: Span
    label: PIIType
    score: float


# ---------------- Providers ----------------


class NERProvider:
    def analyze_batch(self, texts: Sequence[str], language: str = "en") -> list[list[NERSpan]]:
        raise NotImplementedError


class SpaCyProvider(NERProvider):
    def __init__(self, model: str | None = None) -> None:
        self.model = model

    def analyze_batch(self, texts: Sequence[str], language: str = "en") -> list[list[NERSpan]]:
        nlp = _load_spacy(self.model, language)
        out: list[list[NERSpan]] = []
        if nlp is None:
            return [[] for _ in texts]
        docs = list(nlp.pipe(texts, disable=["tagger", "lemmatizer"]))
        for text, doc in zip(texts, docs, strict=False):
            spans: list[NERSpan] = []
            # PERSON via spaCy ents
            for ent in getattr(doc, "ents", []) or []:
                if ent.label_ == "PERSON":
                    spans.append(
                        NERSpan(
                            span=Span(
                                ent.start_char, ent.end_char, text[ent.start_char : ent.end_char]
                            ),
                            label=PIIType.PERSON,
                            score=0.85,
                        )
                    )
            # EMAIL via robust regex
            for m in EMAIL_RE.finditer(text):
                spans.append(
                    NERSpan(
                        span=Span(m.start(), m.end(), m.group(0)), label=PIIType.EMAIL, score=0.99
                    )
                )
            # PHONE via robust regex
            for m in PHONE_US_RE.finditer(text):
                spans.append(
                    NERSpan(
                        span=Span(m.start(), m.end(), m.group(0)),
                        label=PIIType.PHONE_NUMBER,
                        score=0.90,
                    )
                )
            out.append(spans)
        return out


class PresidioProvider(NERProvider):  # pragma: no cover - exercised via mocks in tests
    def __init__(self) -> None:
        self._engine = None
        self._init()

    def _init(self) -> None:
        if self._engine is not None:
            return
        # Defer heavy imports
        if __import__("os").environ.get("CPS_OFFLINE"):
            self._engine = None
            return
        try:
            from presidio_analyzer import AnalyzerEngine  # type: ignore

            self._engine = AnalyzerEngine()
        except Exception:
            self._engine = None

    def analyze_batch(self, texts: Sequence[str], language: str = "en") -> list[list[NERSpan]]:
        if self._engine is None:
            return [[] for _ in texts]
        out: list[list[NERSpan]] = []
        for text in texts:
            try:
                results = self._engine.analyze(text=text, language=language)  # type: ignore[no-untyped-call]
            except Exception:
                results = []
            spans: list[NERSpan] = []
            for r in results or []:
                # Presidio entity labels; map to our PIIType
                et = str(getattr(r, "entity_type", "")).upper()
                start = int(getattr(r, "start", 0))
                end = int(getattr(r, "end", 0))
                score = float(getattr(r, "score", 0.0))
                text_span = text[start:end]
                if et in {"PERSON", "PER"}:
                    label = PIIType.PERSON
                elif et in {"EMAIL", "EMAIL_ADDRESS"}:
                    label = PIIType.EMAIL
                elif et in {"PHONE", "PHONE_NUMBER", "PHONENUMBER"}:
                    label = PIIType.PHONE_NUMBER
                else:
                    # ignore other labels for now
                    continue
                spans.append(NERSpan(span=Span(start, end, text_span), label=label, score=score))
            out.append(spans)
        return out


def get_provider(cfg: NERConfig) -> NERProvider:
    if cfg.provider == "presidio":
        return PresidioProvider()
    return SpaCyProvider(model=cfg.spacy_model)


def detect_ner_spans(
    texts: Sequence[str], cfg: NERConfig | None = None, provider: NERProvider | None = None
) -> list[list[NERSpan]]:
    """Detect NER spans for a batch of texts using the configured provider.

    - Applies confidence gating from cfg if provided (post-filter at consumer if custom).
    - Returns a list aligned to the input texts; each entry is a list of NERSpan.
    """
    cfg = cfg or NERConfig()
    prov = provider or get_provider(cfg)
    all_spans = prov.analyze_batch(texts, language=cfg.language)
    # Apply global gating
    gated: list[list[NERSpan]] = []
    for spans in all_spans:
        gated.append([s for s in spans if s.score >= cfg.confidence_min])
    return gated


def merge_with_rules(
    text: str,
    ner_spans: Iterable[NERSpan],
    confidence_min: float = 0.0,
) -> dict[PIIType, float]:
    """Merge NER signals with rules layer by type and emit combined scores.

    Strategy: per PII type, take the max across NER scores (after thresholding) and
    rules heuristic scores if present (via direct regex confidences implied by patterns).
    - EMAIL/PHONE from rules are treated as 0.95/0.85 priors (matching rules.py constants)
    - PERSON defaults to NER only (rules PERSON is weak and optional)
    """
    from .rules import propose_candidates  # local import to avoid cycles during import time

    # NER contribution
    per_type: dict[PIIType, float] = {}
    for s in ner_spans:
        if s.score < max(0.0, confidence_min):
            continue
        per_type[s.label] = max(per_type.get(s.label, 0.0), float(s.score))

    # Rules contribution
    for c in propose_candidates(text):
        if c.rule_label is None:
            continue
        per_type[c.rule_label] = max(per_type.get(c.rule_label, 0.0), float(c.rule_confidence))
    return per_type


# ---------------- Context-level signals (existing) ----------------


def ner_context_signals(context_texts: dict[int, str]) -> dict[int, dict[str, float]]:
    """Run spaCy NER on sanitized context windows only.

    Returns a per-candidate dict of soft signals derived from context entity labels.
    This function never sees raw PII; contexts are expected to be redacted upstream.
    """
    nlp = _load_spacy(model=None, language="en")
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
