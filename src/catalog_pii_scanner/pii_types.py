from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Any


class PIIType(str, Enum):
    EMAIL = "EMAIL"
    PHONE_NUMBER = "PHONE_NUMBER"
    CREDIT_CARD = "CREDIT_CARD"
    SSN = "SSN"
    IP_ADDRESS = "IP_ADDRESS"
    MAC_ADDRESS = "MAC_ADDRESS"
    AADHAAR = "AADHAAR"
    PAN = "PAN"
    PERSON = "PERSON"
    ADDRESS = "ADDRESS"
    DATE = "DATE"


ALL_PII_TYPES: tuple[PIIType, ...] = (
    PIIType.EMAIL,
    PIIType.PHONE_NUMBER,
    PIIType.CREDIT_CARD,
    PIIType.SSN,
    PIIType.IP_ADDRESS,
    PIIType.MAC_ADDRESS,
    PIIType.AADHAAR,
    PIIType.PAN,
    PIIType.PERSON,
    PIIType.ADDRESS,
    PIIType.DATE,
)


@dataclass(frozen=True)
class Span:
    start: int
    end: int
    text: str


@dataclass
class Candidate:
    span: Span
    # initial "rule" label guess (optional)
    rule_label: PIIType | None = None
    # rule confidence in [0,1]
    rule_confidence: float = 0.0
    # checksum or structural validations per type (True/False flags)
    validations: dict[PIIType, bool] | None = None


@dataclass
class Prediction:
    span: Span
    # per-type probabilities from ensemble
    probs: dict[PIIType, float]
    # chosen label and score for convenience
    label: PIIType | None = None
    score: float = 0.0
    # trace of model signals for debugging/analysis
    signals: dict[str, Any] | None = None


JsonLabel = dict[str, Any]


def to_json_label(span: Span, label: PIIType, score: float) -> JsonLabel:
    return {"start": span.start, "end": span.end, "type": label.value, "score": score}


def from_json_label(obj: dict[str, Any]) -> tuple[Span, PIIType]:
    span = Span(start=int(obj["start"]), end=int(obj["end"]), text=str(obj.get("text", "")))
    return span, PIIType(str(obj["type"]))
