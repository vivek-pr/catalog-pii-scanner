from __future__ import annotations

import json
import random
from collections.abc import Iterable
from dataclasses import dataclass

from .pii_types import PIIType, Span, from_json_label


@dataclass
class LabeledExample:
    text: str
    labels: list[tuple[Span, PIIType]]


def _random_email() -> str:
    users = ["john.doe", "jane_smith", "a.brown", "user123"]
    domains = ["example.com", "sample.org", "test.net"]
    return f"{random.choice(users)}@{random.choice(domains)}"


def _random_phone() -> str:
    # US-like phone
    return f"({random.randint(200,999)}) {random.randint(200,999):03d}-{random.randint(0,9999):04d}"


def _luhnify(base16: str) -> str:
    # Create a plausible credit card number with Luhn checksum
    digits = [int(c) for c in base16 if c.isdigit()]
    while len(digits) < 15:
        digits.append(random.randint(0, 9))
    # compute check digit
    s = 0
    parity = (len(digits) + 1) % 2
    for i, d in enumerate(digits):
        if i % 2 == parity:
            d *= 2
            if d > 9:
                d -= 9
        s += d
    check = (10 - (s % 10)) % 10
    return "".join(map(str, digits)) + str(check)


def _random_cc() -> str:
    prefix = random.choice(["4", "51", "52", "53", "54", "55"])  # Visa/Mastercard-ish
    return _luhnify(prefix + "0" * 14)


def _random_ssn() -> str:
    return f"{random.randint(100,999)}-{random.randint(10,99):02d}-{random.randint(1000,9999):04d}"


def _random_ip() -> str:
    return ".".join(str(random.randint(1, 254)) for _ in range(4))


def _random_name() -> str:
    first = random.choice(["John", "Jane", "Alice", "Bob", "Carlos", "Emily"])
    last = random.choice(["Doe", "Smith", "Brown", "Johnson", "Davis", "Miller"])
    return f"{first} {last}"


def _random_date() -> str:
    y = random.randint(1990, 2024)
    m = random.randint(1, 12)
    d = random.randint(1, 28)
    return f"{y:04d}-{m:02d}-{d:02d}"


def generate_synthetic(n: int = 200, seed: int = 1234) -> list[LabeledExample]:
    rnd = random.Random(seed)
    examples: list[LabeledExample] = []
    templates = [
        "Contact {name} via email {email} or phone {phone}.",
        "Visa card {cc} expires on {date}.",
        "SSN for {name} is {ssn}.",
        "Server IP {ip} logged a request from {name} on {date}.",
        "Primary contact: {email}. Secondary: {phone}.",
    ]
    for _ in range(n):
        t = rnd.choice(templates)
        values: dict[str, str] = {
            "name": _random_name(),
            "email": _random_email(),
            "phone": _random_phone(),
            "cc": _random_cc(),
            "ssn": _random_ssn(),
            "ip": _random_ip(),
            "date": _random_date(),
        }
        text = t.format(**values)
        labels: list[tuple[Span, PIIType]] = []
        for key, val in values.items():
            start = text.find(val)
            if start == -1:
                continue
            end = start + len(val)
            span = Span(start=start, end=end, text=val)
            if key == "email":
                labels.append((span, PIIType.EMAIL))
            elif key == "phone":
                labels.append((span, PIIType.PHONE_NUMBER))
            elif key == "cc":
                labels.append((span, PIIType.CREDIT_CARD))
            elif key == "ssn":
                labels.append((span, PIIType.SSN))
            elif key == "ip":
                labels.append((span, PIIType.IP_ADDRESS))
            elif key == "name":
                labels.append((span, PIIType.PERSON))
            elif key == "date":
                labels.append((span, PIIType.DATE))
        examples.append(LabeledExample(text=text, labels=labels))
    return examples


def save_jsonl(path: str, data: Iterable[LabeledExample]) -> None:
    with open(path, "w", encoding="utf-8") as f:
        for ex in data:
            row = {
                "text": ex.text,
                "labels": [
                    {"start": s.start, "end": s.end, "type": t.value, "text": s.text}
                    for s, t in ex.labels
                ],
            }
            f.write(json.dumps(row) + "\n")


def load_jsonl(path: str) -> list[LabeledExample]:
    out: list[LabeledExample] = []
    with open(path, encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            obj = json.loads(line)
            labels = []
            for lbl in obj.get("labels", []):
                span, t = from_json_label(lbl)
                labels.append((span, t))
            out.append(LabeledExample(text=obj["text"], labels=labels))
    return out
