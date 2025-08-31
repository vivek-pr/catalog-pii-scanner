
# catalog-pii-scanner

Universal **PII discovery & tagging** for enterprise metadata catalogs—now with an **AI ensemble**:
- **Targets:** Apache Hive Metastore, AWS Glue, Databricks Unity
- **Engines:** Regex + checksum + **NER (Microsoft Presidio/spaCy)** + **Embeddings classifier (sentence-transformers)** + optional **LLM fallback**
- **Modes:** Full/batch scans + incremental/real-time watchers
- **Outputs:** Native tags/comments, JSON/CSV reports, REST API
- **Compliance:** GDPR, CCPA, HIPAA, PCI-DSS
- **Security:** No raw PII persisted; redaction before model features/LLM

---

## Why AI?
Regex alone misses context and creates noise. We fuse:
1) **Rules** (names/descriptions/tags, checksum, strict regex)  
2) **NER** (Presidio/spaCy—PERSON, EMAIL, PHONE, etc.)  
3) **Embeddings classifier** on **metadata text** + **redacted samples**  
4) **LLM fallback** (optional) for ambiguous columns with strict redaction & cost caps

**Ensemble fusion** (calibrated scores) → higher precision/recall with audit trails.

---

## Architecture

```mermaid
flowchart LR
  A["Connectors: Hive, Glue, Unity"] --> B["Normalizer"]
  B --> C["Detection Engine"]
  C --> C1["Rules / Regex + Luhn"]
  C --> C2["NER (Presidio & spaCy)"]
  C --> C3["Embeddings Classifier (sentence-transformers)"]
  C --> C4["LLM Fallback (optional)"]
  C1 --> D["Ensemble Fusion"]
  C2 --> D
  C3 --> D
  C4 --> D
  D --> E["Actions: Tag, Comment, Alerts"]
  D --> F["Results Store (SQLite, Postgres)"]
  G["Watchers: Poll, EventBridge, HMS Listener"] --> B

````

---

## Feature highlights

* **AI Ensemble:** pluggable scorers + logistic/stacking fusion with calibrated thresholds
* **Safe redaction pipeline:** Presidio masks entities in samples before feature/LLM use
* **Embedding model:** default `sentence-transformers/all-MiniLM-L6-v2` (local), configurable
* **LLM guardrails:** redacted prompts, token & budget caps, caching, on-prem option
* **Evaluation:** golden dataset, per-type precision/recall/F1, PR curves, confusion matrix
* **Drift & audit:** run logs, model versioning, drift monitors

---

## Quickstart

### Prereqs

* Python **3.11+** (or Docker)
* Optional: Spark 3.x (for v0.3 scale)
* Presidio, spaCy model(s), sentence-transformers
* Credentials per connector (Glue/Hive/Unity)

### Install

```bash
python -m venv .venv && source .venv/bin/activate
pip install -U pip wheel
pip install -e ".[full,ml]"   # extras: full, ml, llm, spark
```

### Dev skeleton (this repo)

For local development using the baseline skeleton:

```bash
make setup        # create venv, install deps, install pre-commit
make test         # run tests
make fmt && make lint

# CLI help
cps --help

# Run FastAPI stub (two options)
make run-api                       # uvicorn with reload
cps serve --host 0.0.0.0 --port 8000 --reload
```

Docker build and run the CLI help:

```bash
docker build -t cps .
docker run --rm cps --help
```

### Repository layout

```
catalog-pii-scanner/
├─ src/
│  └─ catalog_pii_scanner/
│     ├─ __init__.py           # version
│     ├─ cli.py                # Typer CLI (cps)
│     └─ api.py                # FastAPI stub (GET /healthz)
├─ tests/                      # pytest
│  ├─ test_cli.py
│  └─ test_api.py
├─ pyproject.toml              # packaging + tooling config
├─ Makefile                    # setup / fmt / lint / test
├─ .pre-commit-config.yaml     # ruff, black, mypy
├─ Dockerfile                  # non-root runtime image
├─ .dockerignore
└─ .gitignore
```

### Minimal run (deterministic + NER + embeddings; no LLM)

```bash
cps scan --full --config config/config.yaml
cps export --format json --out findings.json
```

---

## Configuration (AI section)

```yaml
ai:
  mode: "ensemble"         # options: rules | ensemble | ensemble+llm
  ner:
    enabled: true
    provider: "presidio"   # or "spacy"
    confidence_min: 0.60
  embeddings:
    enabled: true
    model: "sentence-transformers/all-MiniLM-L6-v2"
    device: "cpu"          # or "cuda"
    features:
      from_metadata: true  # names/descriptions/tags
      from_samples: true   # after redaction only
  ensemble:
    weights:
      rules: 0.4
      ner: 0.3
      embed: 0.3
    decision_threshold: 0.55
  llm:
    enabled: false
    provider: "local"       # local|openai|azure|vertex
    model: "llama3-8b-instruct"
    max_tokens: 256
    temperature: 0.0
    redact: true
    cost_cap_usd_per_scan: 0.50
    cache_ttl_minutes: 1440
```

---

## CLI

```bash
# Scan
cps scan --full --config config/config.yaml
cps scan --since 24h --config config/config.yaml

# Train / Evaluate AI
cps train --dataset data/golden.csv --out models/pii-ensemble-v1
cps eval --dataset data/golden.csv --model models/pii-ensemble-v1 --report reports/eval.json

# Serve API
cps serve --port 8080 --config config/config.yaml
```

---

## Data & Privacy

* **No raw PII persisted.**
* Sampling is **in-memory**; logs redacted.
* **Embeddings/LLM get only redacted text** (e.g., “My email is \[EMAIL]”).
* On-cloud LLMs are **opt-in**; default is local models.

---

## Outputs

* Per-column: `types[]`, `confidence`, `hit_rate`, `model_version`, `last_scanned_at`
* Tag back: `pii=true`, `pii_types=...` + comment with timestamp
* Reports: JSON/CSV; eval reports with metrics by PII type & regulation mapping

---

## Roadmap

* v0.1-ML-MVP: ensemble, eval harness, tag-back, CI
* v0.2-Realtime-LLM: listeners, LLM fallback, active learning + review UI
* v0.3-Scale-MLOps: Spark inference, MLflow registry, drift monitoring
