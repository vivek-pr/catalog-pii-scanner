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
  A[Connectors\n(Hive | Glue | Unity)] --> B[Normalizer]
  B --> C[Detection Engine]
  C --> C1[Rules/Regex+Luhn]
  C --> C2[NER (Presidio/spaCy)]
  C --> C3[Embeddings Classifier\n(sentence-transformers)]
  C --> C4[LLM Fallback (optional)]
  C1 --> D[Ensemble Fusion]
  C2 --> D
  C3 --> D
  C4 --> D
  D --> E[Actions\nTag/Comment/Alerts]
  D --> F[Results Store\n(SQLite/Postgres)]
  G[Watchers\nPoll | EventBridge | HMS Listener] --> B
