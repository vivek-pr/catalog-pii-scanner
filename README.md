# catalog-pii-scanner

Universal **PII discovery & tagging** for enterprise metadata catalogs:
- **Metastores:** Apache Hive Metastore, AWS Glue Data Catalog, Databricks Unity Catalog
- **Engines:** Regex + checksum + **NLP (Microsoft Presidio/spaCy)**
- **Modes:** Full/batch scans + incremental/real-time watchers
- **Outputs:** Native tags/comments in catalogs, JSON/CSV reports, web API
- **Compliance:** GDPR, CCPA, HIPAA, PCI-DSS aligned metadata classification
- **Design principle:** **Never** persist raw PII; only store classifications & metrics

> If you don’t know exactly where PII sits, you can’t protect it. This repo gives you fast, accurate, audit-friendly discovery at metadata scale.

---

## Features

- **Wide PII coverage:** names, emails, phones, national IDs, credit cards (Luhn), IP/MAC, DOB, addresses, healthcare IDs, country-specific formats (extensible).
- **Two-pass detection:** 
  1) **Metadata inference** (names/descriptions/tags)
  2) **Data sampling** (configurable %, threshold, NLP) for proof.
- **Real-time & batch:** initial full sweep + continuous watch via event/listener/polling.
- **Tagging back:** writes `pii=true` + granular types (e.g., `pii_types=Email,Phone`) into Glue/Unity/Hive; optional comments with last-scan timestamp.
- **Thin and safe:** zero storage of sensitive values; sanitized logs; least-privilege IAM/roles.
- **Pluggable:** add connectors (Atlas/Collibra/Purview) and recognizers without touching core.

---

## Architecture (high level)

```mermaid
flowchart LR
  A[Connectors\n(Hive | Glue | Unity)] --> B[Metadata Normalizer]
  B --> C[Detection Engine\nRegex + Luhn + Presidio]
  C -->|Classifications| D[Actions\nTag/Comment Back]
  C -->|Findings| E[Results Store\n(Postgres/SQLite/JSON)]
  E --> F[Reports/Exports\n(JSON/CSV)]
  E --> G[REST API / CLI]
  H[Watchers\n(EventBridge | HMS Listener | Poll)] --> B
````

---

## Supported targets

* **Hive Metastore:** Thrift/JDBC for schema; optional SQL sample via JDBC/Spark.
* **AWS Glue Data Catalog:** Boto3 `GetDatabases/GetTables`; optional sampling via Athena/Glue/Spark.
* **Databricks Unity Catalog:** REST API / JDBC (information\_schema); sample via Spark in-workspace or external JDBC.

> Optional sinks: **DataHub**/Amundsen tagging via REST, Slack/Jira alerts for critical hits (e.g., PAN/SSN).

---

## Quickstart

### Prereqs

* Python **3.11+** (or Docker)
* Optional: Spark 3.x (Databricks/EMR/local) for distributed scans
* Credentials:

  * **AWS**: IAM perms for Glue read, S3/Athena read for sampling, optional Lake Formation tag writes
  * **Hive**: read metadata; optional SELECT for sampling
  * **Databricks**: PAT + UC read; optional comment/tag write
* NLP (optional but recommended): `presidio-analyzer`, `spacy` model(s)

### Install (local)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -U pip wheel
pip install catalog-pii-scanner[full]   # when published; for now use editable:
# pip install -e .
```

### Or Docker

```bash
docker build -t catalog-pii-scanner .
docker run --rm -it \
  -v $PWD/config:/app/config \
  -e AWS_REGION=us-east-1 \
  catalog-pii-scanner cps scan --full --config config/config.yaml
```

---

## Configuration

Create `config/config.yaml`:

```yaml
app:
  results_store: sqlite:///cps_results.db  # postgres://user:pass@host/db for team use
  redact_logs: true
  timezone: "Asia/Kolkata"

detection:
  enable_metadata_pass: true
  enable_content_pass: true
  sample:
    rows_per_column: 500         # cost/latency lever
    percent_per_table: 0.05      # alternative to rows_per_column
  thresholds:
    hit_rate_min: 0.10           # >=10% matches => classify column
    confidence_min: 0.60         # Presidio confidence gate
  pii_sets: ["universal", "finance", "healthcare", "country_in"] # extend as needed
  languages: ["en"]              # e.g., ["en","hi","es"]; requires model packs
  recognizers:
    credit_card: true
    email: true
    phone: true
    ip: true
    mac: true
    national_ids:
      - type: aadhaar
        regex: "(?<!\\d)(\\d{4}\\s\\d{4}\\s\\d{4})(?!\\d)"
        checksum: null
      - type: pan_india
        regex: "(?<![A-Z])[A-Z]{5}[0-9]{4}[A-Z](?![A-Z])"
        checksum: null

connectors:
  glue:
    enabled: true
    regions: ["ap-south-1","us-east-1"]
    tag_back: true
    comment_back: true
    athena:
      workgroup: "PIIScanner"
      database: "pii_temp"
  hive:
    enabled: true
    metastore_uri: "thrift://hive-metastore:9083"
    jdbc_url: "jdbc:hive2://hive-server:10000/default"
    tag_back: true
    comment_back: true
  unity:
    enabled: true
    host: "https://dbc-xxxxx.cloud.databricks.com"
    pat_env: "DATABRICKS_TOKEN"
    tag_back: true
    comment_back: true

watchers:
  enable: true
  strategy: "poll"  # options: poll | events
  poll_interval_seconds: 900

actions:
  writeback:
    enable: true
    add_tags: ["pii=true"]
    types_tag_key: "pii_types"
  alerts:
    slack_webhook: ""    # optional
    high_severity_types: ["credit_card","ssn","national_id"]
    create_jira: false
```

---

## CLI

```bash
# Full catalog sweep (all connectors enabled)
cps scan --full --config config/config.yaml

# Incremental: scan items added/changed in last 24h
cps scan --since 24h --config config/config.yaml

# Targeted: one catalog/table
cps scan --target unity://catalog.schema.* --config config/config.yaml

# Serve REST API (GET /health, POST /scan, GET /findings)
cps serve --port 8080 --config config/config.yaml

# Dry-run (no writebacks)
cps scan --full --no-writeback

# Export results (JSON/CSV)
cps export --format json --out findings.json
```

**Return codes**

* `0`: OK
* `2`: Findings contain **high-severity** PII (useful in CI to block merges)
* `3`: Connector/permission error(s)

---

## Output & Tagging

* **Per column** classification with:

  * `types`: `[Email, Phone, CreditCard, Aadhaar, ...]`
  * `confidence`: 0–1 (max of recognizers)
  * `hit_rate`: matched / sampled
  * `last_scanned_at`: ISO8601
* **Write-back**:

  * Glue/Unity/Hive tags: `pii=true`, `pii_types=Email,Phone`
  * Optional **comment**: `Contains PII: Email, Phone (scanned 2025-08-31 IST)`
* **Reports**: `reports/` JSON/CSV, plus summary by regulation (PCI/HIPAA/GDPR categories)

---

## Permissions (minimum)

* **AWS Glue/Athena/S3**: `glue:Get*`, `athena:StartQueryExecution/GetQueryResults`, read S3 on datasets; optional Lake Formation tag write.
* **Hive**: HMS read; optional `SELECT` for sampling.
* **Databricks UC**: UC metadata read; optional comment/tag APIs; Spark read for sampling (workspace/job with cluster policy).
* Store creds via env/secret manager; **never** in config files.

---

## Security posture

* **No raw PII persisted**; sampling in memory only.
* Logs are **sanitized**; redact patterns; opt-out of sampling per dataset.
* Principle of Least Privilege (service roles); audit log of scans & writebacks.

---

## Dev setup

```bash
make setup          # lint/format hooks
make test           # unit tests
make integ          # spins localstack/hms for connector tests
make run            # dev run with sample config
```

### Repo layout

```
catalog-pii-scanner/
├─ cps/                     # core package
│  ├─ cli.py                # click/typer CLI
│  ├─ server.py             # FastAPI REST
│  ├─ config.py             # pydantic config loader
│  ├─ connectors/           # glue|hive|unity|...
│  ├─ detectors/            # regex|luhn|presidio wrappers
│  ├─ actions/              # writeback|alerts|exports
│  ├─ store/                # sqlite/postgres models
│  └─ watchers/             # pollers/event listeners
├─ tests/
├─ docker/
├─ scripts/
└─ README.md
```

---

## Roadmap (short, realistic)

* [ ] Collibra/Atlas/Purview connectors
* [ ] Lineage-aware propagation (downstream flagging)
* [ ] RBAC’d web UI dashboard (findings, trends, overrides)
* [ ] Built-in **country packs** (ID formats per geography)
* [ ] Presidio model bundles + language auto-detect
* [ ] Helm chart + K8s EventBridge watcher sidecar

---

## Limitations (don’t kid yourself)

* Sampling can miss low-frequency PII — tune `rows_per_column` and thresholds.
* Name/Address detection is inherently noisy; rely on **context + multiple signals**.
* Real-time **listener** coverage depends on platform capabilities; fallback is polling.

---

## License

Apache-2.0 (proposed).

---

## Compliance mapping (cheat sheet)

* **PCI-DSS**: detect PAN + Luhn, tag **PCI scope**, alert.
* **HIPAA**: mark PHI identifiers (name, DOB, MRN, plan IDs, etc.).
* **GDPR/CCPA**: personal identifiers + audit trail + inventory export.

---

## Example: minimal run (Glue only)

```yaml
connectors:
  glue:
    enabled: true
    regions: ["ap-south-1"]
    tag_back: true
    comment_back: true
detection:
  enable_content_pass: false   # fast metadata-only pass to start
```

```bash
cps scan --full --config config/config.yaml --no-writeback   # dry run
cps scan --since 24h --config config/config.yaml             # incremental with tagging
```

---

## FAQ

**Q: Will this slow down prod?**
A: Metadata pass is trivial. Content pass uses **small, bounded samples**; run off-peak or via isolated compute (Athena/Spark). You control the dials.

**Q: Can it miss PII?**
A: Yes, if you sample too little or names are obfuscated. That’s why we combine **names/descriptions + patterns + NLP** and let you increase sampling where it matters.

**Q: Does it store PII?**
A: No. It stores **classifications & metrics only**.

```
