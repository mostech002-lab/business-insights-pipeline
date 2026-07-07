# Business Insights Pipeline

A production-style, all-AWS batch data pipeline that ingests transactional order
data from SQL Server, processes it with PySpark across a medallion data lake, and
serves business metrics (CLV, RFM, churn, sales trends, loyalty, location,
pricing) to a Streamlit dashboard.

> **Design principle:** all processing in PySpark, all infrastructure on AWS, no
> external/licensed tools. Every service choice is justified in
> [`docs/solution_design.docx`](docs/solution_design.docx) — the "why," not just
> the "what."

---

## Architecture

```
RDS (SQL Server)
   │  Glue + JDBC, incremental watermark (predicate push-down)
   ▼
S3 raw/  (bronze)      verbatim copy, partitioned by load_date
   │  Glue PySpark: type, standardize, quarantine, derive LINE_REVENUE
   ▼
S3 prod/ (silver)      typed + standardized, partitioned by ORDER_DATE
   │  Glue PySpark: aggregate CLV / RFM / metrics
   ▼
S3 reporting/ (gold)   small pre-aggregates, unpartitioned
   │  direct Parquet read
   ▼
Streamlit dashboard
```

Cross-cutting: **Step Functions** orchestrates the three Glue stages,
**EventBridge** triggers the daily run, failures alert via **SNS** and quarantine
bad records to `rejects/`. **SSE-KMS**, **TLS**, **Secrets Manager**, and
least-privilege **IAM** apply throughout. Full diagram in
[`docs/architecture/`](docs/architecture/).

### Why these choices (short form)

| Decision | Choice | Why |
|---|---|---|
| Ingest | Glue + JDBC | Native PySpark, no new license; watermark = incremental. |
| Incremental state | DynamoDB control table | Atomic conditional write; can't be clobbered by a stale run; reusable across tables. |
| Processing | Glue (not EMR) | Bursty scheduled batch → ephemeral, per-second billing; no idle cluster. |
| Storage | 1 bucket, 4 prefixes | Simple lifecycle, per-prefix IAM; medallion raw/prod/reporting + rejects. |
| Orchestration | Step Functions | JSON state passing, Choice branching, per-state Retry/Catch, visual DAG. |
| Idempotency | Dynamic partition overwrite | Re-run a date → overwrite only that partition; no double-counted RUNNING_LTV. |
| Serving | Direct Parquet read | Gold is small/pre-aggregated; Athena only if it outgrows memory or needs SQL push-down. |

---

## Repo layout

```
business-insights-pipeline/
├── glue/                 # PySpark Glue jobs (flat modules; deploy via --extra-py-files)
│   ├── lib_ingest.py         # watermark, secrets/JDBC, config-driven query builder, read/write
│   └── ingest_raw_job.py     # Layer 1 entrypoint: RDS → S3 raw/
├── orchestration/        # Step Functions state machine definition
├── infra/                # IaC: S3, DynamoDB watermark table, IAM, Glue, KMS
├── notebooks/            # EDA + one-time RDS setup/load notebooks
├── docs/                 # design doc, architecture diagram, step notes, requirements
├── data/samples/         # small committed sample (full data is gitignored)
├── dashboard/            # Streamlit app (Step 6)
└── tests/                # pytest — pure-logic unit tests (no Spark/AWS needed)
```

---

## Load strategies

The ingest job is **config-driven** (`glue/lib_ingest.py::TABLE_CONFIG`) — adding a
source table is a config edit, not a code change:

| Table | Strategy | Mechanism |
|---|---|---|
| `order_items` | incremental watermark | `WHERE CREATION_TIME_UTC > <watermark>` pushed to SQL Server |
| `order_item_options` | incremental by parent | `EXISTS` semi-join to newly-loaded `order_items` (no timestamp of its own) |
| `date_dim` | full reload | bounded static dimension (365 rows) |

The watermark is stored in DynamoDB and advanced **once per run**, from the actual
batch max — `order_items` and `order_item_options` share the same pre-run watermark.

---

## Quickstart (local)

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env      # fill in your values (never commit .env)
pytest -q                 # runs pure-logic tests, no AWS/Spark required
```

### Deploying the ingest job to Glue

- Upload `glue/ingest_raw_job.py` as the job script.
- `--extra-py-files s3://.../lib_ingest.py`
- `--extra-jars s3://.../mssql-jdbc-<ver>.jar`  (Glue doesn't bundle the SQL Server driver)
- Job args: `--load_date`, `--secret_name`, `--raw_base_path`
- Attach a Glue **security configuration** so S3/CloudWatch/bookmarks are KMS-encrypted.

---

## Status

- [x] Step 1 — Source data explanation & integrity
- [x] Step 2 — Schema, relationships & insight map
- [x] Step 3 — Architecture + solution design
- [ ] **Step 4 — Build the pipeline** ← in progress
  - [x] Layer 1 — Glue JDBC ingest (RDS → raw/)
  - [ ] Silver transform (raw → prod)
  - [ ] Gold transform (prod → reporting)
- [ ] Step 5 — Metrics
- [ ] Step 6 — Streamlit dashboard
- [ ] Step 7 — CI/CD, docs, demo

See [`docs/PROGRESS.md`](docs/PROGRESS.md) for the detailed log.
