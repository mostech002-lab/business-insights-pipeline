# Business Insights Assessment — Progress Tracker

**Owner:** Mohammad Saim · **Goal:** FAANG-ready portfolio project
**Working style:** Guide, don't hand answers. For coding, walk through step by step (no full solutions up front). Give the answer only after 3 wrong attempts. Track progress with a task list each session.

---

## ✅ Step 1 — Source Data Explanation & Integrity (DONE)
Deliverable: `Step1_Data_Quality_Notes.md`
- 3 files verified: `order_items` 203,519 / `order_item_options` 193,017 / `date_dim` 365 — all match spec.
- **Key fix:** naive read gave 203,531 rows + null "staircase." Cause = embedded newlines in text fields. Fixed with `multiLine="true"`.
- `inferSchema` typo (`inferShema`) was silently ignored → all strings; fixed spelling. But inference mistyped `PRINTED_CARD_NUMBER` as Long → **IDs must be strings** (leading zeros / overflow). Prefer explicit `StructType` in the pipeline.
- 1 corrupt row (null `LINEITEM_ID`) → drop + log. 17,808 null `USER_ID` = guest checkouts → keep, exclude from customer-level metrics only.

## ✅ Step 2 — Schema, Relationships & Insight Map (DONE)
Deliverable: `Step2_Schema_Relationships.md`
- Grains: `order_items` = line item (`LINEITEM_ID`); `order_item_options` = option; `date_dim` = date.
- **Relationships:** `order_items → order_item_options` = **one-to-many** (`ORDER_ID`+`LINEITEM_ID`; max 152 options on one item; 28 orphan options flagged). `order_items → date_dim` = **many-to-one** (`to_date(CREATION_TIME_UTC)` = `date_key`).
- **Revenue (fan-out fix):** pre-aggregate options to line grain first → `TOTAL_OPTIONS_PRICE = Σ(OPTION_PRICE×OPTION_QUANTITY)`; left-join to items; `LINE_REVENUE = ITEM_PRICE×ITEM_QUANTITY + coalesce(TOTAL_OPTIONS_PRICE, 0)`.
- Roll-up chain: option → line → order → **(USER_ID, date)** → lifetime. **CLV grain = (USER_ID, date).**
- Column→metric map complete. `RESTAURANT_ID` = the location dimension.

## Work artifacts
- `EDA.ipynb` — PySpark load, integrity checks, joins, revenue column.
- Data lives locally; read with `header/multiLine/inferSchema` (move to explicit schema in pipeline).

---

## 🔄 Step 3 — Architecture + Solution Design + SME Sign-off (IN PROGRESS)
Requirements:
- Design a **production** AWS pipeline (all AWS, no Snowflake/DBT/external tools, no new licenses). Source = **SQL Server**. All logic in **PySpark**.
- Must include: scheduling, encryption, failure/reload mechanism.
- Flow: SQL Server → ingestion → S3 (raw/curated) → Glue/EMR PySpark → S3 curated → Streamlit.
- Deliverables: architecture diagram (draw.io/miro), solution design doc (with the "WHY" per AWS choice), **written SME approval**. Do not build until SME clears.

### ✅ 3a. CLV data model — DESIGNED (primary goal, DONE)
- **Storage = Design A**: one row per `(USER_ID, order-DAY)`. Compact. **Serve** the "value on any day" experience (Design B) via **forward-fill / as-of join** at query time — no need to store a row per customer per calendar day.
- **Final columns:** `USER_ID` · `SNAPSHOT_DATE` · `DAILY_SPEND` (Decimal) · `RUNNING_LTV` (Decimal) · `CLV_TAG` (High/Med/Low).
- **Build logic:** (1) `groupBy(USER_ID, ORDER_DATE).agg(sum(LINE_REVENUE)) → DAILY_SPEND` (collapse line items to one row/day first — critical, or the running sum is wrong). (2) Window `partitionBy(USER_ID).orderBy(ORDER_DATE)` + `sum(DAILY_SPEND)` → `RUNNING_LTV`. Verified on 1 customer: LTV climbs 9.99 → 145.82. ✔
- **CLV_TAG:** recompute so tags can **migrate** (Low→Med→High). Rank over the **FULL customer base as-of the date** (carry-forward each customer's latest LTV) — NOT just that day's buyers. Refresh **cadence** can be decoupled (e.g., weekly) but population is always the full base.
- **Money = Decimal, never double** (saw float drift `103.86999999999999`).
- **RFM segmentation is a SEPARATE table**, weekly/biweekly cadence fine (windowed metric, fluctuates). CLV is cumulative/monotonic so it doesn't whipsaw. Keep the two distinct.

### ✅ 3b. Layer 1 Ingestion — DECIDED
- **AWS Glue + JDBC** reads SQL Server → S3. WHY: Glue runs PySpark natively → one tool, one language, no new license. (DMS considered, rejected: 2nd service, not PySpark.)
- **Incremental load** via **watermark** on `CREATION_TIME_UTC` (`WHERE CREATION_TIME_UTC > last_watermark`; save max each run in DynamoDB/SSM/S3) — OR **Glue Job Bookmarks** (managed equivalent). Assumption to state: insert-timestamp catches NEW rows, not edits → fine for append-only orders.

### ✅ 3c. Environment / source setup — DONE (Mac)
- Source SQL Server is on **Amazon RDS** (already created).
- **SSMS replacement:** Azure Data Studio is **retired (Feb 28, 2026)** → using **VS Code + MSSQL extension** (installed & connected). DBeaver is the alt.
- RDS connect prereqs: Publicly accessible = Yes + security group inbound TCP 1433 from your IP.
- **Loaded 3 CSVs into RDS** via Python (`pandas` + `SQLAlchemy` + `pymssql`), **philosophy B** (raw landing, all typing/validation deferred to PySpark).
  - Schemas: permissive VARCHAR landing; hybrid kept `DATETIME2`/`DECIMAL`/`INT` on clean cols; `order_items.LINEITEM_ID NOT NULL`.
  - Loader pattern: `read_csv(dtype=str, na_values=[""])` → filter null `LINEITEM_ID` → parse timestamp `tz_localize(None)` + `to_numeric(errors="coerce")` → `to_sql(if_exists="append", chunksize=1000)`.
  - **order_items = 203,518 loaded** ✔ (dropped 1 corrupt). options loader → expect **193,017**; date_dim → expect **365** (loaders written, run pending / likely done).

### ✅ 3d. Layer 2 — S3 storage zones — DECIDED
- **One bucket, four prefixes** (WHY: simpler lifecycle; IAM still scopable per-prefix — no need for 4 buckets on this project).
- Medallion mapping: `raw/` = bronze, `prod/` = silver, `reporting/` = gold, plus `rejects/` = ops/quarantine.

| Zone (folder) | Holds | Format | Partition |
|---|---|---|---|
| `raw/` | verbatim RDS pull, all strings, no typing | Parquet | `load_date` |
| `prod/` | typed + standardized (PySpark) | Parquet | `ORDER_DATE` |
| `reporting/` | aggregated CLV / RFM / metrics | Parquet | none |
| `rejects/` | quarantined bad records (corrupt row, orphan options) | Parquet | `load_date` |

- **Key pattern:** raw partitioned by **ingestion/load date** (no parsing of source → stays "dumb," replayable); curated partitioned by **business date** (`ORDER_DATE`) since we're transforming in PySpark there anyway.
- **Reporting NOT partitioned** — small aggregates; partitioning would cause small-file problem and Streamlit reads whole table anyway.
- **Ops split (3 different things):** rejected *records* → S3 `rejects/`; execution *logs* → **CloudWatch Logs**; *alert* → **SNS** (wired in Layer 4, not in the bucket).

### ✅ Layer 3 — Processing — DECIDED: **AWS Glue** (serverless PySpark); EMR rejected.
- **Cost:** job is bursty/scheduled (minutes/day) → EMR cluster would sit idle 23h burning money; Glue is ephemeral (spin up → run → tear down, per-second billing). Idle-cost is the core argument.
- **Ops overhead:** Glue fully managed — AWS owns runtime; nothing to patch/version/tune/babysit. EMR = own cluster patching, AMI/Spark version mgmt, scaling. (Precision: Glue isn't infinite auto-scale — still set worker count/type; it removes *server management*, not sizing.)
- **EMR boundary (the honest "why not EMR"):** EMR wins when cluster is highly-utilized/long-running — many analysts sharing interactively, sustained large-scale jobs, or deep Spark customization (versions/libs/tuning). At high steady volume economics flip (amortized always-on cluster beats repeated Glue per-job premium).
- **One-liner:** "Glue for scheduled bursty batch; EMR for sustained, shared, or heavily-customized Spark."
- **Glue's own weaknesses to disclose:** cold-start latency (~1 min startup — fine for batch, fatal for interactive) and less control over Spark internals.

### ✅ Layer 4 — Orchestration + failure/reload — DECIDED: **Step Functions** state machine.
- **Orchestrator = Step Functions** (not Glue Workflows, not MWAA). WHY over Glue Workflows: SF passes JSON state between states → Choice branching + per-state **Retry/Catch** with exponential backoff + native SNS/Lambda hooks + visual DAG. Glue Workflows only chains jobs with weak run-properties, no real conditional/retry. WHY not MWAA: overkill + always-on cost; only worth it for complex DAGs, backfills, non-AWS-native stacks.
- **Two triggers into ONE state machine:** (1) **EventBridge** cron rule = automatic daily; (2) manual **`StartExecution`** (console button / `aws stepfunctions start-execution`) = the reload path after a fix. (Rejected as primary triggers: Glue Trigger — not all-Glue; S3 event — pipeline starts at RDS not S3; cron on a box — machine-dependent, no retries/monitoring.)
- **(c) Retries:** transient failures → SF **Retry** with exponential backoff (increasing intervals, capped count). Exhausted → **Catch** does two things: SNS alert to analyst (+ mark execution failed) AND write bad records to `rejects/`.
- **(d) Idempotency = dynamic partition overwrite.** Spark `partitionOverwriteMode=dynamic` + `.mode("overwrite")`. Because `prod/` is partitioned by `ORDER_DATE`, a re-run overwrites ONLY that day's partition → job is idempotent → no double-counted `RUNNING_LTV`. Gold `reporting/` is idempotent differently: unpartitioned, fully recomputed off silver each run (overwrite-by-nature). **Key line: partitioning enables safe reprocessing, not just query speed.**
- **Full reload story:** run fails → fix bug → manual `StartExecution` for the date → transform reads preserved `raw/` → dynamic-overwrites `prod/` partition → recomputes `reporting/`. No duplication, no re-hitting RDS. ✔ (satisfies assessment's failure/reload requirement)

### ✅ Layer 5 — Security — DECIDED.
- **At rest:** S3 → **SSE-KMS** (customer-managed key; WHY over SSE-S3: key control + rotation/revoke + CloudTrail audit trail + key-level policy = defense in depth — S3 access alone isn't enough, need KMS decrypt too). RDS → encryption-at-rest on (KMS; covers storage, backups, snapshots, replicas). Glue → **security configuration** so temp shuffle/spill, CloudWatch logs, job bookmarks are KMS-encrypted too.
- **In transit (TLS):** RDS→Glue JDBC pull → force SSL (`encrypt=true`). Glue↔S3 → HTTPS by default, *enforced* via bucket policy denying `aws:SecureTransport=false`.
- **Secrets:** DB creds in **AWS Secrets Manager**; Glue fetches at runtime via its IAM role; supports auto-rotation. No password in scripts/config.
- **IAM least-privilege** across all components — each service its own role, minimum perms (e.g. Glue reads `raw/`, writes `prod/`, nothing more).

### ✅ Layer 6 — Serving — DECIDED: **direct Parquet read** into Streamlit (no Athena).
- WHY: gold tables are small, pre-aggregated, unpartitioned → Streamlit reads Parquet straight into pandas (`awswrangler`/`pyarrow`). Direct read = just an S3 GET (≈free); Athena would add a moving part + per-scan cost for no benefit at this size.
- **Flip to Athena only if:** (1) reporting tables outgrow memory (direct read loads whole file into Streamlit RAM; Athena filters/aggregates server-side, returns only the slice), (2) ad-hoc SQL slicing over large partitioned data (partition pruning), or (3) a shared SQL interface for other BI tools (QuickSight)/analysts. NOTE: cost favors direct-read — the flip driver is scale/memory + SQL push-down, NOT cost.
- One-liner: "Direct Parquet read because gold tables are small/pre-aggregated; switch to Athena only if they outgrow memory or need ad-hoc SQL push-down."

### ✅ ALL 6 ARCHITECTURE LAYERS DESIGNED. Deliverables produced:
- **Architecture diagram** — `Step3_Architecture_Diagram.svg` (viewable) + `Step3_Architecture_Diagram.drawio` (editable in diagrams.net / importable to Miro). End-to-end, all 6 layers + orchestration & security bands.
- **Solution design doc** — `Step3_Solution_Design.docx` (validated). TOC, embedded diagram, WHY-tables per layer, CLV appendix, open questions, SME sign-off block.
- ⬜ **Written SME approval** — HUMAN GATE, still pending. Do NOT build (Step 4) until the SME signs Section 12 of the doc.

### 👉 NEXT SESSION STARTS HERE
1. Get SME to review `Step3_Solution_Design.docx` + diagram → capture written approval (and answers to the 3 open questions).
2. On approval → begin **Step 4: build the pipeline** (Glue ingest + PySpark transforms on AWS), following the layers above.
3. Then Step 5 (metrics), Step 6 (Streamlit), Step 7 (repo + CI/CD + docs + demo).
- **Layer 4 — Orchestration/scheduling + failure/reload** (Step Functions / Glue Workflows / MWAA / EventBridge; retries, idempotency, reprocess-from-raw).
- **Layer 5 — Security** (encryption at rest = KMS/SSE; in transit = TLS).
- **Layer 6 — Serving** (curated S3 → Streamlit; via Athena or direct Parquet read).
- Then: **architecture diagram** (draw.io), **solution design doc** (WHY per AWS choice), **written SME approval**. Do NOT build (Step 4) until SME clears.

## Open questions to raise with SME (from Steps 1–2)
1. **Discounts:** spec says detect via `OPTION_PRICE < 0` but data has **0 negatives** — how are discounts encoded (or are there none)? Blocks discount metric.
2. `OPTION_QUANTITY` always 1 (zero variance) — expected?
3. Confirm drop/handling policy for 28 orphan options + 1 corrupt line item.

## Remaining roadmap
- Step 4: Build the pipeline (PySpark ingest + transform on AWS).
- Step 5: Metrics — CLV (daily, High/Med/Low tags: top20/mid60/bottom20), RFM, churn, sales trends, loyalty, locations, pricing/discounts.
- Step 6: Streamlit dashboard (6 views).
- Step 7: GitHub repo + CI/CD (GitHub Actions), docs w/ "WHY per tech stack," demo video.
