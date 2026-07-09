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

## ✅ Step 3 — Architecture + Solution Design + SME Sign-off (DONE — approval assumed granted)
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
- ✅ **Written SME approval** — assumed granted (proceeding to build).

---

## 🔄 Step 4 — Build the Pipeline (IN PROGRESS)

Clean git repo lives in subfolder `business-insights-pipeline/` (structure, README, .gitignore, requirements, .env.example, tests). **Pushed to GitHub:** https://github.com/mostech002-lab/business-insights-pipeline (public, branch `main`). `.env` + large CSVs gitignored; 200-row sample committed.

### ✅ Layer 1 — Ingestion (RDS → S3 `raw/`) — DONE
`glue/lib_ingest.py` + `glue/ingest_raw_job.py`:
- DynamoDB watermark control table: `get_watermark` / `put_watermark` (atomic conditional write; no-`Z` ISO so lexicographic == chronological).
- Secrets Manager creds + TLS JDBC (`encrypt=true`).
- Config-driven `build_ingest_query` (`TABLE_CONFIG`): order_items = incremental watermark; order_item_options = incremental-by-parent `EXISTS` semi-join (shares order_items' watermark); date_dim = full reload.
- `read_new_rows` (JDBC pushdown), `write_to_raw` (Parquet, partitioned by `load_date`, dynamic partition overwrite).
- `main()`: one pre-run watermark → items → options → date_dim → advance watermark once from batch max. Empty-batch guard + `.cache()`.
- 5 pure-logic pytest tests passing (conftest stubs pyspark/boto3).

### ✅ Layer 2 — Silver transform (`raw/` → `prod/`) LOGIC — DONE
`glue/lib_transform.py` (compiles clean, 7 functions):
- **3 explicit `StructType` schemas** (IDs as strings, `CREATION_TIME_UTC` timestamp, `ITEM_PRICE`/`OPTION_PRICE` = `DecimalType(10,2)`, `IS_LOYALTY`/`is_holiday`/`is_weekend` kept as **string** → cast later, `date_key` kept as **string** → parsed later because formats are inconsistent). Read boundary is **permissive** (all nullable=True); not-null enforced by quarantine, NOT by schema flags (Spark doesn't enforce `nullable=False` on read).
- `build_join_condition` — reusable aliased equi-join builder (for when key names differ / non-equi; NOT needed for same-name key joins → use `on=join_keys` there).
- `quarantine_corrupt_order_items(df, required_columns, path)` — splits rows where any required col is null → writes to `rejects/` with a per-row `REJECT_REASON` (concat_ws of which cols were null), partitioned by `load_date`, dynamic overwrite. Returns (valid, corrupt).
- `quarantine_orphan_options(options, valid_orders, join_keys, path, load_date)` — `left_anti` = orphans (the 28), `left_semi` = keepers. Only options→items orphans quarantined; items with no options are LEGIT (handled by left join + coalesce, not rejected).
- `preaggregate_options(valid_options)` — `groupBy(ORDER_ID, LINEITEM_ID)` → `TOTAL_OPTIONS_PRICE = Σ(OPTION_PRICE×OPTION_QUANTITY)` + `NUM_OPTIONS`. NO coalesce here (null OPTION_PRICE = corruption, not $0).
- `compute_line_revenue(valid_items, options_agg, join_keys)` — **LEFT** join (`on=join_keys` clean form), `LINE_REVENUE = ITEM_PRICE×ITEM_QUANTITY + coalesce(TOTAL_OPTIONS_PRICE, 0)` (coalesce belongs HERE — no-option line = $0 add-ons).
- `derive_order_date(df)` — `ORDER_DATE = to_date(CREATION_TIME_UTC)` + `year`/`month`/`day` int columns.
- `write_to_prod(df, prod_path, table_name)` — dynamic partition overwrite, **partitioned by `year`/`month`/`day`** (switched from flat `ORDER_DATE` → nested, coarse→fine, for multi-year scale + easy retention). Returns target path.

### ✅ Layer 2 entrypoint — `glue/transform_prod_job.py` — WIRED & COMPLETE
Both TODOs done + `date_dim` folded in:
1. `read_raw(spark, schema, raw_base, table_name, load_date)` — `spark.read.schema(schema).parquet(f"{raw_base}/{table_name}").filter(F.col("load_date")==load_date)`. Table-root path so partition discovery re-attaches `load_date` even though it's not in the explicit schema. Added `from pyspark.sql import functions as F` import.
2. 8-step `main()` pipeline wired: read items+options → quarantine corrupt → **`valid_items = valid_items.cache()`** (consumed 3× downstream) → quarantine orphans → preaggregate → compute revenue → derive date → `write_to_prod` as `order_items_enriched`.
3. **`date_dim` silver pass (step 9)** folded into `main()`: `read_raw(date_dim_schema)` → `parse_date_dim` → **fail-fast guard** (`.count()` of null `DATE` → raise) → `write_to_prod(..., partition_cols=None)`.
- New job arg added: `qurantine_base_path` (orphan options write here; corrupt items → `rejects_base_path`). **Orchestration/Step Functions must now pass this 6th param.** (`qurantine` misspelled consistently in both `main()` and `lib_transform` — cosmetic, fix later.)

### ✅ `lib_transform.py` additions
- `parse_date_dim(df)` — inconsistent `date_key` string → `DATE` (DateType) via `F.coalesce(to_date(dd-MM-yyyy), to_date(M/d/yy))` (two formats found in Step 1). Unparseable → null → caught by job guard.
- `write_to_prod(df, prod_path, table_name, partition_cols=("year","month","day"))` — now takes optional `partition_cols`; pass `None` for small unpartitioned dims (date_dim full-refresh).

### ✅ Tests — `tests/test_transform.py` (real Spark, all passing)
- `conftest.py` reworked: **conditional** pyspark stub (stub only if real pyspark absent; else use it — e.g. user's `mlstack` venv), added **session-scoped `spark` fixture**. boto3 always stubbed.
- 4 passing tests: `compute_line_revenue` no-options coalesce → non-null revenue; `preaggregate_options` sum/count; `parse_date_dim` both date formats → `datetime.date`; **idempotency** (`write_to_prod` written twice + a 2nd partition → 2 rows not 3, dynamic per-partition overwrite proven).
- **Run tests in `mlstack` venv:** `pytest tests/test_transform.py -v`. (This sandbox has no pyspark — do NOT `pip install` it; user's venv `mlstack` has it.)

### ✅ Layer 3 — Gold (`prod/` → `reporting/`) — COMPLETE
`glue/lib_gold.py` — 4 functions, all logic done & syntax-clean:
- `compute_daily_spend_and_ltv(enriched_df)` — drop null `USER_ID` → groupBy `(USER_ID, SNAPSHOT_DATE)` sum `LINE_REVENUE` as `DAILY_SPEND` → cumulative `RUNNING_LTV` via `Window.partitionBy("USER_ID").orderBy("SNAPSHOT_DATE")` (default frame = running total).
- `rank_customers(clv_df)` — `LATEST_LTV = max(RUNNING_LTV)` per `USER_ID` (monotonic → max = latest); `QUINTILE = ntile(5)` over **no-partition** `Window.orderBy(LATEST_LTV.asc())` (ranks whole base; quintile 5 = top spenders).
- `assign_clv_tags(daily_df, ranked_df)` — QUINTILE→CLV_TAG (5 High / 1 Low / else Med) on `ranked_df`, then **LEFT** join onto daily on `USER_ID`, drop `QUINTILE`+`LATEST_LTV`. Final: `USER_ID/SNAPSHOT_DATE/DAILY_SPEND/RUNNING_LTV/CLV_TAG`. Tag denormalized onto every daily row → flat single-scan serving table (no query-time join). Tag = as-of latest run; whole history carries current tag (intentional; migration across runs).
- `compute_rfm(enriched_df, reference_date)` — grain USER_ID: one agg → `MAX_ORDER_DATE`+`FREQUENCY`(countDistinct ORDER_ID)+`MONETARY`(sum LINE_REVENUE); then `RECENCY = datediff(lit(reference_date), MAX_ORDER_DATE)`; drop scratch col. `reference_date` = 'yyyy-MM-dd' str or date.

**CLV_TAG design (kept for interview):** each RUN = ONE full-base ranking as-of reporting date; migration Low→Med→High across runs, NOT per-historical-date. Tag by population percentile (top20/mid60/bottom20) via ntile(5) — NOT share-of-total.

**Tests — `tests/test_gold.py` (real Spark, 4 tests, syntax-clean):** daily/LTV (null drop + running sum 220→270→395), rank_customers (ntile buckets), assign_clv_tags (5/1/mid → High/Low/Med, tag per daily row), compute_rfm (freq=2, monetary=180, recency=6). **Run in `mlstack`:** `pytest tests/test_gold.py -v`. (Sandbox has no pyspark — py_compile only here.)

**Gold entrypoint — `glue/transform_reporting_job.py` COMPLETE:** args `JOB_NAME/prod_base_path/reporting_base_path/reference_date` (NO load_date — reporting fully recomputed off silver each run). `read_from_prod` = plain `spark.read.parquet` (no schema/filter; parquet self-describing, read whole table). Wiring: read `order_items_enriched` → CLV chain (daily→rank→tag) + RFM → `write_to_prod(..., partition_cols=None)` for `clv` and `rfm` to `reporting/`. Reuses `write_to_prod` from lib_transform.

## ✅ Step 5 — Metrics — MOSTLY DONE

**Metric-home decision (interview point):** make it a gold table only if the agg is heavy over the full fact AND reused across views; trivial slices of already-small tables stay Streamlit-side.

| Metric | Home | Status |
|---|---|---|
| CLV | gold `clv` | ✅ |
| RFM | gold `rfm` | ✅ |
| Sales trends | gold `daily_sales` | ✅ |
| Locations | gold `location_perf` | ✅ |
| Churn | flag on `rfm` (`IS_CHURNED`) | ✅ |
| Loyalty | Streamlit-side (Step 6) | ➖ deferred |
| Pricing/discounts | blocked — SME open Q | ⛔ |

New `lib_gold.py` functions (syntax-clean):
- `flag_churn(rfm_df, churn_threshold_days=90)` — `IS_CHURNED = RECENCY > threshold`; rides on rfm, no extra fact pass. 90d = stated assumption.
- `compute_daily_sales(enriched_df)` — grain ORDER_DATE: `TOTAL_REVENUE`/`NUM_ORDERS`(distinct)/`NUM_LINE_ITEMS`/`AOV`. **Guests KEPT** (real revenue — only customer-level metrics drop null USER_ID).
- `compute_location_performance(enriched_df)` — grain RESTAURANT_ID: `TOTAL_REVENUE`/`NUM_ORDERS`/`AOV`. Guests kept.

`transform_reporting_job.py` now writes 4 gold tables: `clv`, `rfm` (with churn flag), `daily_sales`, `location_perf`. `tests/test_gold.py` = 7 tests (added flag_churn, daily_sales, location_perf). All py_compile-clean; run `pytest tests/ -v` in `mlstack` to confirm on real Spark.

### 👉 NEXT SESSION STARTS HERE (Step 6)
1. **Run `pytest tests/ -v` in `mlstack`** (only py_compile done in sandbox this session).
2. **Pricing/discounts** — still blocked on SME: `OPTION_PRICE` has 0 negatives, how are discounts encoded? Get an answer or document the assumption.
3. **Step 6 — Streamlit** dashboard (6 views) reading `reporting/` Parquet direct into pandas. Loyalty = live groupby on `IS_LOYALTY` here. Views ≈ CLV segments, RFM, sales trend, locations, churn, loyalty.
4. **Step 7 — CI/CD** (GitHub Actions running pytest) + docs "WHY per stack" + demo video.
5. **Repo hygiene:** revoke the throwaway PAT (still in `.git/config` remote URL) + `git remote set-url` to clean HTTPS.

**Working style reminder:** guide, don't hand answers; walk code step-by-step; give the answer only after 3 wrong tries; track with the task list. Reconnect the folder at the start of each chat (`Business Insights Assessment` → `business-insights-pipeline/`).

**Repo hygiene note:** commits `1f0c43b` (Layer 1) + `ea14168` (Layer 2 logic + scaffold) on GitHub `main`. **Uncommitted this session:** `transform_prod_job.py` (wired), `lib_transform.py` (parse_date_dim + write_to_prod arg), `lib_gold.py` (new), `tests/conftest.py` + `tests/test_transform.py`, this PROGRESS update, plus the earlier `f1ac03f` if not yet pushed. **Commit + push before/after next session.** GitHub auth = HTTPS + PAT for `mostech002-lab`; throwaway PAT should be revoked if not already.

## Open questions to raise with SME (from Steps 1–2)
1. **Discounts:** spec says detect via `OPTION_PRICE < 0` but data has **0 negatives** — how are discounts encoded (or are there none)? Blocks discount metric.
2. `OPTION_QUANTITY` always 1 (zero variance) — expected?
3. Confirm drop/handling policy for 28 orphan options + 1 corrupt line item.

## Remaining roadmap
- Step 4: Build the pipeline (PySpark ingest + transform on AWS).
- Step 5: Metrics — CLV (daily, High/Med/Low tags: top20/mid60/bottom20), RFM, churn, sales trends, loyalty, locations, pricing/discounts.
- Step 6: Streamlit dashboard (6 views).
- Step 7: GitHub repo + CI/CD (GitHub Actions), docs w/ "WHY per tech stack," demo video.
