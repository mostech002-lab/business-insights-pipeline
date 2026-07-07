# Business Insights Assessment — Plan (Fri eve → Mon night)

**Goal:** Ship all 7 steps by Monday night. The extra day is spent de-risking the two things most likely to blow up: AWS setup and the SME sign-off. Architecture + approval move to Saturday so the pipeline has all of Sunday.

**Scope-cutting rule:** If you fall behind, protect the *end-to-end story* (data → pipeline → CLV/RFM → dashboard → repo) over polishing any single metric. A working thin slice beats a half-built perfect one.

---

## Friday night (~3 hrs) — Data + Understanding

| # | Task | Output |
|---|------|--------|
| 1 | Download `order_items`, `order_item_options`, `date_dim` from the Drive links | 3 raw CSVs saved locally |
| 2 | Integrity check: row counts (203,519 / 193,017 / dates), nulls, dupes, bad types | Short data-quality notes |
| 3 | Map the schema: join keys (`order_id`, `lineitem_id`, `user_id`, `date_key`), grain of each table | 1-page relationship sketch |
| 4 | Load into **SQL Server** (source-of-truth per requirement — watch the setup video) | Tables queryable in SSMS |

**Done when:** you can query all three tables in SQL Server and explain how they join.

---

## Saturday (~6 hrs) — Architecture + Sign-off (Step 3)

| # | Task | Output |
|---|------|--------|
| 5 | Draw AWS architecture (draw.io): SQL Server → ingestion → S3 (raw/curated) → Glue/EMR PySpark → S3 → Streamlit. Include scheduling, encryption, failure-reload | Diagram PNG |
| 6 | Write the **Solution Design Document** — include the "WHY" for each AWS choice | `solution_design.md`/PDF |
| 7 | Send diagram + doc for **SME approval** (required deliverable — send by Saturday evening so it's back before you build) | Approval request out |
| 8 | While waiting: stand up the AWS basics (S3 buckets, IAM, Glue/EMR access) or the local PySpark env | Environment ready |

**Done when:** design is submitted for approval and your build environment runs.

---

## Sunday (~8 hrs) — Pipeline + Metrics (Steps 4 + 5)

| # | Task | Output |
|---|------|--------|
| 9 | PySpark ingestion + transforms; build the daily **CLV** model (this is the *primary* goal) | ETL scripts |
| 10 | Secondary metrics: RFM segments, churn indicators, sales trends, loyalty vs non, location ranking, discount effectiveness | PySpark jobs + curated tables |
| 11 | Persist curated outputs (S3 / local parquet) that the dashboard will read | Curated dataset |

**Done when:** CLV updates per customer per day and all six metric tables exist.

> ⚠️ **Biggest risk = AWS.** If cloud setup stalls, run the exact same PySpark locally against parquet and keep the AWS diagram/doc as the "designed" architecture. Don't let infra block the metrics + dashboard.

---

## Monday (~8 hrs) — Dashboard + Ship

**Morning — Streamlit (Step 6): six views**
| # | Dashboard |
|---|-----------|
| 12 | Customer Segmentation (RFM + loyalty) |
| 13 | Churn Risk indicators |
| 14 | Sales Trends & Seasonality |
| 15 | Loyalty Program Impact |
| 16 | Location Performance |
| 17 | Pricing & Discount Effectiveness |

**Afternoon — Submission (Step 7)**
| # | Task |
|---|------|
| 18 | GitHub repo: ETL scripts, PySpark/SQL, configs, dashboard, README |
| 19 | **CI/CD** with GitHub Actions (lint + run) |
| 20 | Documentation cleanup + "WHY behind each tech stack" section |
| 21 | Record short **demo video / presentation** |
| 22 | Final review: naming, deliverables checklist, submit |

**Done when:** repo is public/shared, CI is green, video recorded, all deliverables linked.

---

## Deliverables checklist
- [ ] Architecture diagram + solution design doc
- [ ] Written SME approval
- [ ] Working pipeline (ingest + transform)
- [ ] CLV (daily) + RFM + churn + trends + loyalty + locations + discounts
- [ ] Streamlit dashboard (6 views)
- [ ] GitHub repo + CI/CD
- [ ] Demo video / presentation
- [ ] "WHY" per tech-stack choice

---

## How we'll work together
Per your setup: I'll **guide, not hand you answers**. For coding I'll walk you through it step by step rather than dropping full solutions, and only give a direct answer if you're stuck after three tries. Tell me which task number you want to start, and we'll go.
