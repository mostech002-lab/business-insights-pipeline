# Step 1 — Source Data Explanation & Data-Quality Notes

**Project:** Business Insights Assessment (GlobalPartners)
**Author:** Mohammad Saim
**Date:** 2026-07-03
**Tooling:** PySpark (`spark.read.csv`), analysis in `EDA.ipynb`

## 1. Source Files

Three source files were downloaded from Google Drive and loaded into Spark DataFrames.

| File | Grain (one row = ) | Expected rows | Loaded rows | Match |
|---|---|---|---|---|
| `order_items` | one line item within an order | 203,519 | 203,519 | ✅ |
| `order_item_options` | one add-on / modifier on a line item | 193,017 | 193,017 | ✅ |
| `date_dim` | one calendar date (2023) | 365 | 365 | ✅ |

## 2. Load Configuration & Why It Matters

Files are read with `header=true`, `multiLine=true`, and `inferSchema=true`.

- **`multiLine=true` was required.** A naive line-by-line read returned **203,531** rows for `order_items` (12 too many) and produced a "staircase" of nulls in the trailing columns. Root cause: several text fields (e.g. `ITEM_NAME` such as `"1/4 Smoked Chicken Plate,\nWhite Meat"`) contain embedded newlines, which split one logical record into multiple physical rows. Enabling `multiLine` corrected the count to the expected 203,519 and eliminated the false nulls.
- **`inferSchema` caveat.** Inference is convenient but mistyped `PRINTED_CARD_NUMBER` as `LongType`. Identifiers must be treated as **strings** (to preserve leading zeros and avoid 64-bit overflow). For the production pipeline (Step 4) an **explicit `StructType` schema** is recommended over `inferSchema` — it is faster (no extra scan), deterministic, and gives full control over ID vs numeric typing.

## 3. Table-by-Table Findings

### 3.1 order_items
- **Grain / key:** one row per **line item**. `ORDER_ID` repeats (an order has multiple items); the unique key is **`LINEITEM_ID`**. `ORDER_ID` + `LINEITEM_ID` is the join to `order_item_options`.
- **1 corrupt record** — a single row has null `LINEITEM_ID` / `ITEM_CATEGORY` / `ITEM_NAME`. **Action:** drop it (logged, not silently deleted). Negligible impact on a 203K-row table.
- **`USER_ID` null on 17,808 rows** — legitimate data (guest checkouts), **not** a parsing error. **Action:** keep, but flag: these orders cannot be attributed to a customer and are therefore excluded from customer-level metrics (CLV, RFM). Revenue-level metrics can still include them.
- **`PRINTED_CARD_NUMBER` null on 157,435 rows** — expected. Aligns 1:1 with `IS_LOYALTY = false`; non-members have no loyalty card. **Not** a quality issue. Cast to **string**.
- **`CREATION_TIME_UTC`** correctly inferred as timestamp; will be used to derive a date key for the `date_dim` join.
- `ORDER_ID`, `IS_LOYALTY`, `CURRENCY` (all USD), `ITEM_PRICE`, `ITEM_QUANTITY` — 0 nulls, clean.

### 3.2 order_item_options
- **Clean table** — 0 nulls across all columns, no fully duplicated rows.
- Joins to `order_items` on `ORDER_ID` + `LINEITEM_ID`.
- **`OPTION_QUANTITY` is always 1** (single distinct value across all 193,017 rows) — a zero-variance column carrying no information; note for downstream use.
- **Discount discrepancy (open item for Step 5):** Step 5 specifies detecting discounts via `OPTION_PRICE < 0`, but the data contains **0 negative values** (and 127,980 zeros). Discounts are either encoded differently here or absent. Flagged for SME / Step 5.

### 3.3 date_dim
- **Clean** — the only nulls are in `holiday_name` (353), which is expected (null on the 12 non-holiday... i.e. non-holiday days).
- **`date_key` needs standardizing** — format is inconsistent across copies (`dd-mm-yyyy` vs `m/d/yy`) and remained `StringType` under inference. **Action:** parse to a proper `DateType` with an explicit format string before joins.
- **`week 52` on Jan 1, 2023 is correct, not a bug** — Jan 1, 2023 was a Sunday, which belongs to ISO week 52 of the prior year. Keep as-is (confirm ISO-week convention downstream).

## 4. Referential Integrity (follow-up)
Join keys are confirmed (`ORDER_ID`/`LINEITEM_ID` between the two fact tables; derived date → `date_key`). A full orphan check (options with no matching line item, and vice-versa) should be run once all data is loaded to SQL Server in Step 3/4.

## 5. Actions Summary
1. Read with `multiLine=true`; use an explicit schema in the pipeline (IDs as strings).
2. Drop the 1 corrupt `order_items` row (logged).
3. Keep null-`user_id` orders; exclude from customer-level metrics only.
4. Standardize `date_key` to `DateType`.
5. Carry forward two open questions to Step 5/SME: the discount encoding and the zero-variance `option_quantity`.
