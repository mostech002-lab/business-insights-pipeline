# Step 2 — Initial Data Analysis: Schema, Relationships & Insight Map

**Project:** Business Insights Assessment (GlobalPartners)
**Author:** Mohammad Saim
**Date:** 2026-07-03

## 1. Table Grains

| Table | Grain (one row = ) | Unique key |
|---|---|---|
| `order_items` | one **line item** within an order | `LINEITEM_ID` |
| `order_item_options` | one **option / modifier** on a line item | (`ORDER_ID`, `LINEITEM_ID`, option) — many per line item |
| `date_dim` | one **calendar date** (2023) | `date_key` |

## 2. Relationships (ER Sketch)

```
                        ┌─────────────────────┐
                        │      date_dim       │   (dimension)
                        │  date_key (PK)      │
                        └──────────▲──────────┘
                                   │  many-to-one
              order date (to_date(CREATION_TIME_UTC)) = date_key
                                   │
                        ┌──────────┴──────────┐
                        │     order_items     │   (fact — line-item grain)
                        │  LINEITEM_ID (PK)   │
                        │  ORDER_ID, USER_ID  │
                        └──────────▲──────────┘
                                   │  one-to-many
                     ORDER_ID + LINEITEM_ID
                                   │
                        ┌──────────┴──────────┐
                        │ order_item_options  │   (fact — option grain)
                        └─────────────────────┘
```

| Relationship | Keys | Cardinality | Notes |
|---|---|---|---|
| `order_items` → `order_item_options` | `ORDER_ID` + `LINEITEM_ID` | **one-to-many** | 102,712 line items have options (max 152 on one item); 100,822 have none; 28 orphan options flagged |
| `order_items` → `date_dim` | `to_date(CREATION_TIME_UTC)` = `date_key` | **many-to-one** | requires deriving a date column and standardizing `date_key` to `DateType` |

## 3. Revenue Grain & Roll-up (the core calculation)

Revenue lives in two tables at different grains, so the **many side must be pre-aggregated before joining** to avoid fan-out double-counting.

1. **Collapse options to line-item grain:**
   `TOTAL_OPTIONS_PRICE = Σ(OPTION_PRICE × OPTION_QUANTITY)` grouped by `(ORDER_ID, LINEITEM_ID)`.
2. **Left join** back to `order_items` (keeps line items with no options).
3. **Per line:** `LINE_REVENUE = ITEM_PRICE × ITEM_QUANTITY + coalesce(TOTAL_OPTIONS_PRICE, 0)`.

**Roll-up hierarchy:** option → line item → order (`ORDER_ID`) → customer-day (`USER_ID`, date) → lifetime.
The **CLV grain is `(USER_ID, date)`**, accumulated over time.

> `coalesce(..., 0)` is valid *only* on `TOTAL_OPTIONS_PRICE` (null there = "no add-ons" = $0). It must **not** be applied to `OPTION_PRICE`/`OPTION_QUANTITY`, where a null would signal corruption, not zero.

## 4. Column → Insight Map

| Metric | Key columns | Source |
|---|---|---|
| **CLV** (daily) | `USER_ID`, `LINE_REVENUE`, order date | order_items + options |
| **RFM segmentation** | `USER_ID`, order date (Recency), distinct `ORDER_ID` count (Frequency), `LINE_REVENUE` (Monetary) | order_items + options |
| **Churn indicators** | `USER_ID`, order date → days since last order, avg gap between orders, spend trend | order_items |
| **Sales trends & seasonality** | `LINE_REVENUE`, `date_key`/`month`/`week`/`day_of_week`/`is_holiday`, `RESTAURANT_ID`, `ITEM_CATEGORY` | all three |
| **Loyalty impact** | `IS_LOYALTY`, `USER_ID`, `LINE_REVENUE`, `ORDER_ID` | order_items + options |
| **Location performance** | `RESTAURANT_ID` (= store/location), `LINE_REVENUE`, `ORDER_ID`, order date; `APP_NAME` for channel | order_items + options |
| **Pricing / discount** | `ITEM_PRICE`, `OPTION_PRICE` (pricing OK); discount detection **blocked** | order_item_options |

**Common slicing dimensions** available on every metric: `IS_LOYALTY`, `RESTAURANT_ID`, `APP_NAME`, and any `date_dim` attribute.

## 5. Open Questions for SME
1. **Discount encoding.** Spec says detect discounts via `OPTION_PRICE < 0`, but the data has **0 negatives**. Are discounts encoded elsewhere, or absent? Blocks the discount-effectiveness metric.
2. **`OPTION_QUANTITY` is always 1** (zero variance) — confirm expected.
3. **28 orphan options** and **1 corrupt line item** — confirm drop/handling policy.
