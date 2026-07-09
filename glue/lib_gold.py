"""
lib_gold.py — Reusable helpers for the Layer-3 (gold) reporting job.

Pure, side-effect-light logic for the reporting stage (prod/ -> reporting/):
the daily CLV model (DAILY_SPEND -> RUNNING_LTV -> CLV_TAG) and the separate
RFM segmentation table. Split from the Glue entrypoint so this logic stays
unit-testable without a live SparkContext or AWS.

Business Insights Assessment · Step 4 · Layer 3 (S3 prod/ -> S3 reporting/)
"""

from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ─────────────────────────────────────────────────────────────────────────────
# CLV — stage 1 & 2: daily spend, then cumulative running lifetime value
# ─────────────────────────────────────────────────────────────────────────────

def compute_daily_spend_and_ltv(enriched_df):
    """
    Grain: (USER_ID, SNAPSHOT_DATE).

    DAILY_SPEND  = sum(LINE_REVENUE) per customer per transaction date.
    RUNNING_LTV  = cumulative DAILY_SPEND over time per customer (monotonic).

    Guest checkouts (USER_ID IS NULL) are dropped — they can't be attributed
    to a customer, so they're excluded from all customer-level metrics.
    """
    daily = (
        enriched_df
        .filter(F.col("USER_ID").isNotNull())
        .withColumnRenamed("ORDER_DATE", "SNAPSHOT_DATE")
        .groupBy("USER_ID", "SNAPSHOT_DATE")
        .agg(F.sum("LINE_REVENUE").alias("DAILY_SPEND"))
    )

    # Ordered window -> default frame is UNBOUNDED PRECEDING .. CURRENT ROW,
    # i.e. a running total of everything up to and including this date.
    running = Window.partitionBy("USER_ID").orderBy("SNAPSHOT_DATE")
    return daily.withColumn("RUNNING_LTV", F.sum("DAILY_SPEND").over(running))


# ─────────────────────────────────────────────────────────────────────────────
# CLV — stage 3: rank the full customer base into spend quintiles
# ─────────────────────────────────────────────────────────────────────────────

def rank_customers(clv_df):
    """
    One row per USER_ID with a spend QUINTILE (1..5).

    LATEST_LTV = max(RUNNING_LTV) per customer. RUNNING_LTV is monotonic, so its
    max == its latest value == the customer's lifetime total to date.

    QUINTILE   = ntile(5) over a NO-partition window ordered by LATEST_LTV asc.
    No partitionBy => ranks the entire customer base against each other (a
    population percentile, not share-of-total). asc => quintile 5 = top spenders.
    Maps onto the 20/60/20 tag scheme: 1 = bottom 20%, 2-4 = middle 60%,
    5 = top 20%.
    """
    latest = clv_df.groupBy("USER_ID").agg(F.max("RUNNING_LTV").alias("LATEST_LTV"))
    window = Window.orderBy(F.col("LATEST_LTV").asc())
    return latest.withColumn("QUINTILE", F.ntile(5).over(window))


# ─────────────────────────────────────────────────────────────────────────────
# CLV — moves 3 & 4: quintile -> High/Med/Low tag, denormalized onto daily grain
# ─────────────────────────────────────────────────────────────────────────────

def assign_clv_tags(daily_df, ranked_df):
    """
    Tag each customer High/Med/Low from their spend quintile, then broadcast the
    tag onto every daily row (denormalize for the flat serving table).

    QUINTILE -> CLV_TAG: 5 = High (top 20%), 1 = Low (bottom 20%), 2-4 = Med.

    LEFT join on USER_ID keeps every daily row. inner would be safe here (ranked
    set covers the daily set) but left encodes the "keep all daily rows" intent.

    Returns exactly: USER_ID / SNAPSHOT_DATE / DAILY_SPEND / RUNNING_LTV / CLV_TAG.
    """
    is_high = F.col("QUINTILE") == 5
    is_low = F.col("QUINTILE") == 1

    ranked_df = ranked_df.withColumn(
        "CLV_TAG",
        F.when(is_high, "High").when(is_low, "Low").otherwise("Med"),
    )

    return (
        daily_df.join(ranked_df, on="USER_ID", how="left")
        .drop(ranked_df["QUINTILE"])
        .drop(ranked_df["LATEST_LTV"])
    )


# ─────────────────────────────────────────────────────────────────────────────
# RFM — separate table, grain USER_ID, as-of a reference date
# ─────────────────────────────────────────────────────────────────────────────

def compute_rfm(enriched_df, reference_date):
    """
    One row per customer with Recency / Frequency / Monetary.

    RECENCY   = days since last order = datediff(reference_date, last ORDER_DATE).
                Smaller = more recently active.
    FREQUENCY = count of distinct ORDER_ID.
    MONETARY  = sum of LINE_REVENUE.

    reference_date: a 'yyyy-MM-dd' string (or date) — the as-of point to measure
    recency from (typically the reporting run date). F.lit + datediff cast it
    against the DateType ORDER_DATE.

    Guest checkouts (USER_ID IS NULL) are dropped, same as CLV.

    Returns: USER_ID / FREQUENCY / MONETARY / RECENCY.
    """
    return (
        enriched_df
        .filter(F.col("USER_ID").isNotNull())
        .groupBy("USER_ID")
        .agg(
            F.max("ORDER_DATE").alias("MAX_ORDER_DATE"),
            F.countDistinct("ORDER_ID").alias("FREQUENCY"),
            F.sum("LINE_REVENUE").alias("MONETARY"),
        )
        .withColumn("RECENCY", F.datediff(F.lit(reference_date), "MAX_ORDER_DATE"))
        .drop("MAX_ORDER_DATE")
    )
