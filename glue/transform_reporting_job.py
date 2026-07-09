"""
transform_reporting_job.py — Glue entrypoint for Layer 3 (gold) reporting.

Reads the whole silver fact (prod/order_items_enriched), builds the two
reporting tables, and lands them unpartitioned in the gold zone (reporting/):

  - clv : daily grain (USER_ID, SNAPSHOT_DATE) with DAILY_SPEND, RUNNING_LTV
          and a denormalized CLV_TAG (High/Med/Low).
  - rfm : one row per customer with RECENCY / FREQUENCY / MONETARY as of
          --reference_date.

Idempotency: reporting/ is unpartitioned and FULLY recomputed off silver every
run (overwrite-by-nature), so there's no load_date to scope — read the entire
silver table each time.

Job parameters (--arg):
  --JOB_NAME             (Glue-provided)
  --prod_base_path       s3://<bucket>/prod
  --reporting_base_path  s3://<bucket>/reporting
  --reference_date       as-of date for RFM recency, e.g. 2026-07-06

Deploy notes:
  - --extra-py-files s3://.../lib_gold.py,s3://.../lib_transform.py
  - Glue security configuration -> KMS for S3/CloudWatch.
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

from lib_gold import (
    compute_daily_spend_and_ltv,
    rank_customers,
    assign_clv_tags,
    compute_rfm,
)
from lib_transform import write_to_prod


def read_from_prod(spark, prod_base, table_name):
    """
    Read a full silver table (all partitions) from the prod/ zone.

    Parquet is self-describing — types were baked in during the silver pass —
    so no explicit schema is applied here. Gold recomputes off the entire fact,
    so there is no load_date filter.
    """
    path = f"{prod_base}/{table_name}"
    return spark.read.parquet(path)


def main():
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "prod_base_path",
            "reporting_base_path",
            "reference_date",
        ],
    )
    job_name = args["JOB_NAME"]
    prod_base = args["prod_base_path"]
    reporting_base = args["reporting_base_path"]
    reference_date = args["reference_date"]

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(job_name, args)

    # ── Pipeline ──────────────────────────────────────────────────────────
    # 1. read the whole silver fact
    enriched_df = read_from_prod(spark, prod_base, "order_items_enriched")

    # 2. CLV chain: daily spend + running LTV -> full-base ranking -> tag join
    daily_spend_ltv = compute_daily_spend_and_ltv(enriched_df)
    ranked_cust_df = rank_customers(daily_spend_ltv)
    clv = assign_clv_tags(daily_spend_ltv, ranked_cust_df)

    # 3. RFM: one row per customer as of reference_date
    rfm = compute_rfm(enriched_df, reference_date)

    # 4. land both gold tables, unpartitioned (small pre-aggregated outputs)
    write_to_prod(clv, reporting_base, "clv", partition_cols=None)
    write_to_prod(rfm, reporting_base, "rfm", partition_cols=None)

    job.commit()


if __name__ == "__main__":
    main()
