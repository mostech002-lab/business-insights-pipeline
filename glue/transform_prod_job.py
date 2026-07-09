"""
transform_prod_job.py — Glue entrypoint for Layer 2 (silver) transform.

Reads the bronze zone (raw/) for a given load_date, applies explicit schemas,
enforces data quality (corrupt + orphan quarantine -> rejects/), rolls options
up to line-item grain, computes LINE_REVENUE, and lands the enriched fact in
the silver zone (prod/), partitioned by event date (year/month/day).

Job parameters (--arg):
  --JOB_NAME         (Glue-provided)
  --load_date        raw partition to process, e.g. 2026-07-06
  --raw_base_path    s3://<bucket>/raw
  --prod_base_path   s3://<bucket>/prod
  --rejects_base_path s3://<bucket>/rejects
  --region           (optional) AWS region, default us-east-1

Deploy notes:
  - --extra-py-files s3://.../lib_transform.py
  - Glue security configuration -> KMS for S3/CloudWatch.
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

from lib_transform import (
    order_items_schema,
    order_item_options_schema,
    date_dim_schema,
    quarantine_corrupt_order_items,
    quarantine_orphan_options,
    preaggregate_options,
    compute_line_revenue,
    derive_order_date,
    parse_date_dim,
    write_to_prod,
)

# Join key for order_items <-> order_item_options
OPTIONS_JOIN_KEYS = ["ORDER_ID", "LINEITEM_ID"]
# A line item is unusable if its primary key is null
ITEMS_REQUIRED_COLS = ["LINEITEM_ID"]


def read_raw(spark, schema, raw_base, table_name, load_date):
    """
    Read one raw table for a single load_date, applying the explicit schema.
    load_date is a partition column of raw/, so it's re-attached by partition
    discovery and used to scope the read to just this batch.
    """
    path = f"{raw_base}/{table_name}"
    df = spark.read.schema(schema).parquet(path).filter(F.col("load_date") == load_date)
    return df


def main():
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "load_date",
            "raw_base_path",
            "prod_base_path",
            "rejects_base_path",
            "qurantine_base_path",
        ],
    )
    job_name = args["JOB_NAME"]
    load_date = args["load_date"]
    raw_base = args["raw_base_path"]
    prod_base = args["prod_base_path"]
    rejects_base = args["rejects_base_path"]
    qurantine_base = args["qurantine_base_path"]

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(job_name, args)

    # ── Pipeline ──────────────────────────────────────────────────────────
    # 1. read raw order_items  (order_items_schema)
    items_raw = read_raw(spark, order_items_schema, raw_base, "order_items", load_date)

    # 2. read raw options      (order_item_options_schema)
    options_raw = read_raw(
        spark, order_item_options_schema, raw_base, "order_item_options", load_date
    )

    # 3. quarantine_corrupt_order_items  -> valid_items
    valid_items, corrupt_items = quarantine_corrupt_order_items(
        items_raw, ITEMS_REQUIRED_COLS, rejects_base
    )
    # valid_items is consumed 3x downstream (orphan anti/semi joins + revenue join);
    # cache once so its lineage (raw read + corrupt filter) isn't recomputed per action.
    valid_items = valid_items.cache()

    # 4. quarantine_orphan_options       -> valid_options
    valid_options, orphan_options = quarantine_orphan_options(
        options_raw, valid_items, OPTIONS_JOIN_KEYS, qurantine_base, load_date
    )

    # 5. preaggregate_options            -> options_agg
    options_agg = preaggregate_options(valid_options)

    # 6. compute_line_revenue            -> enriched
    enriched = compute_line_revenue(valid_items, options_agg, OPTIONS_JOIN_KEYS)

    # 7. derive_order_date               -> enriched (+ year/month/day)
    enriched = derive_order_date(enriched)

    # 8. write_to_prod                   -> prod/order_items_enriched
    write_to_prod(enriched, prod_base, "order_items_enriched")

    # 9. date_dim silver pass  (static conformed dim; join happens in gold)
    date_dim_raw = read_raw(spark, date_dim_schema, raw_base, "date_dim", load_date)
    date_dim = parse_date_dim(date_dim_raw)
    bad_dates = date_dim.filter(F.col("DATE").isNull()).count()
    if bad_dates:
        raise ValueError(f"parse_date_dim: {bad_dates} date_key rows failed to parse")
    write_to_prod(date_dim, prod_base, "date_dim", partition_cols=None)

    job.commit()


if __name__ == "__main__":
    main()
