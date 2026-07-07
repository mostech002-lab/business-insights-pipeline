"""
ingest_raw_job.py — Glue entrypoint for Layer 1 ingestion.

Pulls incremental batches from SQL Server (RDS) and lands them verbatim in the
S3 bronze zone (raw/), partitioned by ingestion date (load_date). Incremental
state is tracked with an explicit watermark in a DynamoDB control table.

Watermark discipline:
  - order_items         : incremental on CREATION_TIME_UTC.
  - order_item_options  : piggybacks on the SAME order_items watermark (by-parent).
  - date_dim            : full reload (bounded static dimension).
  Read one pre-run watermark; advance it ONCE at the end from the batch max.

Job parameters (--arg):
  --JOB_NAME      (Glue-provided)
  --load_date     ingestion/business run date, e.g. 2026-07-06
  --secret_name   Secrets Manager secret id holding DB creds JSON
  --raw_base_path s3://<bucket>/raw
  --region        (optional) AWS region, default us-east-1

Deploy notes:
  - --extra-py-files s3://.../lib_ingest.py
  - --extra-jars     s3://.../mssql-jdbc-<ver>.jar   (SQL Server JDBC driver)
  - Glue security configuration -> KMS for S3/CloudWatch/bookmarks.
"""

import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F

from lib_ingest import (
    WATERMARK_FMT,
    build_jdbc,
    get_db_credentials,
    get_watermark,
    put_watermark,
    read_new_rows,
    write_to_raw,
)


def main():
    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "load_date", "secret_name", "raw_base_path"],
    )
    job_name = args["JOB_NAME"]
    load_date = args["load_date"]
    raw_base = args["raw_base_path"]
    region = args.get("region", "us-east-1")

    # JOB_RUN_ID is exposed in the environment for observability stamping.
    run_id = _safe_run_id()

    sc = SparkContext()
    glue_context = GlueContext(sc)
    spark = glue_context.spark_session
    job = Job(glue_context)
    job.init(job_name, args)

    # ── Credentials + TLS JDBC ────────────────────────────────────────────
    creds = get_db_credentials(args["secret_name"], region=region)
    jdbc_url, connection_props = build_jdbc(creds)

    # ── One pre-run watermark drives both order_items and options ─────────
    wm = get_watermark("order_items")
    print(f"[ingest] load_date={load_date} watermark={wm}")

    # ── order_items (drives the watermark) ────────────────────────────────
    oi = read_new_rows(spark, "order_items", wm, jdbc_url, connection_props).cache()
    n_items = oi.count()
    write_to_raw(oi, "order_items", load_date, raw_base)
    print(f"[ingest] order_items new rows: {n_items}")

    # ── order_item_options (same watermark, by-parent) ────────────────────
    opt = read_new_rows(spark, "order_item_options", wm, jdbc_url, connection_props)
    n_opts = opt.count()
    write_to_raw(opt, "order_item_options", load_date, raw_base)
    print(f"[ingest] order_item_options new rows: {n_opts}")

    # ── date_dim (full reload; watermark ignored) ─────────────────────────
    dd = read_new_rows(spark, "date_dim", wm, jdbc_url, connection_props)
    n_dd = dd.count()
    write_to_raw(dd, "date_dim", load_date, raw_base)
    print(f"[ingest] date_dim rows (full reload): {n_dd}")

    # ── Advance watermark ONCE, from the actual batch max ─────────────────
    if n_items > 0:
        new_wm_ts = oi.agg(F.max("CREATION_TIME_UTC")).collect()[0][0]
        new_wm = new_wm_ts.strftime(WATERMARK_FMT)
        advanced = put_watermark("order_items", new_wm, run_id)
        print(f"[ingest] watermark -> {new_wm} (advanced={advanced})")
    else:
        print("[ingest] no new order_items; watermark unchanged")

    oi.unpersist()
    job.commit()


def _safe_run_id() -> str:
    """Best-effort Glue run id for observability; falls back to 'manual'."""
    import os
    return os.environ.get("JOB_RUN_ID") or os.environ.get("GLUE_RUN_ID") or "manual"


if __name__ == "__main__":
    main()
