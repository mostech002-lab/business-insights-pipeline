"""
lib_transform.py — Reusable helpers for the Layer-2 transform Glue job.

Pure, side-effect-light logic for the silver stage (raw/ -> prod/):
explicit schemas, corrupt/orphan quarantine, options pre-aggregation,
LINE_REVENUE roll-up, and ORDER_DATE derivation. Split from the Glue
entrypoint (transform_prod_job.py) so this logic stays unit-testable
without a live SparkContext or AWS.

Business Insights Assessment · Step 4 · Layer 2 (S3 raw/ -> S3 prod/)
"""

from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DecimalType,
    TimestampType,
)

# ─────────────────────────────────────────────────────────────────────────────
# Explicit schemas (deterministic — replaces inferSchema; IDs stay strings)
# ─────────────────────────────────────────────────────────────────────────────

order_items_schema = StructType([
    StructField("APP_NAME",StringType()),
    StructField("RESTAURANT_ID",StringType()),
    StructField("CREATION_TIME_UTC",TimestampType()),
    StructField("ORDER_ID",StringType()),
    StructField("USER_ID",StringType()),
    StructField("PRINTED_CARD_NUMBER",StringType()),
    StructField("IS_LOYALTY",StringType()),
    StructField("CURRENCY",StringType()),
    StructField("LINEITEM_ID",StringType()),
    StructField("ITEM_CATEGORY",StringType()),
    StructField("ITEM_NAME",StringType()),
    StructField("ITEM_PRICE",DecimalType(10,2)),
    StructField("ITEM_QUANTITY",IntegerType())
    
])


order_item_options_schema = StructType([
    StructField("ORDER_ID",StringType()),
    StructField("LINEITEM_ID",StringType()),
    StructField("OPTION_GROUP_NAME",StringType()),
    StructField("OPTION_NAME",StringType()),
    StructField("OPTION_PRICE",DecimalType(10,2)),
    StructField("OPTION_QUANTITY",IntegerType())
    
])


date_dim_schema = StructType([
    StructField("date_key",StringType()),
    StructField("year",IntegerType()),
    StructField("month",IntegerType()),
    StructField("week",IntegerType()),
    StructField("day_of_week",StringType()),
    StructField("is_holiday",StringType()),
    StructField("is_weekend",StringType()),
    StructField("holiday_name",StringType()),
    
])


# ─────────────────────────────────────────────────────────────────────────────
# Join helpers
# ─────────────────────────────────────────────────────────────────────────────

def build_join_condition(left_alias: str, right_alias: str, join_keys: list[str]):
    return reduce(
        lambda a, b: a & b,
        [
            F.col(f"{left_alias}.{key}") == F.col(f"{right_alias}.{key}")
            for key in join_keys
        ]
    )


# ─────────────────────────────────────────────────────────────────────────────
# Quarantine (data-quality enforcement -> rejects/)
# ─────────────────────────────────────────────────────────────────────────────

def quarantine_corrupt_order_items(df,required_columns,qurantine_path):

    df.sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    condition = reduce(lambda x, y: x | y,
                       [F.col(c).isNull() for c in required_columns]
                       )
    corrupt_df = df.filter(condition)

    reject_reason = F.concat_ws(", ",*[F.when(F.col(c).isNull(),F.lit(f"{c} is NULL"))
                                       for c in required_columns]
                                )
    corrupt_df = corrupt_df.withColumn("REJECT_REASON",reject_reason)

    valid_order_df = df.filter(~condition)



    corrupt_df.write \
        .mode("overwrite") \
        .partitionBy("load_date") \
        .format("parquet") \
        .save(qurantine_path)
         

    return valid_order_df,corrupt_df

def quarantine_orphan_options(
    options_df: DataFrame,
    valid_orders_df: DataFrame,
    join_keys: list[str],
    quarantine_path: str,
    load_date: str
) -> tuple[DataFrame, DataFrame]:
    """
    Finds option rows that do not have a matching valid order.

    Returns:
        valid_options_df: options with matching orders
        orphan_options_df: options without matching orders
    """
    options_df.sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    orders = valid_orders_df.alias("orders")
    options = options_df.alias("options")

    join_condition = build_join_condition(
        left_alias="options",
        right_alias="orders",
        join_keys=join_keys
    )

    orphan_options_df = (
        options
        .join(orders, on=join_condition, how="left_anti")
        .withColumn("reject_reason", F.lit("ORPHAN_OPTION_NO_MATCHING_ORDER"))
        .withColumn("load_date", F.lit(load_date))
    )

    valid_options_df = (
        options
        .join(orders, on=join_condition, how="left_semi")
    )

    (
        orphan_options_df
        .write
        .mode("overwrite")
        .partitionBy("load_date")
        .format("parquet")
        .save(quarantine_path)
    )

    return valid_options_df, orphan_options_df


# ─────────────────────────────────────────────────────────────────────────────
# Pre-aggregation + revenue roll-up  (stage 4 & 5 — coming next)
# ─────────────────────────────────────────────────────────────────────────────

def preaggregate_options(valid_options_df):
    


    options_grouped = valid_options_df.groupBy("ORDER_ID","LINEITEM_ID") \
                                      .agg(
                                            (F.sum(F.col("OPTION_PRICE") * F.col("OPTION_QUANTITY"))
                                            .alias("TOTAL_OPTIONS_PRICE"))
                                            ,F.count("*").alias("NUM_OPTIONS")
                                            )
    return options_grouped


def compute_line_revenue(valid_items_df, options_agg_df, join_keys):
    joined_df = valid_items_df.join(options_agg_df, on=join_keys, how="left")

    joined_df = joined_df.withColumn(
        "LINE_REVENUE",
        F.col("ITEM_PRICE") * F.col("ITEM_QUANTITY")
        + F.coalesce(F.col("TOTAL_OPTIONS_PRICE"), F.lit(0)),
    )
    return joined_df

def derive_order_date(df):
    df = (
        df
        .withColumn("ORDER_DATE", F.to_date("CREATION_TIME_UTC"))
        .withColumn("year", F.year("ORDER_DATE"))
        .withColumn("month", F.month("ORDER_DATE"))
        .withColumn("day", F.dayofmonth("ORDER_DATE"))
    )
    return df


def parse_date_dim(df):
    """
    Standardize the inconsistent `date_key` string into a real DateType `DATE`.

    Step 1 found two formats in this column across source copies
    (`dd-MM-yyyy` and `M/d/yy`), so we try each in order and take the first
    that parses. `date_dim` is a small controlled calendar dimension, so
    unparseable rows are not expected — surface them loudly rather than
    silently drop a day (see the fail-fast guard in the job).
    """
    df = df.withColumn(
        "DATE",
        F.coalesce(
            F.to_date(F.col("date_key"), "dd-MM-yyyy"),
            F.to_date(F.col("date_key"), "M/d/yy"),
        ),
    )
    return df


def write_to_prod(df, prod_path, table_name, partition_cols=("year", "month", "day")):
    """
    Write a silver table to prod/. Partitioned by year/month/day by default
    (dynamic overwrite -> only touched partitions are replaced, so re-runs are
    idempotent). Pass partition_cols=None for small unpartitioned dims like
    date_dim, where mode("overwrite") does a clean full-refresh replace.
    """
    df.sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    target = f"{prod_path}/{table_name}"
    writer = df.write.mode("overwrite").format("parquet")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(target)
    return target