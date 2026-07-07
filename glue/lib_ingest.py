"""
lib_ingest.py — Reusable helpers for the Layer-1 ingestion Glue job.

Deliberately split from the Glue entrypoint (ingest_raw_job.py) so the pure,
side-effect-free logic here — especially build_ingest_query — is unit-testable
without a SparkContext, a network, or AWS.

Business Insights Assessment · Step 4 · Layer 1 (RDS SQL Server -> S3 raw/)
"""

import json

import boto3
from botocore.exceptions import ClientError
from pyspark.sql import functions as F

# ─────────────────────────────────────────────────────────────────────────────
# Constants
# ─────────────────────────────────────────────────────────────────────────────

WATERMARK_TABLE = "watermarks"
# No trailing 'Z': must match the T-SQL DATETIME2 literal format used in the
# pushdown predicate AND stay lexicographically ordered == chronologically
# ordered (fixed-width ISO-8601).
FLOOR_WATERMARK = "1900-01-01T00:00:00"
WATERMARK_FMT = "%Y-%m-%dT%H:%M:%S"

# Per-table load strategy. The dict is the single source of truth AND an
# allow-list: read_new_rows only ever builds SQL for a name that appears here,
# so interpolating table_name into the query has no injection surface.
TABLE_CONFIG = {
    "order_items": {
        "strategy": "incremental_watermark",
        "watermark_col": "CREATION_TIME_UTC",
    },
    "order_item_options": {
        # No timestamp of its own — filter by the parent line item's timestamp.
        "strategy": "incremental_by_parent",
        "parent_table": "order_items",
        "parent_watermark_col": "CREATION_TIME_UTC",
        "join_keys": ["ORDER_ID", "LINEITEM_ID"],
    },
    "date_dim": {
        # Bounded static dimension (365 rows) — full reload each run.
        "strategy": "full_reload",
    },
}

_dynamodb = boto3.resource("dynamodb")


# ─────────────────────────────────────────────────────────────────────────────
# Watermark control table (DynamoDB)
# ─────────────────────────────────────────────────────────────────────────────

def get_watermark(table_name: str) -> str:
    """Return the stored watermark for a source table, or FLOOR on first run."""
    table = _dynamodb.Table(WATERMARK_TABLE)
    try:
        item = table.get_item(Key={"table_name": table_name}).get("Item")
        if item is None:
            return FLOOR_WATERMARK
        return item.get("watermark", FLOOR_WATERMARK)
    except ClientError as e:
        raise RuntimeError(f"Failed to retrieve watermark for {table_name}") from e


def put_watermark(table_name: str, new_watermark: str, run_id: str = "manual") -> bool:
    """
    Advance the watermark only if strictly newer than what's stored (or first
    write). Guards against a stale run clobbering a newer watermark via an
    atomic conditional write. Returns True if advanced, False if skipped.
    """
    from datetime import datetime, timezone

    table = _dynamodb.Table(WATERMARK_TABLE)
    now_iso = datetime.now(timezone.utc).strftime(WATERMARK_FMT)
    try:
        table.update_item(
            Key={"table_name": table_name},
            UpdateExpression="SET #wm = :new, last_run_id = :rid, updated_at = :ts",
            ConditionExpression="attribute_not_exists(#wm) OR #wm < :new",
            ExpressionAttributeNames={"#wm": "watermark"},
            ExpressionAttributeValues={
                ":new": new_watermark,
                ":rid": run_id,
                ":ts": now_iso,
            },
        )
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise RuntimeError(f"Failed to update watermark for {table_name}") from e


# ─────────────────────────────────────────────────────────────────────────────
# Credentials + JDBC (Secrets Manager, TLS enforced)
# ─────────────────────────────────────────────────────────────────────────────

def get_db_credentials(secret_name: str, region: str = "us-east-1") -> dict:
    """Fetch DB creds JSON from Secrets Manager (never stored in the script)."""
    client = boto3.client("secretsmanager", region_name=region)
    resp = client.get_secret_value(SecretId=secret_name)
    return json.loads(resp["SecretString"])


def build_jdbc(creds: dict):
    """Build the SQL Server JDBC URL + connection props, forcing TLS."""
    url = (
        f"jdbc:sqlserver://{creds['host']}:{creds.get('port', 1433)}"
        f";databaseName={creds['dbname']}"
        ";encrypt=true;trustServerCertificate=false"  # in-transit encryption
    )
    props = {
        "user": creds["username"],
        "password": creds["password"],
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
    }
    return url, props


# ─────────────────────────────────────────────────────────────────────────────
# Query builder (pure — unit-testable) + JDBC read + bronze write
# ─────────────────────────────────────────────────────────────────────────────

def build_ingest_query(table_name: str, watermark: str) -> str:
    """
    Build the parenthesized, aliased SQL Server pushdown subquery for a table,
    based on its configured load strategy. Pure: string in, string out.
    """
    cfg = TABLE_CONFIG.get(table_name)
    if cfg is None:
        raise ValueError(f"No load strategy defined for table: {table_name}")

    strategy = cfg["strategy"]

    if strategy == "incremental_watermark":
        wm_col = cfg["watermark_col"]
        query = f"""
            SELECT *
            FROM {table_name}
            WHERE {wm_col} > '{watermark}'
        """

    elif strategy == "incremental_by_parent":
        parent = cfg["parent_table"]
        parent_wm = cfg["parent_watermark_col"]
        join_cond = " AND ".join(f"i.{k} = o.{k}" for k in cfg["join_keys"])
        query = f"""
            SELECT o.*
            FROM {table_name} o
            WHERE EXISTS (
                SELECT 1
                FROM {parent} i
                WHERE {join_cond}
                  AND i.{parent_wm} > '{watermark}'
            )
        """

    elif strategy == "full_reload":
        query = f"SELECT * FROM {table_name}"

    else:
        raise ValueError(f"Unknown load strategy '{strategy}' for table: {table_name}")

    return f"({query}) AS SRC"


def read_new_rows(spark, table_name: str, watermark: str, jdbc_url: str, connection_props: dict):
    """Read a table's incremental batch from SQL Server via JDBC pushdown."""
    query = build_ingest_query(table_name, watermark)
    return (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", query)
        .options(**connection_props)
        .load()
    )


def write_to_raw(df, table_name: str, load_date: str, raw_base_path: str) -> str:
    """
    Write an ingest batch to bronze as Parquet, partitioned by load_date.
    Dynamic partition overwrite -> re-running the same load_date replaces only
    that partition (idempotent), leaving other days intact.
    """
    df.sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    out = df.withColumn("load_date", F.lit(load_date))
    target = f"{raw_base_path}/{table_name}"
    (
        out.write
        .mode("overwrite")
        .partitionBy("load_date")
        .format("parquet")
        .save(target)
    )
    return target
