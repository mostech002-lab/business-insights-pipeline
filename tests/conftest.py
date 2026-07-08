"""
Test bootstrap.

1. Put the flat Glue modules (glue/) on sys.path so tests can `import lib_ingest`
   / `import lib_transform` exactly the way Glue does at runtime
   (--extra-py-files uploads them flat).
2. If real pyspark is available (e.g. the `mlstack` venv), use it — the
   transform tests need a live SparkSession to exercise joins/coalesce/to_date.
   If it is NOT installed (bare CI), stub pyspark so the PURE logic tests
   (build_ingest_query, etc.) can still import and run. Spark-dependent tests
   skip themselves via the `spark` fixture.
3. boto3 / botocore are always stubbed — no test needs real AWS.
"""

import sys
import types
from pathlib import Path

import pytest

# ── 1. make glue/ importable ────────────────────────────────────────────────
GLUE_DIR = Path(__file__).resolve().parent.parent / "glue"
sys.path.insert(0, str(GLUE_DIR))

# ── 2. use real pyspark if present, else stub it ────────────────────────────
try:
    import pyspark  # noqa: F401

    HAS_PYSPARK = True
except ModuleNotFoundError:
    HAS_PYSPARK = False

    _fake_F = types.ModuleType("pyspark.sql.functions")
    _fake_sql = types.ModuleType("pyspark.sql")
    _fake_sql.functions = _fake_F
    _fake_pyspark = types.ModuleType("pyspark")
    _fake_pyspark.sql = _fake_sql
    sys.modules.setdefault("pyspark", _fake_pyspark)
    sys.modules.setdefault("pyspark.sql", _fake_sql)
    sys.modules.setdefault("pyspark.sql.functions", _fake_F)

# ── 3. always stub boto3 / botocore (no test needs real AWS) ─────────────────
_fake_boto3 = types.ModuleType("boto3")
_fake_boto3.resource = lambda *a, **k: None
_fake_boto3.client = lambda *a, **k: None
sys.modules.setdefault("boto3", _fake_boto3)


class _ClientError(Exception):
    ...


_fake_bce = types.ModuleType("botocore.exceptions")
_fake_bce.ClientError = _ClientError
_fake_botocore = types.ModuleType("botocore")
_fake_botocore.exceptions = _fake_bce
sys.modules.setdefault("botocore", _fake_botocore)
sys.modules.setdefault("botocore.exceptions", _fake_bce)


# ── 4. session-scoped SparkSession for transform tests ──────────────────────
@pytest.fixture(scope="session")
def spark():
    """
    One local SparkSession reused across the whole test session (JVM startup is
    slow). Tests that request this fixture are automatically skipped when
    pyspark is not installed.
    """
    if not HAS_PYSPARK:
        pytest.skip("pyspark not installed — skipping Spark-dependent tests")

    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("transform-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
