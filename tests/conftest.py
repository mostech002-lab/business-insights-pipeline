"""
Test bootstrap.

1. Put the flat Glue modules (glue/) on sys.path so tests can `import lib_ingest`
   exactly the way Glue does at runtime (--extra-py-files uploads them flat).
2. Stub `pyspark` and `boto3` so the PURE logic (build_ingest_query, etc.) can be
   imported and tested without a SparkContext, network, or AWS credentials.
"""

import sys
import types
from pathlib import Path

# ── 1. make glue/ importable ────────────────────────────────────────────────
GLUE_DIR = Path(__file__).resolve().parent.parent / "glue"
sys.path.insert(0, str(GLUE_DIR))

# ── 2. stub heavy/external deps so import lib_ingest succeeds ────────────────
_fake_F = types.ModuleType("pyspark.sql.functions")
_fake_sql = types.ModuleType("pyspark.sql")
_fake_sql.functions = _fake_F
_fake_pyspark = types.ModuleType("pyspark")
_fake_pyspark.sql = _fake_sql
sys.modules.setdefault("pyspark", _fake_pyspark)
sys.modules.setdefault("pyspark.sql", _fake_sql)
sys.modules.setdefault("pyspark.sql.functions", _fake_F)

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
