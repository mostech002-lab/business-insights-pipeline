"""
Unit tests for the pure query builder in lib_ingest.

No Spark, no AWS — conftest stubs those. This is the payoff of keeping
build_ingest_query side-effect-free: fast, deterministic SQL assertions.
"""

import lib_ingest as L
import pytest


def _norm(sql: str) -> str:
    """Collapse whitespace so we can assert on structure, not formatting."""
    return " ".join(sql.split())


def test_incremental_watermark_predicate():
    q = _norm(L.build_ingest_query("order_items", "2023-06-01T00:00:00"))
    assert q.startswith("( SELECT * FROM order_items")
    assert "WHERE CREATION_TIME_UTC > '2023-06-01T00:00:00'" in q
    assert q.endswith("AS SRC")


def test_incremental_by_parent_uses_exists_and_composite_keys():
    q = _norm(L.build_ingest_query("order_item_options", "2023-06-01T00:00:00"))
    assert "SELECT o.* FROM order_item_options o" in q
    assert "WHERE EXISTS" in q
    # composite join keys rendered from config, in order
    assert "i.ORDER_ID = o.ORDER_ID AND i.LINEITEM_ID = o.LINEITEM_ID" in q
    assert "i.CREATION_TIME_UTC > '2023-06-01T00:00:00'" in q
    assert q.endswith("AS SRC")


def test_full_reload_has_no_predicate():
    q = _norm(L.build_ingest_query("date_dim", "2023-06-01T00:00:00"))
    assert q == "(SELECT * FROM date_dim) AS SRC"
    assert "WHERE" not in q


def test_unknown_table_raises():
    with pytest.raises(ValueError, match="No load strategy"):
        L.build_ingest_query("not_a_table", "2023-06-01T00:00:00")


def test_every_configured_table_builds():
    # guardrail: adding a table to TABLE_CONFIG must not break query building
    for name in L.TABLE_CONFIG:
        assert L.build_ingest_query(name, "2023-01-01T00:00:00").endswith("AS SRC")
