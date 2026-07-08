"""
Unit tests for the pure transform logic in lib_transform.

These exercise REAL Spark behavior (joins, coalesce, to_date) on tiny in-memory
DataFrames, so they need a live SparkSession — supplied by the `spark` fixture
in conftest.py. When pyspark isn't installed, that fixture skips these tests.

Run (inside the mlstack venv):   pytest tests/test_transform.py -v
"""

import lib_transform as T
import datetime

def test_line_revenue_handles_item_with_no_options(spark):
    # ── Arrange: two line items — L1 has options, L2 has none ───────────────
    items = spark.createDataFrame(
        [
            ("O1", "L1", 10.00, 2),   # will match an options rollup
            ("O1", "L2",  5.00, 1),   # NO matching options  <-- the key case
        ],
        ["ORDER_ID", "LINEITEM_ID", "ITEM_PRICE", "ITEM_QUANTITY"],
    )
    # options rollup only covers L1
    options_agg = spark.createDataFrame(
        [
            ("O1", "L1", 3.00, 2),
        ],
        ["ORDER_ID", "LINEITEM_ID", "TOTAL_OPTIONS_PRICE", "NUM_OPTIONS"],
    )

    # ── Act: run the function under test ────────────────────────────────────
    out = T.compute_line_revenue(items, options_agg, ["ORDER_ID", "LINEITEM_ID"])

    # ── Assert: key results by line item so row order doesn't matter ────────
    rev = {r["LINEITEM_ID"]: float(r["LINE_REVENUE"]) for r in out.collect()}

    # L1: 10*2 + 3(options) == 23
    assert rev["L1"] == 23.0
    # L2: 5*1 + 0 == 5  — coalesce turned the LEFT-join NULL into 0, not NULL
    assert rev["L2"] == 5.0



def test_preaggregate_options(spark):
    df = spark.createDataFrame(
    [
        ('O1','L1',40,2),
        ('O1','L1',10,5),
    ],
    ['ORDER_ID','LINEITEM_ID','OPTION_PRICE','OPTION_QUANTITY']
    )

    out = T.preaggregate_options(df)

    final_option_price = {r['ORDER_ID']:float(r['TOTAL_OPTIONS_PRICE']) for r in out.collect()}
    final_num_options = {r['ORDER_ID']:float(r['NUM_OPTIONS']) for r in out.collect()}

    assert final_option_price['O1'] == 130
    assert final_num_options['O1'] == 2



def test_parse_date_dim(spark):

        df = spark.createDataFrame(
            [("T1","30-01-2022"),
             ("T2","4/30/26"),

            ],

            ['USER_ID',"date_key"]

        )

        out = T.parse_date_dim(df)

        case = {c['USER_ID']:c["DATE"] for c in out.collect()}

        assert case['T1'] == datetime.date(2022,1,30)
        assert case['T2'] == datetime.date(2026,4,30)


def test_write_to_prod_is_idempotent(spark, tmp_path):
    """
    Re-running the same load_date must not duplicate data. write_to_prod uses
    mode('overwrite') + partitionOverwriteMode=dynamic, so writing the same
    partition twice REPLACES it. Proof: write a 1-row frame twice, read back 1.
    A second, different partition written in between must survive untouched —
    that's what makes the overwrite dynamic (per-partition) rather than global.
    """
    day_a = spark.createDataFrame(
        [("O1", "L1", 2022, 1, 30, 10.0)],
        ["ORDER_ID", "LINEITEM_ID", "year", "month", "day", "LINE_REVENUE"],
    )
    day_b = spark.createDataFrame(
        [("O2", "L9", 2022, 2, 15, 99.0)],
        ["ORDER_ID", "LINEITEM_ID", "year", "month", "day", "LINE_REVENUE"],
    )
    prod = str(tmp_path)

    # write day A, then a different partition day B, then day A AGAIN (the re-run)
    T.write_to_prod(day_a, prod, "fact")
    T.write_to_prod(day_b, prod, "fact")
    T.write_to_prod(day_a, prod, "fact")

    out = spark.read.parquet(f"{prod}/fact")
    rows = out.collect()

    # idempotent: day A re-run did NOT duplicate -> still exactly 1 row for it
    a_rows = [r for r in rows if r["year"] == 2022 and r["month"] == 1]
    assert len(a_rows) == 1
    # dynamic overwrite: day B's separate partition was left intact
    b_rows = [r for r in rows if r["year"] == 2022 and r["month"] == 2]
    assert len(b_rows) == 1
    # total is 2, not 3 — the re-run replaced rather than appended
    assert len(rows) == 2

