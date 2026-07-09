import lib_gold as T
import datetime
from pyspark.sql.window import Window


def test_compute_daily_spend_and_ltv(spark):

    df = spark.createDataFrame(
        [
            ("U1",datetime.date(2026,1,22),220),
            ("U1",datetime.date(2026,1,23),50),
            ("U1",datetime.date(2026,1,24),100),
            ("U1", datetime.date(2026, 1, 24), 25),
            (None,datetime.date(2026,1,24),100),
            
        ],

        ["USER_ID","ORDER_DATE","LINE_REVENUE"]
    )

    out = T.compute_daily_spend_and_ltv(df)

    # out.orderBy("USER_ID","SNAPSHOT_DATE").show()

    rows = {

        (r['USER_ID'],r["SNAPSHOT_DATE"]):{"DAILY_SPEND": float(r["DAILY_SPEND"]),
                                           "RUNNING_LTV": float(r['RUNNING_LTV']),
                                          }
            for r in out.collect()

        }
        
    assert len(rows) == 3

    assert rows[("U1",datetime.date(2026, 1, 24))]["DAILY_SPEND"] == 125
    
    assert rows[("U1", datetime.date(2026, 1, 22))]["RUNNING_LTV"] == 220.0
    assert rows[("U1", datetime.date(2026, 1, 23))]["RUNNING_LTV"] == 270.0
    assert rows[("U1", datetime.date(2026, 1, 24))]["RUNNING_LTV"] == 395.0






def test_rank_customers(spark):

  
    df = spark.createDataFrame(
        [
            ("U1",datetime.date(2026,1,22),220,220),
            ("U1",datetime.date(2026,1,23),50,270),
            ("U1",datetime.date(2026,1,24),125,395),
             ("U2",datetime.date(2026,1,22),100,100),
            ("U2",datetime.date(2026,1,23),200,300),
            ("U2",datetime.date(2026,1,24),300,600),
        ],

        ["USER_ID","SNAPSHOT_DATE","DAILY_SPEND","RUNNING_LTV"]
    )

    out = T.rank_customers(df)

    # out.orderBy("USER_ID").show()

    tests = {r['USER_ID'] : r["QUINTILE"]for r in out.collect()}

    assert tests['U1'] == 1
    assert tests['U2'] == 2



def test_assign_clv_tags(spark):
    daily_df = spark.createDataFrame(

        [
            ("U1", datetime.date(2026, 1, 22), 100.0, 100.0),
            ("U1", datetime.date(2026, 1, 23), 50.0, 150.0),
            ("U2", datetime.date(2026, 1, 22), 20.0, 20.0),
            ("U3", datetime.date(2026, 1, 22), 300.0, 300.0),
        ],

        ["USER_ID", "SNAPSHOT_DATE", "DAILY_SPEND", "RUNNING_LTV"]

    )
        
    ranked_df = spark.createDataFrame(

        [
            ("U1", 3, 150.0),  # Med
            ("U2", 1, 20.0),   # Low
            ("U3", 5, 300.0),  # High
        ],

        ["USER_ID", "QUINTILE", "LATEST_LTV"]

    )

    out = T.assign_clv_tags(daily_df,ranked_df)

    # out.show()

    final_dict = {(r['USER_ID'],r['SNAPSHOT_DATE']): r["CLV_TAG"]
                  for r in out.collect()}
    
    assert final_dict[("U1",datetime.date(2026,1,22))] == 'Med'
    assert final_dict[("U2",datetime.date(2026,1,22))] == 'Low'
    assert final_dict[("U3",datetime.date(2026,1,22))] == 'High'



def test_compute_rfm(spark):
    # ── Arrange ─────────────────────────────────────────────────────────────
    # U1: 2 distinct orders (O1, O2) -> FREQUENCY = 2
    #     line revenue 100 + 50 + 30 -> MONETARY = 180
    #     last order 2026-01-25; reference 2026-01-31 -> RECENCY = 6 days
    # The null-USER_ID row is a guest checkout and must be dropped.
    df = spark.createDataFrame(
        [
            ("U1", "O1", datetime.date(2026, 1, 20), 100),
            ("U1", "O1", datetime.date(2026, 1, 20),  50),  # same order, 2nd line
            ("U1", "O2", datetime.date(2026, 1, 25),  30),
            (None, "O9", datetime.date(2026, 1, 28), 999),  # guest -> dropped
        ],
        ["USER_ID", "ORDER_ID", "ORDER_DATE", "LINE_REVENUE"],
    )

    reference_date = datetime.date(2026, 1, 31)

    # ── Act ─────────────────────────────────────────────────────────────────
    out = T.compute_rfm(df, reference_date)

    # ── Assert ──────────────────────────────────────────────────────────────
    rows = {
        r["USER_ID"]: {
            "RECENCY": r["RECENCY"],
            "FREQUENCY": r["FREQUENCY"],
            "MONETARY": float(r["MONETARY"]),
        }
        for r in out.collect()
    }

    # guest checkout dropped -> only U1 survives
    assert len(rows) == 1
    assert rows["U1"]["FREQUENCY"] == 2          # distinct ORDER_ID
    assert rows["U1"]["MONETARY"] == 180.0       # 100 + 50 + 30
    assert rows["U1"]["RECENCY"] == 6            # 2026-01-31 - 2026-01-25


def test_flag_churn(spark):
    # RECENCY 10 -> active (<=90); 120 -> churned (>90)
    rfm = spark.createDataFrame(
        [
            ("U1", 10, 2, 180.0),
            ("U2", 120, 1, 50.0),
        ],
        ["USER_ID", "RECENCY", "FREQUENCY", "MONETARY"],
    )

    out = T.flag_churn(rfm, churn_threshold_days=90)

    churned = {r["USER_ID"]: r["IS_CHURNED"] for r in out.collect()}
    assert churned["U1"] is False
    assert churned["U2"] is True


def test_compute_daily_sales(spark):
    # 2026-01-22: orders O1 (2 lines: 100+50) + O2 (30) -> rev 180, orders 2,
    #             line items 3, AOV 90. Guest checkout (null USER_ID) is KEPT.
    df = spark.createDataFrame(
        [
            ("U1", "O1", datetime.date(2026, 1, 22), 100),
            ("U1", "O1", datetime.date(2026, 1, 22),  50),
            (None, "O2", datetime.date(2026, 1, 22),  30),  # guest -> still counts
            ("U2", "O3", datetime.date(2026, 1, 23),  40),
        ],
        ["USER_ID", "ORDER_ID", "ORDER_DATE", "LINE_REVENUE"],
    )

    out = T.compute_daily_sales(df)

    rows = {
        r["ORDER_DATE"]: {
            "TOTAL_REVENUE": float(r["TOTAL_REVENUE"]),
            "NUM_ORDERS": r["NUM_ORDERS"],
            "NUM_LINE_ITEMS": r["NUM_LINE_ITEMS"],
            "AOV": float(r["AOV"]),
        }
        for r in out.collect()
    }

    d22 = rows[datetime.date(2026, 1, 22)]
    assert d22["TOTAL_REVENUE"] == 180.0     # guest revenue included
    assert d22["NUM_ORDERS"] == 2
    assert d22["NUM_LINE_ITEMS"] == 3
    assert d22["AOV"] == 90.0                # 180 / 2


def test_compute_location_performance(spark):
    # R1: O1 (100+50) + O2 (30) -> rev 180, orders 2, AOV 90
    # R2: O3 (40)               -> rev 40,  orders 1, AOV 40
    df = spark.createDataFrame(
        [
            ("R1", "O1", 100),
            ("R1", "O1",  50),
            ("R1", "O2",  30),
            ("R2", "O3",  40),
        ],
        ["RESTAURANT_ID", "ORDER_ID", "LINE_REVENUE"],
    )

    out = T.compute_location_performance(df)

    rows = {
        r["RESTAURANT_ID"]: {
            "TOTAL_REVENUE": float(r["TOTAL_REVENUE"]),
            "NUM_ORDERS": r["NUM_ORDERS"],
            "AOV": float(r["AOV"]),
        }
        for r in out.collect()
    }

    assert rows["R1"]["TOTAL_REVENUE"] == 180.0
    assert rows["R1"]["NUM_ORDERS"] == 2
    assert rows["R1"]["AOV"] == 90.0
    assert rows["R2"]["TOTAL_REVENUE"] == 40.0
    assert rows["R2"]["AOV"] == 40.0