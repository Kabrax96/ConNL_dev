import os, pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, text
from app.etl_central.assets.balance_presupuestario_cp import single_upsert, bulk_overwrite

def _mk_test_table(metadata):
    return Table(
        "nuevo_leon_balance_presupuestario_cp_test",
        metadata,
        Column("surrogate_key", String, primary_key=True),
        Column("concept", String),
        Column("sublabel", String),
        Column("year_quarter", String),
        Column("full_date", String),
        Column("type", String),
        Column("amount", Float),
    )

def _engine():
    dsn = f"postgresql+psycopg2://{os.getenv('DB_USERNAME')}:{os.getenv('DB_PASSWORD')}@{os.getenv('SERVER_NAME')}:{os.getenv('PORT')}/{os.getenv('DATABASE_NAME')}"
    return create_engine(dsn)

def test_bulk_and_single_upsert_cycle():
    eng = _engine()
    md = MetaData(bind=eng)
    tbl = _mk_test_table(md)
    md.create_all(eng)

    # 1) bulk_overwrite with two rows
    df1 = pd.DataFrame([
        {"surrogate_key":"k1","concept":"A1","sublabel":"x","year_quarter":"2020_CP","full_date":"2020-12-31","type":"devengado","amount":10.0},
        {"surrogate_key":"k2","concept":"B1","sublabel":"y","year_quarter":"2020_CP","full_date":"2020-12-31","type":"devengado","amount":20.0},
    ])
    class DummyPg: engine = eng
    bulk_overwrite(df1, DummyPg(), tbl, md)

    with eng.begin() as con:
        cnt = con.execute(text("select count(*) from nuevo_leon_balance_presupuestario_cp_test")).scalar_one()
    assert cnt == 2

    # 2) single_upsert updates one row and inserts a new one
    df2 = pd.DataFrame([
        {"surrogate_key":"k2","concept":"B1","sublabel":"y","year_quarter":"2020_CP","full_date":"2020-12-31","type":"devengado","amount":25.0},
        {"surrogate_key":"k3","concept":"C1","sublabel":"z","year_quarter":"2020_CP","full_date":"2020-12-31","type":"devengado","amount":30.0},
    ])
    # Fake client holder for engine (matches your functions' expectations)
    class DummyPg2: engine = eng
    single_upsert(df2, DummyPg2(), tbl, md)

    with eng.begin() as con:
        rows = con.execute(text("select surrogate_key, amount from nuevo_leon_balance_presupuestario_cp_test order by surrogate_key")).fetchall()
    # Expect k1(10), k2(updated 25), k3(30) => 3 rows total
    assert len(rows) == 3
    amounts = {r[0]: r[1] for r in rows}
    assert amounts["k2"] == 25.0
