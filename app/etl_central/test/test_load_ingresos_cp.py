# app/etl_central/test/test_load_ingresos_cp.py
import os
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, text

from app.etl_central.assets.ingresos_detallado_cp import single_load, bulk_load

def _engine():
    dsn = (
        f"postgresql+psycopg2://"
        f"{os.getenv('DB_USERNAME')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('SERVER_NAME')}:{os.getenv('PORT')}/{os.getenv('DATABASE_NAME')}"
    )
    return create_engine(dsn)

def _mk_test_table(metadata: MetaData) -> Table:
    return Table(
        "nuevo_leon_ingresos_detallado_cp_test",
        metadata,
        Column("surrogate_key", String, primary_key=True),
        Column("concepto", String),
        Column("estimado", Float),
        Column("ampliaciones_reducciones", Float),
        Column("modificado", Float),
        Column("devengado", Float),
        Column("recaudado", Float),
        Column("diferencia", Float),
        Column("clave_primaria", String),
        Column("clave_secundaria", String),
        Column("fecha", String),
        Column("cuarto", String),
        Column("seccion", String),
    )

def test_bulk_then_single_upsert_cycle():
    eng = _engine()
    md = MetaData()
    tbl = _mk_test_table(md)
    md.create_all(eng)

    # 1) bulk overwrite
    df1 = pd.DataFrame([
        {"surrogate_key":"k1","concepto":"A. Tributarios","estimado":1000.0,"ampliaciones_reducciones":0.0,"modificado":1000.0,"devengado":900.0,"recaudado":850.0,"diferencia":150.0,"clave_primaria":"A.","clave_secundaria":None,"fecha":"2024-12-31","cuarto":"CP","seccion":"I"},
        {"surrogate_key":"k2","concepto":"b2) Derechos","estimado":200.0,"ampliaciones_reducciones":10.0,"modificado":210.0,"devengado":150.0,"recaudado":140.0,"diferencia":60.0,"clave_primaria":None,"clave_secundaria":"b2)","fecha":"2024-12-31","cuarto":"CP","seccion":"II"},
    ])
    class _Pg: engine = eng
    bulk_load(df1, _Pg(), tbl, md)

    with eng.begin() as con:
        cnt = con.execute(text("select count(*) from nuevo_leon_ingresos_detallado_cp_test")).scalar_one()
    assert cnt == 2

    # 2) single upsert: update k2, insert k3
    df2 = pd.DataFrame([
        {"surrogate_key":"k2","concepto":"b2) Derechos","estimado":200.0,"ampliaciones_reducciones":10.0,"modificado":210.0,"devengado":151.0,"recaudado":141.0,"diferencia":59.0,"clave_primaria":None,"clave_secundaria":"b2)","fecha":"2024-12-31","cuarto":"CP","seccion":"II"},
        {"surrogate_key":"k3","concepto":"a1) ISR","estimado":-50.0,"ampliaciones_reducciones":0.0,"modificado":-50.0,"devengado":-25.0,"recaudado":-30.0,"diferencia":5.0,"clave_primaria":None,"clave_secundaria":"a1)","fecha":"2024-12-31","cuarto":"CP","seccion":"I"},
    ])
    class _Pg2: engine = eng
    single_load(df2, _Pg2(), tbl, md)

    with eng.begin() as con:
        rows = con.execute(text("select surrogate_key, concepto, devengado from nuevo_leon_ingresos_detallado_cp_test order by surrogate_key")).fetchall()

    assert len(rows) == 3
    dev = {r[0]: r[2] for r in rows}
    assert dev["k2"] == 151.0
