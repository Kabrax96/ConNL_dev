# app/etl_central/test/test_transform_ingresos_cp.py
import pandas as pd
from app.etl_central.assets.ingresos_detallado_cp import transform_ingresos_detallado_cp_data

def _sample_cp_ingresos_df():
    rows = []

    # header-ish lines 0..6
    for i in range(7):
        rows.append([None, "", "", "", "", "", "", ""])
    # row 7.. : section I starts
    # Put a date in row 3 across cols 1..7 like "al 31 de diciembre de 2024"
    rows[3] = [None, "al", "31", "de", "diciembre", "de", "2024", ""]

    # Section I rows
    rows += [
        [None, "A. Ingresos Tributarios", "1,000", "", "1,000", "900", "850", "150"],  # primary key A.
        [None, "a1) ISR", "(50)", "", "(50)", "−25", "−30", "5"],                        # sec key a1), unicode minus
        [None, "", "", "", "", "", "", ""],                                             # blank line inside I ok
        [None, "II. Ingresos no tributarios", "", "", "", "", "", ""],                  # II header (ends I)
        [None, "b2) Derechos", "200", "10", "210", "150", "140", "70"],                 # Section II content
        [None, "", "", "", "", "", "", ""],                                             # blank line ends II
    ]
    return pd.DataFrame(rows)

def test_transform_sets_cp_period_and_normalizes():
    df_raw = _sample_cp_ingresos_df()
    out = transform_ingresos_detallado_cp_data(df_raw, year=2024)
    assert not out.empty
    # fecha parsed or fallback to year end
    assert (out["cuarto"] == "CP").all()
    assert "2024" in out["fecha"].iloc[0]  # parsed '2024-12-31' or date from header

    # normalization checks
    a = out[out["concepto"].str.contains("Tributarios", na=False)].iloc[0]
    assert a["estimado"] == 1000.0
    a1 = out[out["concepto"].str.contains("ISR", na=False)].iloc[0]
    assert a1["estimado"] == -50.0           # (50) -> -50.0
    assert a1["devengado"] == -25.0          # unicode minus

def test_transform_keys_and_sections():
    df_raw = _sample_cp_ingresos_df()
    out = transform_ingresos_detallado_cp_data(df_raw, year=2024)
    # keys present
    assert {"clave_primaria","clave_secundaria"}.issubset(out.columns)
    # section tags present
    assert set(out["seccion"].unique()) <= {"I","II"}
    # drop of empty lines worked
    empties = out[(out["concepto"] == "") & out[["estimado","ampliaciones_reducciones","modificado","devengado","recaudado","diferencia"]].isna().all(axis=1)]
    assert len(empties) == 0
