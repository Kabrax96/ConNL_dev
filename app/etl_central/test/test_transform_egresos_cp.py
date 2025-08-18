# app/etl_central/test/test_transform_egresos_cp.py
import pandas as pd
from app.etl_central.assets.egresos_detallado_cp import transform_egresos_detallado_cp_data

def _sample_cp_raw():
    """
    Minimal CP-like DataFrame for 'F6a COG' shape:
      - Column index 1 holds text (concept column)
      - Columns 2..7 hold numeric-like values
      - Row split marker: 'II. Gasto Etiquetado'
      - Section I rows begin at row index 8 (as in your transform)
    """
    rows = []

    # 0..7: filler/header-ish rows
    for i in range(8):
        rows.append([None, "", "", "", "", "", "", ""])

    # Section I (rows 8..11)
    # duplicate A1) to test de-duplication
    rows.append([None, "A1) Servicios personales", "100", "0", "100", "90", "85", "15"])   # 8
    rows.append([None, "A1) Servicios personales", "999", "999", "999", "999", "999", ""]) # 9 (dup)
    rows.append([None, "B1) Materiales", "50", "5", "55", "40", "35", "15"])               # 10
    rows.append([None, "", "", "", "", "", "", ""])                                        # 11 (blank row inside I, safe)

    # Split marker (row 12)
    rows.append([None, "II. Gasto Etiquetado", "", "", "", "", "", ""])                    # 12

    # Section II (rows 13..)
    rows.append([None, "C1) Inversion", "200", "10", "210", "150", "140", "70"])          # 13
    rows.append([None, "", "", "", "", "", "", ""])                                        # 14 (blank ends II)

    return pd.DataFrame(rows)

def test_transform_sets_fixed_period_fields():
    df_raw = _sample_cp_raw()
    out = transform_egresos_detallado_cp_data(df_raw, year=2020)
    assert not out.empty
    assert (out["Fecha"] == "2020-12-31").all()
    assert (out["Cuarto"] == "CP").all()
    assert set(["surrogate_key","Codigo","Concepto","Aprobado","Devengado","Pagado","Fecha","Cuarto","Seccion"]).issubset(out.columns)

def test_transform_splits_sections_and_dedupes_codes():
    df_raw = _sample_cp_raw()
    out = transform_egresos_detallado_cp_data(df_raw, year=2020)

    # Codes should be extracted as A1, B1, C1 (dup A1 collapsed)
    codes = set(out["Codigo"].dropna().tolist())
    assert codes == {"A1", "B1", "C1"}

    # Sections assigned properly
    by_code = {c: out[out["Codigo"] == c]["Seccion"].unique().tolist() for c in codes}
    # A1 and B1 are in Section I, C1 is in Section II
    assert "I" in by_code["A1"] and "I" in by_code["B1"]
    assert "II" in by_code["C1"]

def test_transform_preserves_amount_columns():
    df_raw = _sample_cp_raw()
    out = transform_egresos_detallado_cp_data(df_raw, year=2020)

    # We don't enforce numeric cleaning here; just ensure columns exist and are carried over
    subset = out[["Codigo","Aprobado","Modificado","Devengado","Pagado","Subejercicio"]]
    assert not subset.empty
