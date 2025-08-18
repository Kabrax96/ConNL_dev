# app/etl_central/test/test_transform_cp.py

import pandas as pd
from app.etl_central.assets.balance_presupuestario_cp import transform_cp_data

def _sample_cp_raw():
    """
    Build a minimal CP-like DataFrame resembling the 'F4 BAP' sheet shape:
      - text in column index 1
      - amounts in columns 2/3/4
    Includes:
      • an aggregate row (A.) to be filtered out
      • a duplicate code (A1.) to test de-duplication
      • a valid code (B1.)
      • a non-code row to be filtered out
    """
    data = [
        [None, "A. Ingresos Totales (A=A1+A2+A3)", "100", "90", "95"],    # aggregate -> exclude
        [None, "A1. Ingresos de Libre Disposición", "1,000", "(50)", "900"],  # keep
        [None, "A1. Ingresos de Libre Disposición", "999999", "999999", "999999"],  # duplicate A1 -> dedupe
        [None, "B1. Transferencias Federales", None, "40", "40"],          # keep
        [None, "X. Not a code", "1", "2", "3"],                             # non-code -> exclude
    ]
    # Column 1 is the descriptive text; 2/3/4 are numeric strings
    return pd.DataFrame(data)

def test_transform_sets_period_fields():
    df_raw = _sample_cp_raw()
    out = transform_cp_data(df_raw, year=2020)
    assert not out.empty, "Transform produced an empty DataFrame."
    assert set(["surrogate_key","concept","sublabel","year_quarter","full_date","type","amount"]).issubset(out.columns)
    assert (out["year_quarter"] == "2020_CP").all()
    assert (out["full_date"] == "2020-12-31").all()

def test_transform_filters_and_unpivots():
    df_raw = _sample_cp_raw()
    out = transform_cp_data(df_raw, 2020)
    # Only A1 and B1 should remain; duplicate A1 must be collapsed to one code
    assert set(out["concept"]) == {"A1", "B1"}
    # Each code expands to 3 rows (estimated_or_approved, devengado, recaudado_pagado)
    assert len(out) == 2 * 3

def test_amount_cleaning_and_negatives():
    df_raw = _sample_cp_raw()
    out = transform_cp_data(df_raw, 2020)

    a1 = out[out["concept"] == "A1"]
    # "1,000" -> 1000.0
    est = a1[a1["type"] == "estimated_or_approved"]["amount"].iloc[0]
    assert est == 1000.0
    # "(50)" -> -50.0
    dev = a1[a1["type"] == "devengado"]["amount"].iloc[0]
    assert dev == -50.0
    # B1 has a None in one column -> cleaned to None
    b1 = out[(out["concept"] == "B1") & (out["type"] == "estimated_or_approved")]
    assert b1["amount"].iloc[0] is None

# Optional: lets you run directly with `python -m app.etl_central.test.test_transform_cp`
if __name__ == "__main__":
    import pytest, sys
    sys.exit(pytest.main([__file__]))
