# app/etl_central/test/test_s3_ingresos_cp.py
import os
import pytest
import pandas as pd
from app.etl_central.assets.ingresos_detallado_cp import (
    find_all_ingresos_detallado_cp_years,
    extract_ingresos_detallado_cp_data,
    INGRESOS_CP_PREFIX_DEFAULT,
)

def _bucket(): return os.getenv("BUCKET_NAME", "centralfiles3")
def _prefix(): return os.getenv("INGRESOS_CP_PREFIX", INGRESOS_CP_PREFIX_DEFAULT)

def test_s3_list_years_ingresos_cp():
    years = find_all_ingresos_detallado_cp_years(bucket_name=_bucket(), prefix=_prefix())
    if not years:
        pytest.skip("No Ingresos Detallado CP files found in S3; skipping.")
    assert all(isinstance(y, int) for y in years)

def test_s3_can_read_single_ingresos_cp_file():
    years = find_all_ingresos_detallado_cp_years(bucket_name=_bucket(), prefix=_prefix())
    if not years:
        pytest.skip("No Ingresos Detallado CP files found in S3; skipping.")
    year = max(years)
    df, path = extract_ingresos_detallado_cp_data(
        year=year, source="s3", bucket_name=_bucket(), prefix=_prefix()
    )
    assert path and isinstance(path, str)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
