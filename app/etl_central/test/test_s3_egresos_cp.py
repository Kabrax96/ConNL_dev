# app/etl_central/test/test_s3_egresos_cp.py
import os
import pytest
import pandas as pd
from app.etl_central.assets.egresos_detallado_cp import (
    find_all_egresos_detallado_cp_years,
    extract_egresos_detallado_cp_data,
    CP_S3_PREFIX_DEFAULT,
)

def _bucket() -> str:
    return os.getenv("BUCKET_NAME", "centralfiles3")

def _prefix() -> str:
    # allow override via env var; default to the hardcoded folder
    return os.getenv("EGRESOS_CP_PREFIX", CP_S3_PREFIX_DEFAULT)

def test_s3_list_years_egresos_cp():
    years = find_all_egresos_detallado_cp_years(bucket_name=_bucket(), prefix=_prefix())
    if not years:
        pytest.skip("No Egresos Detallado CP files found in S3; skipping.")
    assert all(isinstance(y, int) for y in years)

def test_s3_can_read_single_egresos_cp_file():
    years = find_all_egresos_detallado_cp_years(bucket_name=_bucket(), prefix=_prefix())
    if not years:
        pytest.skip("No Egresos Detallado CP files found in S3; skipping.")
    year = max(years)
    df, path = extract_egresos_detallado_cp_data(
        year=year,
        source="s3",
        bucket_name=_bucket(),
        prefix=_prefix(),
    )
    assert path and isinstance(path, str)
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
