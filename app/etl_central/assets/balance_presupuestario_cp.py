
# app/etl_central/assets/balance_presupuestario_cp.py
import os
import re
import uuid
import base64
import logging
from io import BytesIO
from typing import Tuple, Optional, List

import pandas as pd
import boto3

from sqlalchemy import MetaData, Table, Column, String, Float
from sqlalchemy import text as sa_text
from sqlalchemy.dialects.postgresql import insert as pg_insert

# Reuse your existing client and helpers
from app.etl_central.connectors.postgresql import PostgreSqlClient
from app.etl_central.assets.helpers import (
    extraer_codigo_y_sublabel,
    _normalize_amount_for_cp,
    clean_amount,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# ---------------------------------------------------------------------
# File naming (NEW pattern only)
# ---------------------------------------------------------------------
# Example: F4_Balance_Presupuestario_LDF_CP2024.xlsx
CP_FILENAME_TEMPLATE = "F4_Balance_Presupuestario_LDF_CP{year}.xlsx"
CP_REGEX = re.compile(r"F4_Balance_Presupuestario_LDF_CP(\d{4})\.xlsx$", re.IGNORECASE)

# ---------------------------------------------------------------------
# Utils
# ---------------------------------------------------------------------
def _generate_surrogate_key() -> str:
    uuid_part = uuid.uuid4().bytes
    entropy = os.urandom(16)
    raw = uuid_part + entropy
    return base64.urlsafe_b64encode(raw).rstrip(b'=').decode('ascii')


def get_target_table(metadata: MetaData) -> Table:
    """Target table for CP data (leave name as-is unless you want a separate table)."""
    return Table(
        "nuevo_leon_balance_presupuestario_cp",
        metadata,
        Column("surrogate_key", String, primary_key=True),
        Column("concept", String),
        Column("sublabel", String),
        Column("year_quarter", String),
        Column("full_date", String),
        Column("type", String),
        Column("amount", Float),
    )

# ---------------------------------------------------------------------
# Extraction (CP) — NEW name only
# ---------------------------------------------------------------------
def extract_cp_data(
    year: int,
    source: str = "local",  # "local" | "s3"
    bucket_name: Optional[str] = None,
    prefix: str = "finanzas/Balance_Presupuestario_CP/raw/",
) -> Tuple[pd.DataFrame, Optional[str]]:
    """
    Read the annual cumulative CP file for the given year.
    NEW naming ONLY: F4_Balance_Presupuestario_LDF_CP{year}.xlsx
    Sheet: 'F4 BAP'
    """
    filename = CP_FILENAME_TEMPLATE.format(year=year)

    if source == "local":
        base_dir = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.abspath(os.path.join(base_dir, "..", "data", "presupuestos"))
        file_path = os.path.join(data_dir, filename)
        if not os.path.exists(file_path):
            logging.warning("Local CP file not found: %s", file_path)
            return pd.DataFrame(), None
        try:
            df = pd.read_excel(file_path, sheet_name="F4 BAP", header=None)
            return df, file_path
        except Exception as e:
            logging.error("Error reading Excel %s: %s", file_path, e)
            return pd.DataFrame(), None

    elif source == "s3":
        if not bucket_name:
            raise ValueError("bucket_name is required for source='s3'")

        s3 = boto3.client("s3")
        # First try direct path (fast path)
        s3_key = f"{prefix.rstrip('/')}/{filename}"
        try:
            obj = s3.get_object(Bucket=bucket_name, Key=s3_key)
            df = pd.read_excel(BytesIO(obj["Body"].read()), sheet_name="F4 BAP", header=None)
            return df, f"s3://{bucket_name}/{s3_key}"
        except Exception:
            # Fallback: scan prefix for the exact (case-insensitive) new pattern for this year
            chosen_key = None
            paginator = s3.get_paginator("list_objects_v2")
            for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix.rstrip("/") + "/"):
                for o in page.get("Contents", []):
                    key = o["Key"]
                    fname = key.split("/")[-1]
                    m = CP_REGEX.match(fname)
                    if m and int(m.group(1)) == int(year):
                        chosen_key = key
                        break
                if chosen_key:
                    break

            if not chosen_key:
                logging.error(
                    "CP file (NEW name) for year=%s not found in s3://%s/%s. "
                    "Expected filename: %s",
                    year, bucket_name, prefix, filename
                )
                return pd.DataFrame(), None

            try:
                obj = s3.get_object(Bucket=bucket_name, Key=chosen_key)
                df = pd.read_excel(BytesIO(obj["Body"].read()), sheet_name="F4 BAP", header=None)
                return df, f"s3://{bucket_name}/{chosen_key}"
            except Exception as e:
                logging.error("Failed to read s3://%s/%s. Error: %s", bucket_name, chosen_key, e)
                return pd.DataFrame(), None

    else:
        raise ValueError("Invalid source. Use 'local' or 's3'.")

def find_all_cp_years(
    bucket_name: str = "centralfiles3",
    prefix: str = "finanzas/Balance_Presupuestario_CP/raw/",
) -> List[int]:
    """
    List all available CP YEARS in S3 using the NEW naming only:
      F4_Balance_Presupuestario_LDF_CPYYYY.xlsx
    """
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")

    years: List[int] = []
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get("Contents", []):
            fname = obj["Key"].split("/")[-1]
            m = CP_REGEX.match(fname)
            if m:
                years.append(int(m.group(1)))
    return sorted(set(years))

# ---------------------------------------------------------------------
# Transform (CP)
# ---------------------------------------------------------------------
def transform_cp_data(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """
    Transform CP (annual cumulative) sheet into long format ready for loading.
    Uses the same logic as trimestral with minimal changes:
      - year_quarter = f"{year}_CP"
      - full_date    = f"{year}-12-31"

    Output columns:
      surrogate_key, concept, sublabel, year_quarter, full_date, type, amount
    """
    if df.empty:
        return pd.DataFrame(
            columns=[
                "surrogate_key", "concept", "sublabel",
                "year_quarter", "full_date", "type", "amount"
            ]
        )

    year_quarter = f"{year}_CP"
    full_date = f"{year}-12-31"

    # 1) keep only detailed codes (exclude A., B., C., etc. aggregates)
    pattern = r"^(A[123]|B[12]|C[12]|E[12]|F[12]|G[12])\."
    mask = df[1].astype(str).str.match(pattern, na=False)
    df_codes = df[mask].copy()

    # 2) deduplicate by code (keep first occurrence)
    df_codes["code"] = df_codes[1].str.extract(pattern)
    df_unique = df_codes.drop_duplicates(subset="code", keep="first").drop(columns="code")

    # 3) select useful columns: col 1 = text; cols 2/3/4 = amounts
    df_final = df_unique.iloc[:, 1:].reset_index(drop=True)
    df_final.columns = ["raw_concept", "estimated_or_approved", "devengado", "recaudado_pagado"]

    # 4) split concept/sublabel
    df_final[["concept", "sublabel"]] = df_final["raw_concept"].apply(
        lambda x: pd.Series(extraer_codigo_y_sublabel(str(x)))
    )
    df_final.drop(columns=["raw_concept"], inplace=True)

    # 5) normalize accounting formats BEFORE cleaning ((), trailing minus, unicode minus)
    amount_cols = ["estimated_or_approved", "devengado", "recaudado_pagado"]
    for c in amount_cols:
        df_final[c] = df_final[c].map(_normalize_amount_for_cp)

    # 6) long format
    df_long = df_final.melt(
        id_vars=["concept", "sublabel"],
        value_vars=amount_cols,
        var_name="type",
        value_name="amount",
    )

    # 7) period fields
    df_long["year_quarter"] = year_quarter
    df_long["full_date"] = full_date
    df_long = df_long[["concept", "sublabel", "year_quarter", "full_date", "type", "amount"]]

    # 8) final amount cleanup with shared helper, ensure NaN -> None
    df_long["amount"] = df_long["amount"].apply(clean_amount)
    df_long["amount"] = df_long["amount"].map(lambda v: None if pd.isna(v) else v)

    # 9) surrogate key and final order
    df_long["surrogate_key"] = [_generate_surrogate_key() for _ in range(len(df_long))]
    df_long = df_long[["surrogate_key", "concept", "sublabel", "year_quarter", "full_date", "type", "amount"]]

    return df_long

# ---------------------------------------------------------------------
# Load helpers
# ---------------------------------------------------------------------
def load(
    df: pd.DataFrame,
    postgresql_client: PostgreSqlClient,
    table: Table,
    metadata: MetaData,
    load_method: str = "upsert",
) -> None:
    """Generic load entrypoint leveraging your PostgreSqlClient helpers."""
    if load_method == "insert":
        postgresql_client.insert(data=df.to_dict(orient="records"), table=table, metadata=metadata)
    elif load_method == "upsert":
        postgresql_client.upsert(data=df.to_dict(orient="records"), table=table, metadata=metadata)
    elif load_method == "overwrite":
        postgresql_client.overwrite(data=df.to_dict(orient="records"), table=table, metadata=metadata)
    else:
        raise ValueError("Invalid load method: choose from [insert, upsert, overwrite]")


def single_upsert(
    df: pd.DataFrame,
    postgresql_client: PostgreSqlClient,
    table: Table,
    metadata: MetaData,
) -> None:
    """
    Incremental UPSERT by surrogate_key using pg_insert(...).on_conflict_do_update(...).
    """
    try:
        metadata.create_all(postgresql_client.engine)
        with postgresql_client.engine.connect() as conn:
            insert_stmt = pg_insert(table).values(df.to_dict(orient="records"))
            update_stmt = insert_stmt.on_conflict_do_update(
                index_elements=["surrogate_key"],
                set_={
                    "concept": insert_stmt.excluded.concept,
                    "sublabel": insert_stmt.excluded.sublabel,
                    "year_quarter": insert_stmt.excluded.year_quarter,
                    "full_date": insert_stmt.excluded.full_date,
                    "type": insert_stmt.excluded.type,
                    "amount": insert_stmt.excluded.amount,
                },
            )
            conn.execute(update_stmt)
            conn.commit()
    except Exception as e:
        raise RuntimeError(f"Single upsert failed: {e}")


def bulk_overwrite(
    df: pd.DataFrame,
    postgresql_client: PostgreSqlClient,
    table: Table,
    metadata: MetaData,
) -> None:
    """
    TRUNCATE + INSERT. Use only for full historical reloads.
    """
    try:
        metadata.create_all(postgresql_client.engine)
        with postgresql_client.engine.connect() as conn:
            conn.execute(sa_text(f"TRUNCATE TABLE {table.name};"))
            conn.execute(table.insert(), df.to_dict(orient="records"))
            conn.commit()
    except Exception as e:
        raise RuntimeError(f"Bulk overwrite failed: {e}")
