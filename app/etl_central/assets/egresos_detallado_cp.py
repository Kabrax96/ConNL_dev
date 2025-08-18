# app/etl_central/assets/egresos_detallado_cp.py
import os
import re
import uuid
import base64
import logging
from io import BytesIO
from typing import Optional, Tuple, List

import pandas as pd
import boto3
from sqlalchemy import MetaData, Table, Column, String, Float, text as sa_text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.etl_central.connectors.postgresql import PostgreSqlClient

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# -----------------------------------------------------------------------------
# Constants (CP naming & location)
# -----------------------------------------------------------------------------
# S3 prefix (folder names remain in Spanish)
CP_S3_PREFIX_DEFAULT = "finanzas/Egresos_Detallado_CP/raw/"
# Example: F6_a_EAPED_Clas_Obj_Gas_LDF_CP2024.xlsx
CP_FILENAME_TEMPLATE = "F6_a_EAPED_Clas_Obj_Gas_LDF_CP{year}.xlsx"
CP_REGEX = re.compile(r"F6_a_EAPED_Clas_Obj_Gas_LDF_CP(\d{4})\.xlsx$", re.IGNORECASE)

# -----------------------------------------------------------------------------
# Utils
# -----------------------------------------------------------------------------
def generate_truly_unique_key() -> str:
    uuid_part = uuid.uuid4().bytes
    entropy = os.urandom(16)
    raw = uuid_part + entropy
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")

def generate_surrogate_key(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["surrogate_key"] = [generate_truly_unique_key() for _ in range(len(df))]
    return df

def extract_codigo(texto: str) -> Optional[str]:
    """
    Extracts a code like 'A1)' -> 'A1'. Keep this regex aligned with your source’s format.
    """
    m = re.match(r"^\s*([A-Za-z])([0-9]+)\)", str(texto))
    if m:
        return (m.group(1).upper() + m.group(2))
    return None

# -----------------------------------------------------------------------------
# Table
# -----------------------------------------------------------------------------
def get_egresos_detallado_cp_table(metadata: MetaData) -> Table:
    """
    Target table for CP data. (Same columns as trimestral, plus we pin 'Cuarto' to 'CP'.)
    """
    return Table(
        "nuevo_leon_egresos_detallado_cp",
        metadata,
        Column("surrogate_key", String, primary_key=True),
        Column("Codigo", String),
        Column("Concepto", String),
        Column("Aprobado", Float),
        Column("Ampliaciones/Reducciones", Float),
        Column("Modificado", Float),
        Column("Devengado", Float),
        Column("Pagado", Float),
        Column("Subejercicio", Float),
        Column("Fecha", String),   # e.g., 2024-12-31
        Column("Cuarto", String),  # always "CP"
        Column("Seccion", String), # "I" or "II"
    )

# -----------------------------------------------------------------------------
# Extract (CP)
# -----------------------------------------------------------------------------
def extract_egresos_detallado_cp_data(
    year: int,
    source: str = "s3",
    bucket_name: Optional[str] = None,
    prefix: str = CP_S3_PREFIX_DEFAULT,
) -> Tuple[pd.DataFrame, Optional[str]]:
    """
    Reads the CP (annual cumulative) Egresos Detallado file for the given year.
    Naming: F6_a_EAPED_Clas_Obj_Gas_LDF_CP{year}.xlsx
    Sheet: 'F6a COG'
    """
    filename = CP_FILENAME_TEMPLATE.format(year=year)

    if source == "s3":
        if not bucket_name:
            raise ValueError("bucket_name is required for S3 extraction")
        s3_key = f"{prefix.rstrip('/')}/{filename}"
        try:
            s3 = boto3.client("s3")
            obj = s3.get_object(Bucket=bucket_name, Key=s3_key)
            df = pd.read_excel(BytesIO(obj["Body"].read()), sheet_name="F6a COG", header=None)
            return df, f"s3://{bucket_name}/{s3_key}"
        except Exception as e:
            logging.error("Failed to read s3://%s/%s. Error: %s", bucket_name, s3_key, e)
            return pd.DataFrame(), None
    elif source == "local":
        base_dir = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.abspath(os.path.join(base_dir, "..", "data", "presupuestos"))
        file_path = os.path.join(data_dir, filename)
        if not os.path.exists(file_path):
            logging.error("Local file not found: %s", file_path)
            return pd.DataFrame(), None
        try:
            df = pd.read_excel(file_path, sheet_name="F6a COG", header=None)
            return df, file_path
        except Exception as e:
            logging.error("Error reading Excel %s: %s", file_path, e)
            return pd.DataFrame(), None
    else:
        raise ValueError("Invalid source. Use 'local' or 's3'.")

def find_all_egresos_detallado_cp_years(
    bucket_name: str = "centralfiles3",
    prefix: str = CP_S3_PREFIX_DEFAULT,
) -> List[int]:
    """
    Lists all available CP YEARS in S3 by the new naming only:
      F6_a_EAPED_Clas_Obj_Gas_LDF_CPYYYY.xlsx
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

# -----------------------------------------------------------------------------
# Transform (CP)
# -----------------------------------------------------------------------------
def procesar_tabla(df_tabla: pd.DataFrame, fecha: str, cuarto: str) -> pd.DataFrame:
    """
    Normalize one section table (I or II):
      - Derive 'Codigo' from 'Concepto'
      - Drop rows without Codigo
      - Drop duplicate Codigo (keep first)
      - Add 'Fecha' and 'Cuarto'
    """
    df_tabla = df_tabla.copy()
    df_tabla["Codigo"] = df_tabla["Concepto"].apply(extract_codigo)
    df_tabla = df_tabla[df_tabla["Codigo"].notna()].drop_duplicates(subset="Codigo").reset_index(drop=True)
    df_tabla["Fecha"] = fecha
    df_tabla["Cuarto"] = cuarto
    return df_tabla

def transform_egresos_detallado_cp_data(df: pd.DataFrame, year: int) -> pd.DataFrame:
    """
    Transform CP (annual) Egresos Detallado:
      - Force Fecha = '{year}-12-31' and Cuarto = 'CP'
      - Split into Section I and II using header 'II. Gasto Etiquetado'
      - Return unified DataFrame with surrogate_key
    """
    if df.empty:
        return pd.DataFrame(
            columns=[
                "surrogate_key", "Codigo", "Concepto",
                "Aprobado", "Ampliaciones/Reducciones", "Modificado",
                "Devengado", "Pagado", "Subejercicio",
                "Fecha", "Cuarto", "Seccion",
            ]
        )

    # Fixed period values for CP
    fecha = f"{year}-12-31"
    cuarto = "CP"

    # Locate the 'II. Gasto Etiquetado' header row to split sections
    idx_ii_candidates = df[1].astype(str).str.contains(r"^\s*II\.\s*Gasto Etiquetado", regex=True, na=False)
    if not idx_ii_candidates.any():
        raise ValueError("Header 'II. Gasto Etiquetado' not found.")
    idx_ii_header = idx_ii_candidates.idxmax()

    # Section I block (rows 8 to header of II)
    columnas = ["Concepto", "Aprobado", "Ampliaciones/Reducciones", "Modificado", "Devengado", "Pagado", "Subejercicio"]
    data_I = df.iloc[8:idx_ii_header, 1:8].values
    tbl_I = pd.DataFrame(data_I, columns=columnas)

    # Section II block (header+1 to first blank row after; or end)
    fila_inicio_II = idx_ii_header + 1
    resto = df.iloc[fila_inicio_II:, 1].astype(str).str.strip()
    vacias = resto[resto == ""].index
    fila_fin_II = vacias[0] if len(vacias) > 0 else df.shape[0]
    data_II = df.iloc[fila_inicio_II:fila_fin_II, 1:8].values
    tbl_II = pd.DataFrame(data_II, columns=columnas)

    # Normalize sections
    df_I = procesar_tabla(tbl_I, fecha, cuarto)
    df_II = procesar_tabla(tbl_II, fecha, cuarto)
    df_I["Seccion"] = "I"
    df_II["Seccion"] = "II"

    out = pd.concat([df_I, df_II], ignore_index=True)
    out = generate_surrogate_key(out)
    return out

# -----------------------------------------------------------------------------
# Load
# -----------------------------------------------------------------------------
def single_load(
    df: pd.DataFrame,
    postgresql_client: PostgreSqlClient,
    table: Table,
    metadata: MetaData,
) -> None:
    """
    Incremental upsert using surrogate_key as PK.
    """
    try:
        metadata.create_all(postgresql_client.engine)
        with postgresql_client.engine.connect() as conn:
            insert_stmt = pg_insert(table).values(df.to_dict(orient="records"))
            update_stmt = insert_stmt.on_conflict_do_update(
                index_elements=["surrogate_key"],
                set_={col.name: insert_stmt.excluded[col.name] for col in table.columns if col.name != "surrogate_key"}
            )
            conn.execute(update_stmt)
            conn.commit()
    except Exception as e:
        raise RuntimeError(f"Single load (upsert) failed: {e}")

def bulk_load(
    df: pd.DataFrame,
    postgresql_client: PostgreSqlClient,
    table: Table,
    metadata: MetaData,
) -> None:
    """
    TRUNCATE + INSERT for full reloads.
    """
    try:
        metadata.create_all(postgresql_client.engine)
        with postgresql_client.engine.connect() as conn:
            conn.execute(sa_text(f"TRUNCATE TABLE {table.name};"))
            conn.execute(table.insert(), df.to_dict(orient="records"))
            conn.commit()
    except Exception as e:
        raise RuntimeError(f"Bulk load failed: {e}")

def load(
    df: pd.DataFrame,
    postgresql_client: PostgreSqlClient,
    table: Table,
    metadata: MetaData,
    load_method: str = "upsert",
) -> None:
    """
    Generic loader (insert | upsert | overwrite) delegating to your PostgreSqlClient.
    """
    if load_method == "insert":
        postgresql_client.insert(data=df.to_dict(orient="records"), table=table, metadata=metadata)
    elif load_method == "upsert":
        postgresql_client.upsert(data=df.to_dict(orient="records"), table=table, metadata=metadata)
    elif load_method == "overwrite":
        postgresql_client.overwrite(data=df.to_dict(orient="records"), table=table, metadata=metadata)
    else:
        raise ValueError("Invalid load method: choose from [insert, upsert, overwrite]")
