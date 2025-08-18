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
from app.etl_central.assets.helpers import (
    _normalize_amount_for_cp,
    clean_amount,
    _first_match_row,
    _find_section_ii_bounds,
)

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("ingresos_detallado_cp")

# ---------------------------------------------------------------------------
# Constantes (nombres y patrón de archivo F5 CP)
# ---------------------------------------------------------------------------
INGRESOS_CP_PREFIX_DEFAULT = "finanzas/Ingresos_Detallado_CP/raw/"
INGRESOS_CP_FILENAME_TMPL = "F5_Edo_Ana_Ing_Det_LDF_CP{year}.xlsx"
INGRESOS_CP_REGEX = re.compile(r"F5_Edo_Ana_Ing_Det_LDF_CP(\d{4})\.xlsx$", re.IGNORECASE)
SHEET_NAME = "F5 EAI"

# ---------------------------------------------------------------------------
# Utils
# ---------------------------------------------------------------------------
def _generate_surrogate_key() -> str:
    uuid_part = uuid.uuid4().bytes
    entropy = os.urandom(16)
    raw = uuid_part + entropy
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")

def _add_surrogate(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["surrogate_key"] = [_generate_surrogate_key() for _ in range(len(df))]
    return df

def _normalize_then_clean(x):
    s = _normalize_amount_for_cp(x)
    return clean_amount(s)

# ---------------------------------------------------------------------------
# Tabla RAW (la que usan tus pipelines)
# ---------------------------------------------------------------------------
def get_ingresos_detallado_cp_table(metadata: MetaData) -> Table:
    return Table(
        "nuevo_leon_ingresos_detallado_cp",
        metadata,
        Column("surrogate_key", String, primary_key=True),
        Column("concepto", String, nullable=True),
        Column("estimado", Float, nullable=True),
        Column("ampliaciones_reducciones", Float, nullable=True),
        Column("modificado", Float, nullable=True),
        Column("devengado", Float, nullable=True),
        Column("recaudado", Float, nullable=True),
        Column("diferencia", Float, nullable=True),
        Column("clave_primaria", String, nullable=True),
        Column("clave_secundaria", String, nullable=True),
        Column("fecha", String, nullable=True),   # YYYY-MM-DD
        Column("cuarto", String, nullable=True),  # 'CP'
        Column("seccion", String, nullable=True), # 'I' | 'II'
    )

# ---------------------------------------------------------------------------
# Extract
# ---------------------------------------------------------------------------
def extract_ingresos_detallado_cp_data(
    year: int,
    source: str = "s3",
    bucket_name: Optional[str] = None,
    prefix: str = INGRESOS_CP_PREFIX_DEFAULT,
) -> Tuple[pd.DataFrame, Optional[str]]:
    """
    Lee el F5 CP del año indicado desde S3 o local.
    """
    filename = INGRESOS_CP_FILENAME_TMPL.format(year=year)

    if source == "s3":
        if not bucket_name:
            raise ValueError("bucket_name es requerido para extracción S3")
        s3_key = f"{prefix.rstrip('/')}/{filename}"
        try:
            s3 = boto3.client("s3")
            obj = s3.get_object(Bucket=bucket_name, Key=s3_key)
            df = pd.read_excel(BytesIO(obj["Body"].read()), sheet_name=SHEET_NAME, header=None)
            return df, f"s3://{bucket_name}/{s3_key}"
        except Exception as e:
            logger.error("Error leyendo s3://%s/%s -> %s", bucket_name, s3_key, e)
            return pd.DataFrame(), None

    elif source == "local":
        base_dir = os.path.dirname(os.path.abspath(__file__))
        data_dir = os.path.abspath(os.path.join(base_dir, "..", "data", "presupuestos"))
        file_path = os.path.join(data_dir, filename)
        if not os.path.exists(file_path):
            logger.error("Archivo local no encontrado: %s", file_path)
            return pd.DataFrame(), None
        try:
            df = pd.read_excel(file_path, sheet_name=SHEET_NAME, header=None)
            return df, file_path
        except Exception as e:
            logger.error("Error leyendo Excel %s -> %s", file_path, e)
            return pd.DataFrame(), None
    else:
        raise ValueError("source inválido. Use 'local' o 's3'.")

def find_all_ingresos_detallado_cp_years(
    bucket_name: str = "centralfiles3",
    prefix: str = INGRESOS_CP_PREFIX_DEFAULT,
) -> List[int]:
    """
    Enumera los años disponibles en S3 para F5 CP.
    """
    s3 = boto3.client("s3")
    paginator = s3.get_paginator("list_objects_v2")
    years: List[int] = []
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get("Contents", []):
            fname = obj["Key"].split("/")[-1]
            m = INGRESOS_CP_REGEX.match(fname)
            if m:
                years.append(int(m.group(1)))
    return sorted(set(years))

# ---------------------------------------------------------------------------
# Transform helpers (detección dinámica y periodo)
# ---------------------------------------------------------------------------
_DATE_RX = re.compile(r"al\s+(\d{1,2})\s+de\s+(\w+)\s+de\s+(\d{4})", re.IGNORECASE)
_MONTHS = {"enero":1,"febrero":2,"marzo":3,"abril":4,"mayo":5,"junio":6,"julio":7,"agosto":8,"septiembre":9,"octubre":10,"noviembre":11,"diciembre":12}

def _extract_fecha_y_cuarto(df: pd.DataFrame) -> Tuple[str, str]:
    for r in range(0, min(8, df.shape[0])):
        txt = " ".join(df.iloc[r, 0:min(12, df.shape[1])].astype(str).str.strip().tolist())
        m = _DATE_RX.search(txt)
        if m:
            d = int(m.group(1)); mon = _MONTHS.get(m.group(2).lower(), 12); y = int(m.group(3))
            return f"{y}-{mon:02d}-{d:02d}", "CP"
    return "", "CP"

def _detect_concept_col(df: pd.DataFrame, search_cols: int = 16) -> int:
    """
    Busca la columna con más matches de encabezados 'A.' y renglones 'a1)'.
    """
    best_col, best_score = 1, -1
    rx_hdr = re.compile(r"^[A-Z]\.\s*")
    rx_det = re.compile(r"^[a-z]\d+\)")
    for c in range(0, min(search_cols, df.shape[1])):
        s = df.iloc[:, c].astype(str).str.strip()
        score = int(s.str.match(rx_hdr).sum()) + int(s.str.match(rx_det).sum())
        if score > best_score:
            best_col, best_score = c, score
    logger.info("Columna 'concepto' detectada: %s (score=%s)", best_col, best_score)
    return best_col

def _detect_amount_cols(df_section: pd.DataFrame, concept_col: int, max_span: int = 10, min_ratio: float = 0.20) -> List[int]:
    """
    Detecta hasta 6 columnas numéricas a la derecha de 'concepto'.
    Considera válida una columna si >= min_ratio (20%) de celdas parsean como número.
    Orden esperado: estimado, ampliaciones/reducciones, modificado, devengado, recaudado, diferencia.
    """
    candidates = []
    start = concept_col + 1
    end = min(df_section.shape[1], concept_col + 1 + max_span)
    sample = df_section.iloc[:, start:end]
    for j in range(sample.shape[1]):
        col_idx = start + j
        parsed = sample.iloc[:, j].map(_normalize_then_clean)
        ratio = parsed.notna().mean()
        if ratio >= min_ratio:
            candidates.append(col_idx)
    while len(candidates) < 6:
        candidates.append(-1)
    return candidates[:6]

def _slice_block(df: pd.DataFrame, r0: int, r1: int, seccion: str, concept_col: int) -> pd.DataFrame:
    """
    Extrae y normaliza un bloque [r0:r1] de la hoja.
    """
    blk = df.iloc[r0:r1, :].copy().reset_index(drop=True)
    concepto = blk.iloc[:, concept_col].astype(str).str.strip()
    amt_idx = _detect_amount_cols(blk, concept_col)

    def pick(idx):
        return [None]*len(blk) if idx == -1 else blk.iloc[:, idx]

    out = pd.DataFrame({"concepto": concepto})
    out["estimado"] = pick(amt_idx[0]).map(_normalize_then_clean)
    out["ampliaciones_reducciones"] = pick(amt_idx[1]).map(_normalize_then_clean)
    out["modificado"] = pick(amt_idx[2]).map(_normalize_then_clean)
    out["devengado"] = pick(amt_idx[3]).map(_normalize_then_clean)
    out["recaudado"] = pick(amt_idx[4]).map(_normalize_then_clean)
    out["diferencia"] = pick(amt_idx[5]).map(_normalize_then_clean)

    # Filas realmente vacías
    concept_blank = out["concepto"].isin(["", "nan", "None", None])
    all_null = out[["estimado","ampliaciones_reducciones","modificado","devengado","recaudado","diferencia"]].isna().all(axis=1)
    out = out.loc[~(concept_blank & all_null)].copy()

    # Claves y sección
    out["clave_primaria"]   = out["concepto"].astype(str).str.extract(r"^([A-Z])\.\s*")[0].map(lambda x: f"{x}." if pd.notna(x) else None)
    out["clave_secundaria"] = out["concepto"].astype(str).str.extract(r"^([a-z]\d+)\)")[0]
    out["seccion"] = seccion
    return out

# ---------------------------------------------------------------------------
# Transform principal (devuelve RAW compatible)
# ---------------------------------------------------------------------------
def transform_ingresos_detallado_cp_data(
    df: pd.DataFrame,
    year: Optional[int] = None,
    **kwargs,
) -> pd.DataFrame:
    """
    Produce el esquema RAW esperado por `nuevo_leon_ingresos_detallado_cp`.
    Aplica corte robusto de Sección II y filtros de DATA.
    """
    empty_raw_cols = [
        "surrogate_key","concepto",
        "estimado","ampliaciones_reducciones","modificado",
        "devengado","recaudado","diferencia",
        "clave_primaria","clave_secundaria",
        "fecha","cuarto","seccion",
    ]
    if df.empty:
        return pd.DataFrame(columns=empty_raw_cols)

    # Fecha/Cuarto
    fecha, cuarto = _extract_fecha_y_cuarto(df)
    if not fecha and year:
        fecha = f"{year}-12-31"

    # Detectores
    concept_col = _detect_concept_col(df)
    # NUEVO: cortes de II usando helpers
    start_ii, end_ii = _find_section_ii_bounds(df)

    # Cortes finales
    start_I_default = 7
    if start_ii is not None:
        start_I = start_I_default
        end_I = start_ii
        start_II = start_ii + 1
        end_II = end_ii if end_ii is not None else df.shape[0]
    else:
        # Fallback: todo como sección I
        start_I = start_I_default
        end_I = df.shape[0]
        start_II = None
        end_II = None

    frames: List[pd.DataFrame] = []
    if start_I < end_I:
        frames.append(_slice_block(df, start_I, end_I, "I", concept_col))
    if start_II is not None and start_II < (end_II or 0):
        frames.append(_slice_block(df, start_II, end_II, "II", concept_col))

    if not frames:
        return pd.DataFrame(columns=empty_raw_cols)

    out = pd.concat(frames, ignore_index=True)

    # -------------------------------
    # Filtros de DATA solicitados
    # -------------------------------
    # 1) Totales tipo "(H=h1+h2+...)" u otros "(X=...)"
    is_total = out["concepto"].str.match(r"^\s*\([A-Z]\s*=", na=False)

    # 2) "Ingresos de Libre Disposición" cuando NO tiene montos
    ild_mask = out["concepto"].str.strip().str.lower().eq("ingresos de libre disposición")
    no_montos = out[["estimado","ampliaciones_reducciones","modificado","devengado","recaudado","diferencia"]].isna().all(axis=1)
    ild_sin_monto = ild_mask & no_montos

    # 3) filas vacías (refuerzo)
    concept_blank = out["concepto"].isin(["", "nan", "None", None])
    vacias = concept_blank & no_montos

    out = out.loc[~(is_total | ild_sin_monto | vacias)].copy()

    # Periodo y surrogate
    out["fecha"] = fecha
    out["cuarto"] = "CP"

    out = _add_surrogate(out)
    out = out[
        [
            "surrogate_key","concepto",
            "estimado","ampliaciones_reducciones","modificado",
            "devengado","recaudado","diferencia",
            "clave_primaria","clave_secundaria",
            "fecha","cuarto","seccion",
        ]
    ]
    out = out.where(pd.notna(out), None)
    return out

# ---------------------------------------------------------------------------
# Load helpers + wrapper genérico
# ---------------------------------------------------------------------------
def single_load(
    df: pd.DataFrame,
    postgresql_client: PostgreSqlClient,
    table: Table,
    metadata: MetaData,
) -> None:
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
        raise RuntimeError(f"Single load (upsert) falló: {e}")

def bulk_load(
    df: pd.DataFrame,
    postgresql_client: PostgreSqlClient,
    table: Table,
    metadata: MetaData,
) -> None:
    try:
        metadata.create_all(postgresql_client.engine)
        with postgresql_client.engine.connect() as conn:
            conn.execute(sa_text(f"TRUNCATE TABLE {table.name};"))
            conn.execute(table.insert(), df.to_dict(orient="records"))
            conn.commit()
    except Exception as e:
        raise RuntimeError(f"Bulk load falló: {e}")

def load(
    df: pd.DataFrame,
    postgresql_client: PostgreSqlClient,
    table: Table,
    metadata: MetaData,
    load_method: str = "upsert",
) -> None:
    if load_method == "insert":
        postgresql_client.insert(data=df.to_dict(orient="records"), table=table, metadata=metadata)
    elif load_method == "upsert":
        postgresql_client.upsert(data=df.to_dict(orient="records"), table=table, metadata=metadata)
    elif load_method == "overwrite":
        postgresql_client.overwrite(data=df.to_dict(orient="records"), table=table, metadata=metadata)
    else:
        raise ValueError("load_method inválido: use [insert, upsert, overwrite]")
