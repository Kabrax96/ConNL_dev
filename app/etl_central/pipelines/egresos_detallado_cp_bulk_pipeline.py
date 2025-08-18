# app/etl_central/pipelines/egresos_detallado_cp_bulk_pipeline.py
import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import MetaData

from app.etl_central.assets.pipeline_logging import PipelineLogging
from app.etl_central.assets.metadata_logging import MetaDataLogging, MetaDataLoggingStatus
from app.etl_central.connectors.postgresql import PostgreSqlClient

from app.etl_central.assets.egresos_detallado_cp import (
    find_all_egresos_detallado_cp_years,
    extract_egresos_detallado_cp_data,
    transform_egresos_detallado_cp_data,
    get_egresos_detallado_cp_table,
    bulk_load,   # uses TRUNCATE + INSERT
)

# Hardcoded S3 prefix (Spanish folder)
CP_S3_PREFIX = "finanzas/Egresos_Detallado_CP/raw/"

def pipeline(pipeline_logging: PipelineLogging):
    logger = pipeline_logging.logger
    logger.info("100 | Starting Egresos Detallado CP bulk pipeline run")
    logger.info(f"105 | Using hardcoded CP_S3_PREFIX='{CP_S3_PREFIX}'")

    # --- Env/config ---
    SERVER_NAME = os.getenv("SERVER_NAME")
    DATABASE_NAME = os.getenv("DATABASE_NAME")
    DB_USERNAME = os.getenv("DB_USERNAME")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    PORT = int(os.getenv("PORT", "5432"))
    BUCKET_NAME = os.getenv("BUCKET_NAME", "centralfiles3")

    # Discover all CP years
    years = find_all_egresos_detallado_cp_years(bucket_name=BUCKET_NAME, prefix=CP_S3_PREFIX)
    logger.info(f"115 | Years detected under s3://{BUCKET_NAME}/{CP_S3_PREFIX}: {years}")
    if not years:
        raise FileNotFoundError(
            f"No valid Egresos Detallado CP files under s3://{BUCKET_NAME}/{CP_S3_PREFIX}"
        )

    # Extract & transform each year
    parts = []
    for year in years:
        logger.info(f"120 | Processing CP year={year}")

        logger.info("200 | Extracting CP Egresos Detallado from S3")
        df_raw, path = extract_egresos_detallado_cp_data(
            year=year,
            source="s3",
            bucket_name=BUCKET_NAME,
            prefix=CP_S3_PREFIX,
        )
        if df_raw.empty:
            logger.warning(f"205 | Skipping year={year}: file empty or unreadable ({path})")
            continue
        logger.info(f"210 | Extracted rows: {df_raw.shape[0]} from {path}")

        logger.info("300 | Transforming CP Egresos Detallado")
        df_tr = transform_egresos_detallado_cp_data(df_raw, year=year)
        logger.info(f"310 | Transformed rows (year={year}): {df_tr.shape[0]}")
        if not df_tr.empty:
            parts.append(df_tr)

    if not parts:
        raise ValueError("No data extracted/transformed from any CP years.")

    final_df = pd.concat(parts, ignore_index=True)
    logger.info(f"320 | Final concatenated rows: {final_df.shape[0]}")

    # Load (TRUNCATE + INSERT)
    logger.info("400 | Preparing DB objects")
    client = PostgreSqlClient(
        server_name=SERVER_NAME,
        database_name=DATABASE_NAME,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        port=PORT,
    )
    metadata = MetaData()
    table = get_egresos_detallado_cp_table(metadata)

    logger.info("410 | Bulk loading into PostgreSQL (TRUNCATE + INSERT)")
    bulk_load(df=final_df, postgresql_client=client, table=table, metadata=metadata)
    logger.info("499 | Egresos CP bulk pipeline run successful")

def run_egresos_cp_bulk_pipeline(pipeline_name: str, log_client: PostgreSqlClient):
    log_dir = os.getenv("LOG_DIR", "./logs")
    pipeline_logging = PipelineLogging(pipeline_name=pipeline_name, log_folder_path=log_dir)
    metadata_logger = MetaDataLogging(
        pipeline_name=pipeline_name,
        postgresql_client=log_client,
        config={},
    )
    try:
        metadata_logger.log()
        pipeline(pipeline_logging=pipeline_logging)
        metadata_logger.log(
            status=MetaDataLoggingStatus.RUN_SUCCESS,
            logs=pipeline_logging.get_logs(),
        )
        pipeline_logging.logger.handlers.clear()
    except Exception as e:
        pipeline_logging.logger.error(f"500 | Egresos CP bulk pipeline run failed: {e}")
        metadata_logger.log(
            status=MetaDataLoggingStatus.RUN_FAILURE,
            logs=pipeline_logging.get_logs(),
        )
        pipeline_logging.logger.handlers.clear()
        raise

if __name__ == "__main__":
    load_dotenv()

    LOGGING_SERVER_NAME = os.getenv("LOGGING_SERVER_NAME")
    LOGGING_DATABASE_NAME = os.getenv("LOGGING_DATABASE_NAME")
    LOGGING_USERNAME = os.getenv("LOGGING_USERNAME")
    LOGGING_PASSWORD = os.getenv("LOGGING_PASSWORD")
    LOGGING_PORT = int(os.getenv("LOGGING_PORT", "5432"))

    log_client = PostgreSqlClient(
        server_name=LOGGING_SERVER_NAME,
        database_name=LOGGING_DATABASE_NAME,
        username=LOGGING_USERNAME,
        password=LOGGING_PASSWORD,
        port=LOGGING_PORT,
    )

    PIPELINE_NAME = "egresos_detallado_cp_bulk_pipeline"
    run_egresos_cp_bulk_pipeline(pipeline_name=PIPELINE_NAME, log_client=log_client)
