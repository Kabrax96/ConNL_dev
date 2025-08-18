# app/etl_central/pipelines/ingresos_detallado_cp_single_pipeline.py
import os
from dotenv import load_dotenv
from sqlalchemy import MetaData

from app.etl_central.assets.pipeline_logging import PipelineLogging
from app.etl_central.assets.metadata_logging import MetaDataLogging, MetaDataLoggingStatus
from app.etl_central.connectors.postgresql import PostgreSqlClient

from app.etl_central.assets.ingresos_detallado_cp import (
    find_all_ingresos_detallado_cp_years,
    extract_ingresos_detallado_cp_data,
    transform_ingresos_detallado_cp_data,
    get_ingresos_detallado_cp_table,
    INGRESOS_CP_PREFIX_DEFAULT,
)

# Hardcoded CP prefix (Spanish folder)
CP_S3_PREFIX = "finanzas/Ingresos_Detallado_CP/raw/"

def _latest_year(bucket: str, logger) -> int | None:
    years = find_all_ingresos_detallado_cp_years(bucket_name=bucket, prefix=CP_S3_PREFIX)
    logger.info(f"115 | Years detected under s3://{bucket}/{CP_S3_PREFIX}: {years}")
    return max(years) if years else None

def pipeline(pipeline_logging: PipelineLogging):
    logger = pipeline_logging.logger
    logger.info("100 | Starting Ingresos Detallado CP single pipeline run")
    logger.info(f"105 | Using hardcoded CP_S3_PREFIX='{CP_S3_PREFIX}'")

    SERVER_NAME = os.getenv("SERVER_NAME")
    DATABASE_NAME = os.getenv("DATABASE_NAME")
    DB_USERNAME = os.getenv("DB_USERNAME")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    PORT = int(os.getenv("PORT", "5432"))
    BUCKET_NAME = os.getenv("BUCKET_NAME", "centralfiles3")

    # 1) Find latest year
    year = _latest_year(BUCKET_NAME, logger)
    if not year:
        raise FileNotFoundError(
            f"No valid Ingresos Detallado CP files under s3://{BUCKET_NAME}/{CP_S3_PREFIX}"
        )
    logger.info(f"120 | Latest CP year detected: {year}")

    # 2) Extract
    logger.info("200 | Extracting CP Ingresos Detallado from S3")
    df_raw, path = extract_ingresos_detallado_cp_data(
        year=year,
        source="s3",
        bucket_name=BUCKET_NAME,
        prefix=CP_S3_PREFIX,
    )
    if df_raw.empty:
        raise ValueError(f"400 | File {path} is empty or could not be read.")
    logger.info(f"210 | Extracted rows: {df_raw.shape[0]} from {path}")

    # 3) Transform
    logger.info("300 | Transforming CP Ingresos Detallado")
    df_tr = transform_ingresos_detallado_cp_data(df_raw, year=year)
    logger.info(f"310 | Transformed rows: {df_tr.shape[0]}")

    # 4) Load (UPSERT)
    logger.info("400 | Preparing DB objects")
    client = PostgreSqlClient(
        server_name=SERVER_NAME,
        database_name=DATABASE_NAME,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        port=PORT,
    )
    metadata = MetaData()
    table = get_ingresos_detallado_cp_table(metadata)

    logger.info("410 | Loading CP data into PostgreSQL (upsert)")
    client.upsert(
        data=df_tr.to_dict(orient="records"),
        table=table,
        metadata=metadata,
    )
    logger.info("499 | Load completed successfully")

def run_ingresos_cp_single_pipeline(pipeline_name: str, log_client: PostgreSqlClient):
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
        pipeline_logging.logger.error(f"500 | Ingresos CP single pipeline run failed: {e}")
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

    PIPELINE_NAME = "ingresos_detallado_cp_single_pipeline"
    run_ingresos_cp_single_pipeline(pipeline_name=PIPELINE_NAME, log_client=log_client)
