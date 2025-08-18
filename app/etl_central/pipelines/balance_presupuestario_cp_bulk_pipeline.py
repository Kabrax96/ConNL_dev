import os
from dotenv import load_dotenv
from sqlalchemy import MetaData

from app.etl_central.assets.pipeline_logging import PipelineLogging
from app.etl_central.assets.metadata_logging import MetaDataLogging, MetaDataLoggingStatus
from app.etl_central.connectors.postgresql import PostgreSqlClient

from app.etl_central.assets.balance_presupuestario_cp import (
    extract_cp_data,
    transform_cp_data,
    get_target_table,
    find_all_cp_years,
)

def _find_latest_cp_year(bucket: str, prefix: str, logger) -> int | None:
    years = find_all_cp_years(bucket_name=bucket, prefix=prefix)
    logger.info(f"115 | Years detected under s3://{bucket}/{prefix}: {years}")
    return max(years) if years else None

def pipeline(pipeline_logging: PipelineLogging):
    logger = pipeline_logging.logger
    logger.info("100 | Starting CP single pipeline run")

    # --- Env/config ---
    SERVER_NAME = os.getenv("SERVER_NAME")
    DATABASE_NAME = os.getenv("DATABASE_NAME")
    DB_USERNAME = os.getenv("DB_USERNAME")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    PORT = int(os.getenv("PORT", "5432"))
    BUCKET_NAME = os.getenv("BUCKET_NAME", "centralfiles3")

    # Make S3 prefix configurable; default to the CP folder
    CP_S3_PREFIX = os.getenv("CP_S3_PREFIX", "finanzas/Balance_Presupuestario_CP/raw/")
    logger.info(f"105 | Using CP_S3_PREFIX='{CP_S3_PREFIX}'")

    # 1) Discover latest CP year in that prefix
    year = _find_latest_cp_year(bucket=BUCKET_NAME, prefix=CP_S3_PREFIX, logger=logger)
    if not year:
        raise FileNotFoundError(
            f"No valid CP Balance Presupuestario file found in S3. "
            f"Checked s3://{BUCKET_NAME}/{CP_S3_PREFIX}"
        )
    logger.info(f"120 | Latest CP year detected: {year}")

    # 2) Extract
    logger.info("200 | Extracting CP data from S3")
    extracted_df, file_path = extract_cp_data(
        year=year,
        source="s3",
        bucket_name=BUCKET_NAME,
        prefix=CP_S3_PREFIX,  # pass the same prefix
    )
    if extracted_df.empty:
        raise ValueError(f"400 | File {file_path} is empty or could not be read.")
    logger.info(f"210 | Extracted rows: {extracted_df.shape[0]} from {file_path}")

    # 3) Transform
    logger.info("300 | Transforming CP data")
    transformed_df = transform_cp_data(extracted_df, year=year)
    logger.info(f"310 | Transformed rows: {transformed_df.shape[0]}")

    # 4) Load (UPSERT)
    logger.info("400 | Preparing DB objects")
    postgresql_client = PostgreSqlClient(
        server_name=SERVER_NAME,
        database_name=DATABASE_NAME,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        port=PORT,
    )
    metadata = MetaData()
    table = get_target_table(metadata)

    logger.info("410 | Loading CP data into PostgreSQL (upsert)")
    postgresql_client.upsert(
        data=transformed_df.to_dict(orient="records"),
        table=table,
        metadata=metadata,
    )
    logger.info("499 | Load completed successfully")

def run_cp_single_pipeline(pipeline_name: str, log_client: PostgreSqlClient):
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
        pipeline_logging.logger.error(f"500 | CP single pipeline run failed: {e}")
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

    PIPELINE_NAME = "balance_presupuestario_cp_single_pipeline"
    run_cp_single_pipeline(pipeline_name=PIPELINE_NAME, log_client=log_client)
