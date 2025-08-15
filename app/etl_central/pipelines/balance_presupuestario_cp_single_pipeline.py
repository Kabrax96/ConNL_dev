import os
import re
import boto3
from dotenv import load_dotenv
from sqlalchemy import MetaData

from app.etl_central.assets.balance_presupuestario_cp import (
    extract_cp_data,
    transform_cp_data,
    get_target_table,
)
from app.etl_central.assets.balance_presupuestario import (
    generate_surrogate_key,  # reuse your existing helper
)
from app.etl_central.assets.pipeline_logging import PipelineLogging
from app.etl_central.assets.metadata_logging import MetaDataLogging, MetaDataLoggingStatus
from app.etl_central.connectors.postgresql import PostgreSqlClient


def find_latest_cp_year(bucket_name: str = "centralfiles3") -> int | None:
    """
    Find the most recent CP YEAR in S3.
    Returns an int year (e.g., 2024) or None if not found.
    """
    prefix = "finanzas/Balance_Presupuestario/raw/"
    s3 = boto3.client("s3")
    pattern = r"formato_4_balance_presupuestario_-_ldf_cp(\d{4})\.xlsx"

    latest_year = None
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get("Contents", []):
            fname = obj["Key"].split("/")[-1]
            m = re.match(pattern, fname, flags=re.IGNORECASE)
            if m:
                year = int(m.group(1))
                latest_year = year if latest_year is None else max(latest_year, year)

    return latest_year


def pipeline(pipeline_logging: PipelineLogging):
    pipeline_logging.logger.info("100 | Starting CP single pipeline run")

    # Env/config
    SERVER_NAME = os.getenv("SERVER_NAME")
    DATABASE_NAME = os.getenv("DATABASE_NAME")
    DB_USERNAME = os.getenv("DB_USERNAME")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    PORT = int(os.getenv("PORT", "5432"))
    BUCKET_NAME = os.getenv("BUCKET_NAME", "centralfiles3")

    # 1) Discover latest CP year
    year = find_latest_cp_year(bucket_name=BUCKET_NAME)
    if not year:
        raise FileNotFoundError("No valid CP Balance Presupuestario file found in S3.")
    pipeline_logging.logger.info(f"110 | Latest CP detected: year={year}")

    # 2) Extract
    pipeline_logging.logger.info("200 | Extracting CP data from S3")
    extracted_df, file_path = extract_cp_data(
        year=year,
        source="s3",
        bucket_name=BUCKET_NAME,
    )
    if extracted_df.empty:
        raise ValueError(f"400 | File {file_path} is empty or could not be read.")
    pipeline_logging.logger.info(f"210 | Extracted rows: {extracted_df.shape[0]} from {file_path}")

    # 3) Transform
    pipeline_logging.logger.info("300 | Transforming data")
    transformed_df = transform_cp_data(extracted_df, year)

    # If your transform already assigns surrogate_key, you can skip the next line.
    transformed_df = generate_surrogate_key(transformed_df)
    pipeline_logging.logger.info(f"310 | Transformed rows: {transformed_df.shape[0]}")

    # 4) Load (UPSERT)
    pipeline_logging.logger.info("400 | Preparing DB objects")
    postgresql_client = PostgreSqlClient(
        server_name=SERVER_NAME,
        database_name=DATABASE_NAME,
        username=DB_USERNAME,
        password=DB_PASSWORD,
        port=PORT,
    )
    metadata = MetaData()
    table = get_target_table(metadata)

    pipeline_logging.logger.info("410 | Loading data into PostgreSQL (upsert)")
    postgresql_client.upsert(
        data=transformed_df.to_dict(orient="records"),
        table=table,
        metadata=metadata,
    )
    pipeline_logging.logger.info("499 | CP single pipeline run successful")


def run_cp_single_pipeline(pipeline_name: str, log_client: PostgreSqlClient):
    """
    Wrapper that:
      - sets up file+stdout logging
      - writes start/success/failure to metadata table
      - runs the single pipeline()
    """
    log_dir = os.getenv("LOG_DIR", "./logs")
    pipeline_logging = PipelineLogging(pipeline_name=pipeline_name, log_folder_path=log_dir)

    metadata_logger = MetaDataLogging(
        pipeline_name=pipeline_name,
        postgresql_client=log_client,
        config={},
    )

    try:
        metadata_logger.log()  # start
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

    # Logging DB (metadata table)
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
