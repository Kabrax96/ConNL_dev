# lambda_handler.py
import os
from urllib.parse import unquote_plus
from app.etl_central.assets.pipeline_logging import PipelineLogging

# -----------------------------------------------------------------------------
# CP pipelines only (imports must match your file names)
# -----------------------------------------------------------------------------
from app.etl_central.pipelines.egresos_detallado_cp_single_pipeline import pipeline as egresos_cp_single_pipeline
from app.etl_central.pipelines.egresos_detallado_cp_bulk_pipeline import pipeline as egresos_cp_bulk_pipeline


from app.etl_central.pipelines.ingresos_detallado_cp_single_pipeline import pipeline as ingresos_cp_single_pipeline
from app.etl_central.pipelines.ingresos_detallado_cp_bulk_pipeline import pipeline as ingresos_cp_bulk_pipeline

from app.etl_central.pipelines.balance_presupuestario_cp_single_pipeline import pipeline as balance_cp_single_pipeline
from app.etl_central.pipelines.balance_presupuestario_cp_bulk_pipeline import pipeline as balance_cp_bulk_pipeline


# -----------------------------------------------------------------------------
# S3 routing helper
# -----------------------------------------------------------------------------
def _route_from_s3_event(event):
    """
    Determine which CP pipeline to run based on the S3 object key prefix.
    Returns a route key (e.g., 'ingresos_cp_single') or None if not from S3.
    """
    try:
        rec = event["Records"][0]
        if rec.get("eventSource") != "aws:s3":
            return None
        key = unquote_plus(rec["s3"]["object"]["key"])
    except Exception:
        return None

    # Balance Presupuestario CP
    if key.startswith("finanzas/Balance_Presupuestario_CP/raw/"):
        return "balance_cp_single"

    # Egresos Detallado CP
    if key.startswith("finanzas/Egresos_Detallado_CP/raw/"):
        return "egresos_cp_single"

    # Ingresos Detallado CP
    if key.startswith("finanzas/Ingresos_Detallado_CP/raw/"):
        return "ingresos_cp_single"

    return None


def _run(pipeline_func, pipeline_name: str):
    """
    Wrap the pipeline execution with local logging setup suited for AWS Lambda.
    """
    os.environ.setdefault("LOG_DIR", "/tmp/logs")
    os.makedirs(os.environ["LOG_DIR"], exist_ok=True)
    plog = PipelineLogging(pipeline_name=pipeline_name, log_folder_path=os.environ["LOG_DIR"])
    pipeline_func(pipeline_logging=plog)


# -----------------------------------------------------------------------------
# Lambda handler
# -----------------------------------------------------------------------------
def handler(event, context):
    """
    Entry point. Priority order:
      1) If triggered by S3, route by object key prefix (CP-only).
      2) Else, allow explicit {"pipeline": "..."} payload.
      3) Else, allow env PIPELINE_TARGET for manual invocations.
    """
    s3_target = _route_from_s3_event(event) if event else None
    target = s3_target or (event or {}).get("pipeline") or os.environ.get("PIPELINE_TARGET", "")

    routes = {
        # Egresos CP
        "egresos_cp_single": lambda: _run(egresos_cp_single_pipeline, "egresos_detallado_cp_single_pipeline"),
        "egresos_cp_bulk":   lambda: _run(egresos_cp_bulk_pipeline,   "egresos_detallado_cp_bulk_pipeline"),

        # Ingresos CP
        "ingresos_cp_single": lambda: _run(ingresos_cp_single_pipeline, "ingresos_detallado_cp_single_pipeline"),
        "ingresos_cp_bulk":   lambda: _run(ingresos_cp_bulk_pipeline,   "ingresos_detallado_cp_bulk_pipeline"),

        # Balance Presupuestario CP
        "balance_cp_single": lambda: _run(balance_cp_single_pipeline,  "balance_presupuestario_cp_single_pipeline"),
        "balance_cp_bulk":   lambda: _run(balance_cp_bulk_pipeline,    "balance_presupuestario_cp_bulk_pipeline"),
    }

    if target not in routes:
        valid = list(routes.keys())
        raise ValueError(f"Unknown pipeline '{target}'. Valid: {valid}")

    routes[target]()
    return {"ok": True, "pipeline": target, "db_logging": False}
