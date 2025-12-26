from __future__ import annotations

import argparse
import os

from .config import USE_VISION
from .context import JobCtx, setup_logger
from .steps.convert_pdf import step_00_convert_to_pdf
from .steps.download import step_00_download, step_03_upload_norm_for_textract, step_08_upload_and_cleanup
from .steps.rotation import step_02_rotation
from .steps.textract_run import step_04_textract
from .steps.unify import step_07_unify
from .steps.vision import step_05_render_for_vision, step_06_vision_async
from .utils import read_json


def _vision_chain(ctx, log):
    if not USE_VISION:
        log.info("[vision] skipped (disabled via env)")
        return
    step_05_render_for_vision(ctx, log)
    step_06_vision_async(ctx, log)
def run_pipeline(payload_or_file_id, s3_path: str | None = None):
    """Run pipeline using either legacy args or a full payload dict.

    payload dict keys expected: file_id, s3_path, user_id, tenant_id,
    customer_id, project_id, filename, version, message.
    """
    if isinstance(payload_or_file_id, dict):
        payload = payload_or_file_id
        file_id = str(payload.get("file_id", "")).strip()
        s3 = str(payload.get("s3_path", "")).strip()
        ctx = JobCtx.build_from_payload(payload)
    else:
        file_id = str(payload_or_file_id)
        s3 = str(s3_path)
        ctx = JobCtx.build(file_id, s3)

    log = setup_logger(ctx)
    log.info("=== START job=%s ===", file_id)
    ctx.load_status()
    if not USE_VISION:
        log.info("[vision] disabled via env (PDF_EXTRACTOR_USE_VISION/USE_VISION)")

    # DB: upsert initial file record using provided metadata
    try:
        from db.connection import SessionLocal
        from helper import handle_file_from_files_ms

        db = SessionLocal()
        db_info = handle_file_from_files_ms(
            db=db,
            external_file_id=file_id,
            user_id=ctx.user_id or file_id,
            tenant_id=ctx.tenant_id or "default_tenant",
            customer_id=ctx.customer_id or (ctx.tenant_id or "default_customer"),
            project_id=ctx.project_id or (ctx.tenant_id or "default_project"),
            file_name=ctx.file_name or file_id,
            platform_file_path=ctx.platform_file_path or s3,
            version=ctx.version or 1,
        )
        if db_info and db_info.get("ai_file_id"):
            ctx.ai_file_id = db_info["ai_file_id"]
        db.close()
    except Exception as exc:
        log.warning("[db] skipped/failed to upsert file record: %s", exc)

    try:
        if ctx.last_step == "":
            step_00_download(ctx, log)
        if ctx.last_step in ("", "download"):
            step_00_convert_to_pdf(ctx, log)
        if ctx.last_step in ("", "download", "convert_pdf"):
            step_02_rotation(ctx, log)
        if ctx.last_step in ("", "download", "convert_pdf", "rotation"):
            step_03_upload_norm_for_textract(ctx, log)
        # vision_cb = (lambda: _vision_chain(ctx, log)) if USE_VISION else None
        # Disabled vision parallel to avoid delays, using fallback only
        vision_cb = None
        if ctx.last_step in ("", "download", "convert_pdf", "rotation", "upload_textract"):
            step_04_textract(ctx, log, vision_callback=vision_cb)
        if USE_VISION:
            step_06_vision_async(ctx, log)
        step_07_unify(ctx, log)
        step_08_upload_and_cleanup(ctx, log)
        log.info("=== DONE job=%s ===", file_id)
    except Exception as exc:
        log.exception("Pipeline failed: %s", exc)
        raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file-id", required=True)
    parser.add_argument("--s3-path", required=True)
    args = parser.parse_args()
    run_pipeline(args.file_id, args.s3_path)


if __name__ == "__main__":  # pragma: no cover
    main()
