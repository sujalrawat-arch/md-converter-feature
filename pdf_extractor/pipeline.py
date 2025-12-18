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


def run_pipeline(file_id: str, s3_path: str):
    ctx = JobCtx.build(file_id, s3_path)
    log = setup_logger(ctx)
    log.info("=== START job=%s ===", file_id)
    ctx.load_status()
    if not USE_VISION:
        log.info("[vision] disabled via env (PDF_EXTRACTOR_USE_VISION/USE_VISION)")
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
