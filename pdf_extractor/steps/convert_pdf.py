from __future__ import annotations

import os
from pathlib import Path

from ..convert import convert_to_pdf


def step_00_convert_to_pdf(ctx, log):
    if ctx.last_step in ("convert_pdf", "classify", "rotation", "upload_textract", "textract"):
        log.info("[convert] skipping (already converted)")
        return

    source = ctx.source_path or ctx.local_pdf
    if not source or not os.path.exists(source):
        if ctx.local_pdf and os.path.exists(ctx.local_pdf):
            source = ctx.local_pdf
        else:
            raise RuntimeError("Source document missing; download step may have failed")

    ext = Path(source).suffix.lower()
    if ext == ".pdf":
        ctx.local_pdf = source
        ctx.save_status("convert_pdf", {"converted": False})
        log.info("[convert] source already PDF, skipping conversion")
        return

    log.info("[convert] converting %s → PDF", source)
    pdf_path = convert_to_pdf(source, ctx.job_dir)
    ctx.local_pdf = pdf_path
    ctx.save_status("convert_pdf", {"converted": True, "converted_path": pdf_path})
    log.info("[convert] output → %s", pdf_path)
