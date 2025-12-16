from __future__ import annotations

import os
import re
import shutil
from pathlib import Path

from ..aws_clients import s3_client
from ..config import BUCKET_OUT, BUCKET_TEX, KEEP_VISION_IMAGES


def step_00_download(ctx, log):
    """Download PDF from S3 using boto3 (authenticated)."""
    if ctx.source_path and os.path.exists(ctx.source_path):
        log.info("[download] already present → %s", ctx.source_path)
        return

    url = ctx.s3_path.strip()
    bucket = key = None
    if url.startswith("s3://"):
        match = re.match(r"s3://([^/]+)/(.+)", url)
        if match:
            bucket, key = match.group(1), match.group(2)
    elif "amazonaws.com" in url:
        match = re.match(r"https://([^\.]+)\.s3[\.-][^/]+\.amazonaws\.com/(.+)", url)
        if match:
            bucket, key = match.group(1), match.group(2)
        else:
            parts = url.split(".s3.")
            if len(parts) >= 2:
                bucket = parts[0].replace("https://", "")
                key = url.split(".amazonaws.com/")[-1]
    if not bucket or not key:
        raise ValueError(f"Cannot parse bucket/key from {url}")

    ext = Path(key).suffix.lower() or ".pdf"
    dest = Path(ctx.source_path).with_suffix(ext)
    dest.parent.mkdir(parents=True, exist_ok=True)
    ctx.source_path = str(dest)

    s3 = s3_client()
    log.info("[download] boto3 copy s3://%s/%s → %s", bucket, key, ctx.source_path)
    s3.download_file(bucket, key, ctx.source_path)
    if os.path.getsize(ctx.source_path) == 0:
        raise RuntimeError("Downloaded file empty")
    ctx.save_status("download", {"source_ext": ext, "source_key": key})
    log.info("[download] done, size=%.2f MB", os.path.getsize(ctx.source_path) / 1e6)


def step_03_upload_norm_for_textract(ctx, log):
    if ctx.last_step in ("upload_textract", "textract"):
        log.info("[upload_textract] skipping")
        return
    if not BUCKET_TEX:
        raise RuntimeError("AWS_BUCKET_PDF_READER_TEXTRACT not set.")
    if not os.path.exists(ctx.norm_pdf):
        log.error("[upload_textract] normalized PDF missing: %s", ctx.norm_pdf)
        raise RuntimeError("Normalized PDF not found; rotation/save may have failed")

    try:
        size_mb = os.path.getsize(ctx.norm_pdf) / 1e6
        log.info("[upload_textract] normalized PDF exists, size=%.2f MB", size_mb)
    except Exception:
        pass
    key = f"{ctx.file_id}/{os.path.basename(ctx.norm_pdf)}"
    s3 = s3_client()
    s3.upload_file(ctx.norm_pdf, BUCKET_TEX, key)
    ctx.save_status("upload_textract", {"textract_key": key})
    log.info("[upload_textract] s3://%s/%s", BUCKET_TEX, key)


def step_08_upload_and_cleanup(ctx, log):
    if not BUCKET_OUT:
        log.warning("[upload] skipping (no BUCKET_OUT)")
        return
    key = f"{ctx.file_id}/{os.path.basename(ctx.final_md)}"
    s3 = s3_client()
    s3.upload_file(ctx.final_md, BUCKET_OUT, key)
    log.info("[upload] s3://%s/%s", BUCKET_OUT, key)

    log.info("[cleanup] deleting local artifacts (vision, textract, uploads)")
    cleanup_files = [ctx.norm_pdf, ctx.textract_raw_json, ctx.local_pdf, ctx.source_path]
    if not KEEP_VISION_IMAGES:
        cleanup_files.append(ctx.vision_json)
    for path in cleanup_files:
        if os.path.exists(path):
            os.remove(path)
    shutil.rmtree(ctx.pages_dir, ignore_errors=True)
    if KEEP_VISION_IMAGES:
        log.info("[cleanup] keeping vision crops → %s", ctx.vision_dir)
    else:
        shutil.rmtree(ctx.vision_dir, ignore_errors=True)
    ctx.save_status("done", {"output_key": key})
    log.info("[cleanup] done")
