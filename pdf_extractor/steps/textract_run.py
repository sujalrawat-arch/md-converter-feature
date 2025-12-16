from __future__ import annotations

import threading
import time

from ..aws_clients import textract_client
from ..config import BUCKET_TEX
from ..utils import read_json, write_json


def _textract_wait_and_fetch(job_id, client, log):
    while True:
        resp = client.get_document_analysis(JobId=job_id)
        if resp["JobStatus"] in ("SUCCEEDED", "FAILED", "PARTIAL_SUCCESS"):
            pages = [resp]
            token = resp.get("NextToken")
            while token:
                chunk = client.get_document_analysis(JobId=job_id, NextToken=token)
                pages.append(chunk)
                token = chunk.get("NextToken")
            return {"status": resp["JobStatus"], "pages": pages}
        time.sleep(2)


def step_04_textract(ctx, log, vision_callback=None):
    if ctx.last_step == "textract":
        log.info("[textract] skipping")
        return
    key = read_json(ctx.status_file, {}).get("textract_key")
    if not key:
        raise RuntimeError("Missing textract_key")
    client = textract_client()
    s3obj = {"S3Object": {"Bucket": BUCKET_TEX, "Name": key}}
    features = ["TABLES", "FORMS", "LAYOUT"]
    log.info("[textract] starting document analysis for s3://%s/%s", BUCKET_TEX, key)
    start = client.start_document_analysis(DocumentLocation=s3obj, FeatureTypes=features)
    job_id = start["JobId"]
    log.info("[textract] started job=%s", job_id)

    if vision_callback:
        def _run_vision_chain():  # pragma: no cover - thread behavior
            try:
                vision_callback()
            except Exception as exc:
                log.warning("[vision-parallel] error: %s", exc)

        try:
            threading.Thread(target=_run_vision_chain, daemon=True).start()
            log.info("[vision-parallel] started Vision processing in background")
        except Exception as exc:
            log.warning("[vision-parallel] could not start: %s", exc)

    res = _textract_wait_and_fetch(job_id, client, log)
    write_json(ctx.textract_raw_json, res)
    log.info("[textract] result written → %s (status=%s)", ctx.textract_raw_json, res.get("status"))
    if res.get("status") not in ("SUCCEEDED", "PARTIAL_SUCCESS"):
        raise RuntimeError(f"Textract failed: {res.get('status')}")
    ctx.save_status("textract")
    log.info("[textract] done → %s", ctx.textract_raw_json)
