from __future__ import annotations

import math
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import cv2
import fitz  # PyMuPDF
import numpy as np
from tqdm import tqdm


def _detect_rotation_angle_gray(img_gray: np.ndarray) -> float:
    edges = cv2.Canny(img_gray, 50, 150)
    lines = cv2.HoughLines(edges, 1, np.pi / 180.0, threshold=200)
    if lines is None:
        return 0.0
    angles = []
    for rho, theta in lines[:, 0]:
        ang = (theta * 180.0 / math.pi) - 90.0
        if -45 <= ang <= 45:
            angles.append(ang)
    return float(np.median(angles)) if angles else 0.0


def step_02_rotation(ctx: JobCtx, log):
    """Rotate skewed pages and save normalized PDF."""
    if ctx.last_step in ("rotation", "textract"):
        log.info("[rotation] skipping (already past)")
        return

    doc = fitz.open(ctx.local_pdf)
    total_pages = doc.page_count
    # MAX_PAGES should be defined in your config, e.g., 100
    ctx.page_count = total_pages 
    
    rotations = {}

    def check_rot(i):
        pix = doc.load_page(i).get_pixmap(dpi=120, colorspace=fitz.csGRAY, alpha=False)
        img = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.h, pix.w)
        a = _detect_rotation_angle_gray(img)
        if a > 30:  return (i, 270)
        if a < -30: return (i, 90)
        return (i, 0)

    with ThreadPoolExecutor(max_workers=min(8, os.cpu_count() or 4)) as ex:
        for fu in tqdm(as_completed([ex.submit(check_rot, i) for i in range(doc.page_count)]),
                       total=doc.page_count, desc="[rotation]"):
            i, rot = fu.result()
            if rot: rotations[i] = rot

    for i, rot in rotations.items():
        page = doc.load_page(i)
        page.set_rotation((page.rotation + rot) % 360)

    doc.save(ctx.norm_pdf, deflate=True, garbage=4)
    doc.close()
    ctx.save_status("rotation", {"rotated_pages": sorted(list(rotations.keys()))})
    log.info("[rotation] rotated=%d  → %s", len(rotations), ctx.norm_pdf)


# ---------------------------------------------------------------------
# Step 03 – Upload normalized PDF for Textract
# ---------------------------------------------------------------------

def step_03_upload_norm_for_textract(ctx: JobCtx, log):
    if ctx.last_step in ("upload_textract", "textract"):
        log.info("[upload_textract] skipping")
        return
    if not BUCKET_TEX:
        raise RuntimeError("AWS_BUCKET_PDF_READER_TEXTRACT not set.")
    key = f"{ctx.file_id}/{os.path.basename(ctx.norm_pdf)}"
    s3 = s3_client()
    s3.upload_file(ctx.norm_pdf, BUCKET_TEX, key)
    ctx.save_status("upload_textract", {"textract_key": key})
    log.info("[upload_textract] s3://%s/%s", BUCKET_TEX, key)