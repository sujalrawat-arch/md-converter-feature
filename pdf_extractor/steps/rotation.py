from __future__ import annotations

import math
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import cv2
import fitz
import numpy as np
from tqdm import tqdm


def _detect_rotation_angle_gray(img_gray: np.ndarray) -> float:
    edges = cv2.Canny(img_gray, 50, 150)
    lines = cv2.HoughLines(edges, 1, math.pi / 180.0, threshold=200)
    if lines is None:
        return 0.0
    angles = []
    for rho, theta in lines[:, 0]:
        angle = (theta * 180.0 / math.pi) - 90.0
        if -45 <= angle <= 45:
            angles.append(angle)
    return float(np.median(angles)) if angles else 0.0


def step_02_rotation(ctx, log):
    if ctx.last_step in ("rotation", "textract"):
        log.info("[rotation] skipping (already past)")
        return

    doc = fitz.open(ctx.local_pdf)
    rotations = {}
    log.info("[rotation] start: checking up to %d pages", ctx.page_count)

    def check_rot(i):
        pix = doc.load_page(i).get_pixmap(dpi=120, colorspace=fitz.csGRAY, alpha=False)
        img = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.h, pix.w)
        angle = _detect_rotation_angle_gray(img)
        if angle > 30:
            return (i, 270)
        if angle < -30:
            return (i, 90)
        return (i, 0)

    with ThreadPoolExecutor(max_workers=min(8, os.cpu_count() or 4)) as ex:
        futures = [ex.submit(check_rot, i) for i in range(ctx.page_count)]
        for future in tqdm(as_completed(futures), total=ctx.page_count, desc="[rotation]"):
            i, rot = future.result()
            if rot:
                rotations[i] = rot

    for i, rot in rotations.items():
        page = doc.load_page(i)
        page.set_rotation((page.rotation + rot) % 360)
    try:
        select_pages = list(range(0, ctx.page_count))
        if select_pages:
            doc.select(select_pages)
    except Exception as exc:
        log.warning("[rotation] select() warning: %s", exc)

    log.warning("[rotation] TEMP: skipping normalized PDF save; using source PDF as normalized")
    try:
        doc.close()
    except Exception:
        pass
    try:
        ctx.norm_pdf = ctx.local_pdf
        size_mb = os.path.getsize(ctx.norm_pdf) / 1e6
        log.info("[rotation] TEMP: norm_pdf → %s (size=%.2f MB)", ctx.norm_pdf, size_mb)
    except Exception as exc:
        log.warning("[rotation] TEMP: failed to point norm_pdf to local_pdf: %s", exc)
    ctx.save_status("rotation", {"rotated_pages": sorted(rotations.keys())})
    log.info("[rotation] rotated=%d  → %s", len(rotations), ctx.norm_pdf)
