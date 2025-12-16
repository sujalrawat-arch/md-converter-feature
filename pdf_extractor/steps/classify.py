from __future__ import annotations

import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

import cv2
import fitz
import numpy as np
import pytesseract
from PIL import Image
from tqdm import tqdm

from ..config import MAX_PAGES


IMG_TEXT_RATIO_MAX = 0.07
IMG_TEXT_LEN_MAX = 450
IMG_WORD_COUNT_MAX = 120
IMG_VAR_MIN = 900
IMG_VAR_HIGH = 9000
TEXTLESS_LEN = 30
OCR_TRIGGER_LEN = 25
OCR_MIN_CHARS = 100
TEXT_FAILSAFE_LEN = 700
WORD_FAILSAFE_COUNT = 160


def _ocr_from_pixmap(pix) -> str:
    try:
        img = Image.frombytes("L", [pix.width, pix.height], pix.samples)
        return pytesseract.image_to_string(img).strip()
    except Exception:
        return ""


def step_01_classify(ctx, log):
    if ctx.last_step in ("classify", "rotation", "textract"):
        log.info("[classify] skipping (already past)")
        return

    doc = fitz.open(ctx.local_pdf)
    total_pages = doc.page_count
    ctx.page_count = min(total_pages, MAX_PAGES)
    log.info(
        "[classify] PDF has %d pages, processing first %d pages only",
        total_pages,
        ctx.page_count,
    )
    img_pages, txt_pages, chart_pages = [], [], []

    def classify_one(i: int):
        page = doc.load_page(i)
        text = page.get_text("text").strip()

        pix = page.get_pixmap(dpi=150, colorspace=fitz.csGRAY, alpha=False)
        img = np.frombuffer(pix.samples, dtype=np.uint8).reshape(pix.h, pix.w)
        fm = cv2.Laplacian(img, cv2.CV_64F).var()

        rect = page.rect
        area = rect.width * rect.height
        words = page.get_text("words")
        text_area = sum((w[2] - w[0]) * (w[3] - w[1]) for w in words)
        text_ratio = text_area / area if area > 0 else 0
        word_count = len(words)
        text_len = len(text)

        used_ocr = False
        if word_count == 0 and text_len <= OCR_TRIGGER_LEN:
            ocr_text = _ocr_from_pixmap(pix)
            if len(ocr_text) >= OCR_MIN_CHARS:
                text = ocr_text
                text_len = len(text)
                word_count = len(re.findall(r"\S+", text))
                used_ocr = True

        is_img = False
        is_chart = False
        reason = "text-rich"
        text_ratio = text_ratio or 0.0
        sparse_text = (
            text_ratio <= IMG_TEXT_RATIO_MAX
            and text_len <= IMG_TEXT_LEN_MAX
            and word_count <= IMG_WORD_COUNT_MAX
        )
        no_text_layer = word_count == 0 and text_len <= TEXTLESS_LEN

        if no_text_layer and fm >= IMG_VAR_MIN:
            reason = "C: no text layer"
            is_img = True
        elif sparse_text and IMG_VAR_MIN <= fm <= IMG_VAR_HIGH:
            reason = "B: sparse text"
            is_img = True
        elif text_ratio <= (IMG_TEXT_RATIO_MAX / 2) and fm >= (IMG_VAR_MIN * 1.3):
            reason = "A: dense visuals"
            is_img = True

        words = re.findall(r"\S+", text)
        numeric_tokens = [w for w in words if re.fullmatch(r"[\d,\.\-:%₹]+", w)]
        numeric_ratio = (len(numeric_tokens) / max(1, len(words))) if words else 0.0
        fy_hits = len(re.findall(r"\bFY\d{2}\b", text))
        pct_hits = text.count('%')
        digit_hits = sum(ch.isdigit() for ch in text)
        currency_hits = text.count('₹') + len(re.findall(r"\b(Crore|Million|Billion|Lakh|crore|million|billion|lakh)\b", text))
        cues = 0
        if fy_hits >= 3:
            cues += 1
        if pct_hits >= 6:
            cues += 1
        if currency_hits >= 2:
            cues += 1
        if digit_hits >= 200:
            cues += 1
        if numeric_ratio >= 0.28 and cues >= 2:
            is_chart = True
        else:
            reason = "D: text-rich"

        if used_ocr and text_len >= OCR_MIN_CHARS:
            is_img = False
            reason = "E: OCR text fallback"

        if (text_len >= TEXT_FAILSAFE_LEN or word_count >= WORD_FAILSAFE_COUNT) and not is_chart:
            if is_img:
                is_img = False
                reason = "F: failsafe volume"

        print(
            f"[classify] page={i} → {'image' if is_img else 'text'} "
            f"(var={fm:.1f}, text_len={text_len}, text_ratio={text_ratio:.4f}, reason={reason}"
            f"{', ocr' if used_ocr else ''})"
        )
        return (i, is_img, is_chart)

    with ThreadPoolExecutor(max_workers=min(8, os.cpu_count() or 4)) as ex:
        futures = [ex.submit(classify_one, i) for i in range(ctx.page_count)]
        for future in tqdm(as_completed(futures), total=ctx.page_count, desc="[classify]"):
            i, is_img, is_chart = future.result()
            (img_pages if is_img else txt_pages).append(i)
            if is_chart and i not in img_pages:
                chart_pages.append(i)

    img_pages.sort()
    txt_pages.sort()
    ctx.image_pages, ctx.text_pages = img_pages, txt_pages
    ctx.chart_pages = sorted(set(chart_pages))
    ctx.save_status("classify")
    log.info(
        "[classify] total=%d  image=%d  chart_like=%d  text=%d",
        ctx.page_count,
        len(img_pages),
        len(ctx.chart_pages),
        len(txt_pages),
    )
