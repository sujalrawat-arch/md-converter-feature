from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from typing import Callable


DOC_EXTS = {".doc", ".docx"}
PPT_EXTS = {".ppt", ".pptx"}
TXT_EXTS = {".txt", ".md"}
IMG_EXTS = {".jpg", ".jpeg", ".png", ".bmp"}


def _run_soffice(input_path: str, output_dir: str, log_label: str) -> str:
    outdir = Path(output_dir)
    outdir.mkdir(parents=True, exist_ok=True)
    soffice_bin = os.environ.get("SOFFICE_PATH") or "soffice"
    cmd = [
        soffice_bin,
        "--headless",
        "--convert-to",
        "pdf",
        "--outdir",
        str(outdir),
        input_path,
    ]
    try:
        result = subprocess.run(cmd, capture_output=True)
    except FileNotFoundError as exc:
        raise RuntimeError(
            "LibreOffice 'soffice' binary not found. Install LibreOffice or set SOFFICE_PATH to the executable path."
        ) from exc
    if result.returncode != 0:
        raise RuntimeError(f"{log_label} conversion failed: {result.stderr.decode().strip()}")
    target = outdir / f"{Path(input_path).stem}.pdf"
    if not target.exists():
        raise RuntimeError(f"{log_label} conversion did not produce output at {target}")
    return str(target)


def convert_docx_to_pdf(input_path: str, output_dir: str) -> str:
    return _run_soffice(input_path, output_dir, "DOC/DOCX")


def convert_pptx_to_pdf(input_path: str, output_dir: str) -> str:
    return _run_soffice(input_path, output_dir, "PPT/PPTX")


def convert_txt_to_pdf(input_path: str, output_dir: str) -> str:
    from reportlab.lib.pagesizes import A4
    from reportlab.pdfbase import pdfmetrics
    from reportlab.pdfgen import canvas

    outdir = Path(output_dir)
    outdir.mkdir(parents=True, exist_ok=True)
    output_path = outdir / f"{Path(input_path).stem}.pdf"

    with open(input_path, "r", encoding="utf-8", errors="ignore") as fh:
        text = fh.read()

    c = canvas.Canvas(str(output_path), pagesize=A4)
    width, height = A4
    margin_left = margin_right = margin_top = margin_bottom = 50
    font_name = "Helvetica"
    font_size = 11
    line_height = 14
    max_width = width - margin_left - margin_right
    c.setFont(font_name, font_size)

    def wrap_line(line: str):
        words = line.split(" ")
        wrapped, current = [], ""
        for word in words:
            trial = word if not current else f"{current} {word}"
            if pdfmetrics.stringWidth(trial, font_name, font_size) <= max_width:
                current = trial
            else:
                wrapped.append(current)
                current = word
        if current:
            wrapped.append(current)
        return wrapped

    y = height - margin_top
    for logical_line in text.splitlines():
        if logical_line.strip() == "":
            y -= line_height
            if y <= margin_bottom:
                c.showPage()
                c.setFont(font_name, font_size)
                y = height - margin_top
            continue
        for physical_line in wrap_line(logical_line):
            c.drawString(margin_left, y, physical_line)
            y -= line_height
            if y <= margin_bottom:
                c.showPage()
                c.setFont(font_name, font_size)
                y = height - margin_top
    c.save()
    return str(output_path)


def convert_image_to_pdf(input_path: str, output_dir: str) -> str:
    from PIL import Image

    outdir = Path(output_dir)
    outdir.mkdir(parents=True, exist_ok=True)
    output_path = outdir / f"{Path(input_path).stem}.pdf"
    img = Image.open(input_path)
    img_converted = img.convert("RGB") if img.mode != "RGB" else img
    img_converted.save(str(output_path), "PDF", resolution=150)
    return str(output_path)


def _copy_pdf(input_path: str, output_dir: str) -> str:
    outdir = Path(output_dir)
    outdir.mkdir(parents=True, exist_ok=True)
    dest = outdir / Path(input_path).name
    if Path(input_path).resolve() == dest.resolve():
        return str(dest)
    shutil.copy2(input_path, dest)
    return str(dest)


def convert_to_pdf(input_path: str, output_dir: str) -> str:
    ext = Path(input_path).suffix.lower()
    if ext == ".pdf":
        return _copy_pdf(input_path, output_dir)
    if ext in DOC_EXTS:
        return convert_docx_to_pdf(input_path, output_dir)
    if ext in PPT_EXTS:
        return convert_pptx_to_pdf(input_path, output_dir)
    if ext in TXT_EXTS:
        return convert_txt_to_pdf(input_path, output_dir)
    if ext in IMG_EXTS:
        return convert_image_to_pdf(input_path, output_dir)
    raise ValueError(f"Unsupported extension for conversion: {ext}")
