from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass, field
from typing import List

import fitz

from .config import DATA_DIR, MAX_PAGES, OUTPUT_ROOT
from .utils import ensure_dir, now_ms, read_json, write_json


@dataclass
class JobCtx:
    file_id: str
    s3_path: str
    job_dir: str
    log_file: str
    status_file: str
    api_status_file: str
    source_path: str
    local_pdf: str
    norm_pdf: str
    pages_dir: str
    vision_dir: str
    textract_raw_json: str
    vision_json: str
    final_md: str
    last_step: str = ""
    page_count: int = 0
    image_pages: List[int] = field(default_factory=list)
    text_pages: List[int] = field(default_factory=list)
    chart_pages: List[int] = field(default_factory=list)

    @staticmethod
    def build(fid: str, s3_path: str) -> "JobCtx":
        job_dir = ensure_dir(os.path.join(OUTPUT_ROOT, fid))
        return JobCtx(
            file_id=fid,
            s3_path=s3_path,
            job_dir=job_dir,
            log_file=os.path.join(job_dir, "job.log"),
            status_file=os.path.join(job_dir, "status.json"),
            api_status_file=os.path.join(job_dir, "api_status.json"),
            source_path=os.path.join(DATA_DIR, f"{fid}.source"),
            local_pdf=os.path.join(DATA_DIR, f"{fid}.pdf"),
            norm_pdf=os.path.join(job_dir, f"{fid}.normalized.pdf"),
            pages_dir=ensure_dir(os.path.join(job_dir, "pages")),
            vision_dir=ensure_dir(os.path.join(job_dir, "vision_imgs")),
            textract_raw_json=os.path.join(job_dir, "textract_raw.json"),
            vision_json=os.path.join(job_dir, "vision_results.json"),
            final_md=os.path.join(job_dir, f"{fid}.pdf.md"),
        )

    def load_status(self) -> None:
        state = read_json(self.status_file, {}) or {}
        self.last_step = state.get("last_step", "")
        saved_page_count = state.get("page_count", 0)
        
        # If no saved page count, read actual page count from PDF
        if saved_page_count > 0:
            self.page_count = min(saved_page_count, MAX_PAGES)
        elif os.path.exists(self.local_pdf):
            try:
                doc = fitz.open(self.local_pdf)
                self.page_count = min(doc.page_count, MAX_PAGES)
                doc.close()
            except Exception:
                self.page_count = MAX_PAGES
        else:
            self.page_count = MAX_PAGES
        
        self.image_pages = [p for p in state.get("image_pages", []) if p < MAX_PAGES]
        self.text_pages = [p for p in state.get("text_pages", []) if p < MAX_PAGES]
        self.chart_pages = [p for p in state.get("chart_pages", []) if p < MAX_PAGES]
        self.source_path = state.get("source_path", self.source_path)
        self.local_pdf = state.get("local_pdf_path", self.local_pdf)
        self.norm_pdf = state.get("norm_pdf_path", self.norm_pdf)

    def save_status(self, step: str, extra: dict | None = None) -> None:
        state = read_json(self.status_file, {}) or {}
        state.update(
            {
                "last_step": step,
                "page_count": self.page_count,
                "image_pages": self.image_pages,
                "text_pages": self.text_pages,
                "chart_pages": self.chart_pages,
                "source_path": self.source_path,
                "local_pdf_path": self.local_pdf,
                "norm_pdf_path": self.norm_pdf,
                "ts": now_ms(),
            }
        )
        if extra:
            state.update(extra)
        write_json(self.status_file, state)
        self.last_step = step


def setup_logger(ctx: JobCtx) -> logging.Logger:
    logger = logging.getLogger(ctx.file_id)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fh = logging.FileHandler(ctx.log_file, encoding="utf-8")
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s: %(message)s"))

    logger.addHandler(fh)
    logger.addHandler(sh)
    return logger
