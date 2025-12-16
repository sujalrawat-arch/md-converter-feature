from __future__ import annotations

import asyncio
import base64
import os
import io
import time
from typing import List, Dict, Any

import fitz  # PyMuPDF
from PIL import Image

try:
    from openai import AsyncOpenAI
except Exception:  # pragma: no cover
    AsyncOpenAI = None

from ..config import OPENAI_API_KEY
from ..utils import write_json, read_json


def _group_blocks_by_page(textract_data: dict) -> Dict[int, List[dict]]:
    """Normalize Textract output that may be either nested under `pages` or flat `Blocks`."""
    pages: Dict[int, List[dict]] = {}

    if textract_data.get("pages"):
        for idx, page in enumerate(textract_data.get("pages", []), start=1):
            # Textract may not place a Page number at the page node; default to its index.
            page_num = int(page.get("Page", page.get("page", idx)) or idx)
            for block in page.get("Blocks", []) or []:
                b_page = int(block.get("Page", page_num) or page_num)
                pages.setdefault(b_page, []).append(block)
    elif textract_data.get("Blocks"):
        for block in textract_data.get("Blocks", []) or []:
            b_page = int(block.get("Page", 1) or 1)
            pages.setdefault(b_page, []).append(block)

    return pages


def _encode_image_bytes(img_bytes: bytes) -> str:
    """Encodes raw image bytes to base64 string."""
    # CHANGED: Use PNG MIME type instead of WEBP
    return f"data:image/png;base64,{base64.b64encode(img_bytes).decode('utf-8')}"


def _get_figure_blocks(blocks_by_page: Dict[int, List[dict]], page_num: int) -> List[dict]:
    """Extract FIGURE/DIAGRAM blocks already grouped by page."""
    figures = []
    for block in blocks_by_page.get(page_num, []):
        if block.get("BlockType") in ["FIGURE", "DIAGRAM", "LAYOUT_FIGURE"]:
            figures.append(block)
    return figures


def _is_relevant_figure_block(block: dict) -> bool:
    """Heuristic filter to skip logos/icons. Keeps medium/large charts/flows."""
    bbox = block.get("Geometry", {}).get("BoundingBox", {})
    w, h = bbox.get("Width", 0), bbox.get("Height", 0)
    area = w * h

    # Reject tiny or thin elements (most logos/icons).
    if area < 0.01:
        return False
    if w < 0.08 or h < 0.08:
        return False

    # Skip very top-left header-like items (common logo spot) if also small-ish.
    if bbox.get("Top", 1) < 0.08 and h < 0.15:
        return False

    return True


def step_05_render_for_vision(ctx, log):
    """
    (Unchanged) Renders full pages for context or fallback.
    """
    if ctx.last_step in ("vision_rendered", "vision", "unify", "done"):
        log.info("[vision-render] skipping (already processed)")
        return
    
    ctx.save_status("vision_rendered", {"vision_rendered": True})


async def _vision_analyze_figure(async_client, semaphore, img_bytes, fig_meta, retries=2):
    """
    Analyzes a single cropped figure (Chart/Graph/Flowchart).
    """
    prompt = """
    Analyze this technical image extracted from a document.
    
    1. If it is a **Chart/Graph**: Extract the data into a **CSV format** and provide a brief insight.
    2. If it is a **Flowchart**: Extract the logic into **Mermaid.js** syntax and summarize the process steps.
    3. If it is a **Table** (saved as image): Transcribe it to Markdown.
    4. If it is a **Generic Image**: Describe it concisely.
    
    **Output format:**
    [TYPE]: (CHART | FLOWCHART | IMAGE)
    [SUMMARY]: (Brief description)
    [DATA]: (CSV content or Mermaid code or Markdown)
    """

    for attempt in range(retries + 1):
        async with semaphore:
            try:
                content = [
                    {"type": "text", "text": prompt},
                    {"type": "image_url", "image_url": {"url": _encode_image_bytes(img_bytes)}},
                ]
                resp = await async_client.chat.completions.create(
                    model="gpt-4o", 
                    messages=[{"role": "user", "content": content}],
                    temperature=0.0,
                    max_tokens=1000,
                )
                txt = resp.choices[0].message.content
                return {
                    "page": fig_meta["page"],
                    "bbox": fig_meta["bbox"],
                    "block_id": fig_meta["id"],
                    "image_path": fig_meta.get("image_path"),
                    "analysis": txt,
                    "ok": True
                }
            except Exception as exc:
                if attempt < retries:
                    await asyncio.sleep(2 * (attempt + 1))
                    continue
                return {
                    "error": str(exc),
                    "ok": False,
                    "block_id": fig_meta["id"],
                    "image_path": fig_meta.get("image_path"),
                }


def step_06_vision_async(ctx, log):
    if ctx.last_step == "vision":
        log.info("[vision] skipping (already processed)")
        return
    
    if not OPENAI_API_KEY or AsyncOpenAI is None:
        log.warning("[vision] skipping (no key)")
        write_json(ctx.vision_json, {"skipped": True})
        ctx.save_status("vision")
        return

    # 1. Load Textract Data
    textract_json = read_json(ctx.textract_raw_json, {})
    
    # Retry logic if Textract JSON isn't ready yet (race condition)
    if not textract_json:
        deadline = time.time() + 120
        while time.time() < deadline:
            textract_json = read_json(ctx.textract_raw_json, {})
            if textract_json:
                break
            time.sleep(1)

    if not textract_json:
        log.warning("[vision] No textract json found after wait; skipping surgical extraction.")
        return

    async def run():
        async_client = AsyncOpenAI(api_key=OPENAI_API_KEY)
        semaphore = asyncio.Semaphore(min(5, (os.cpu_count() or 4)))

        blocks_by_page = _group_blocks_by_page(textract_json)

        # Clear any prior figure crops so the folder only contains the current run.
        os.makedirs(ctx.vision_dir, exist_ok=True)
        for name in os.listdir(ctx.vision_dir):
            if name.endswith(".png"):
                try:
                    os.remove(os.path.join(ctx.vision_dir, name))
                except OSError:
                    pass
        
        tasks = []
        doc = fitz.open(ctx.norm_pdf)
        
        pages_to_scan = sorted(set((ctx.image_pages or []) + (ctx.chart_pages or [])))
        if not pages_to_scan:
            pages_to_scan = range(1, ctx.page_count + 1)

        total_figures = 0
        
        for page_num in pages_to_scan:
            page_blocks = blocks_by_page.get(page_num, [])
            page_text = " ".join(
                b.get("Text", "") for b in page_blocks if b.get("BlockType") == "LINE"
            ).lower()

            fig_blocks = _get_figure_blocks(blocks_by_page, page_num)

            # Fallback: if no figure blocks but page mentions a flow chart, send full page to vision.
            if not fig_blocks and any(k in page_text for k in ("flow chart", "flowchart", "process flow")):
                fig_blocks = [
                    {
                        "BlockType": "FLOWCHART_FALLBACK",
                        "Geometry": {"BoundingBox": {"Left": 0.0, "Top": 0.0, "Width": 1.0, "Height": 1.0}},
                        "Id": f"flowchart_page_{page_num}",
                    }
                ]
            
            # Filter to likely charts/flows; drop logos/icons.
            fig_blocks = [b for b in fig_blocks if _is_relevant_figure_block(b)]

            if not fig_blocks:
                continue

            # Load Page (0-indexed)
            page_obj = doc.load_page(page_num - 1)
            w, h = page_obj.rect.width, page_obj.rect.height

            for block in fig_blocks:
                bbox = (block.get("Geometry", {}) or {}).get("BoundingBox", {}) or {}
                
                # Convert normalized bbox to PDF coordinates
                rect = fitz.Rect(
                    bbox.get("Left", 0.0) * w,
                    bbox.get("Top", 0.0) * h,
                    (bbox.get("Left", 0.0) + bbox.get("Width", 1.0)) * w,
                    (bbox.get("Top", 0.0) + bbox.get("Height", 1.0)) * h
                )
                
                pix = page_obj.get_pixmap(clip=rect, dpi=200, colorspace=fitz.csRGB)
                
                # CHANGED: Use 'png' instead of 'webp' to fix ValueError
                img_bytes = pix.tobytes("png") 

                figure_idx = total_figures + 1
                img_filename = f"page_{page_num:04d}_figure_{figure_idx:04d}.png"
                img_path = os.path.join(ctx.vision_dir, img_filename)
                with open(img_path, "wb") as fh:
                    fh.write(img_bytes)
                
                meta = {
                    "page": page_num,
                    "bbox": float(bbox.get("Top", 0.0)),
                    "id": block.get("Id"),
                    "image_path": img_path,
                }
                
                tasks.append(_vision_analyze_figure(async_client, semaphore, img_bytes, meta))
                total_figures += 1
                
        doc.close()

        if not tasks:
            log.info("[vision] No figures found to analyze.")
            return {"figures": [], "count": 0}

        log.info("[vision] Sending %d cropped figures to GPT-4o...", total_figures)
        results = await asyncio.gather(*tasks)
        
        ok_results = [r for r in results if r.get("ok")]
        return {"figures": ok_results, "count": total_figures}

    result = asyncio.run(run()) or {"figures": [], "count": 0}
    
    write_json(ctx.vision_json, result)
    
    ctx.save_status(
        "vision",
        {
            "vision_figures_processed": result.get("count", 0),
            "vision_done": True,
        },
    )
    log.info("[vision] Processed %d figures.", result.get("count", 0))