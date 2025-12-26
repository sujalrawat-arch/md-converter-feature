from __future__ import annotations

import asyncio
import base64
import os
import io
import time
from typing import List, Dict, Any, Union


import fitz  # PyMuPDF
from PIL import Image

# Import log_llm_credits for logging LLM token usage
try:
    from Prod.helper import log_llm_credits
except ImportError:
    log_llm_credits = None

try:
    from openai import AsyncOpenAI
except Exception:  # pragma: no cover
    AsyncOpenAI = None

from ..config import OPENAI_API_KEY
from ..utils import write_json, read_json


def _group_blocks_by_page(textract_data: Union[dict, list]) -> Dict[int, List[dict]]:
    """
    Normalize Textract output that may be:
    1. A dictionary with 'pages' (document analysis wrapper)
    2. A dictionary with 'Blocks' (standard AWS SDK response)
    3. A direct list of Blocks (flattened or simplified output)
    """
    pages: Dict[int, List[dict]] = {}
    
    # CASE 1: Direct List of Blocks
    if isinstance(textract_data, list):
        blocks = textract_data
        for block in blocks:
            # Default to Page 1 if 'Page' key is missing
            b_page = int(block.get("Page", 1) or 1)
            pages.setdefault(b_page, []).append(block)
        return pages

    # CASE 2 & 3: Dictionary Wrapper
    if isinstance(textract_data, dict):
        # Sub-case: 'pages' key (custom nested structure)
        if textract_data.get("pages"):
            pages_array = textract_data.get("pages", [])
            for item in pages_array:
                # Check if this is a block with a Page number (flat structure)
                if item.get("BlockType") and "Page" in item:
                    b_page = int(item.get("Page") or 1)
                    pages.setdefault(b_page, []).append(item)
                # Or if it's a page container with nested blocks (nested structure)
                elif item.get("BlockType") == "PAGE":
                    page_num = int(item.get("Page", 1) or 1)
                    # Add the PAGE block itself
                    pages.setdefault(page_num, []).append(item)
                    # Also check for blocks nested under this page
                    blocks = item.get("Blocks", item.get("blocks", [])) or []
                    for block in blocks:
                        b_page = int(block.get("Page", page_num) or page_num)
                        pages.setdefault(b_page, []).append(block)
        
        # Sub-case: Standard AWS 'Blocks' key at root
        elif textract_data.get("Blocks"):
            for block in textract_data.get("Blocks", []) or []:
                b_page = int(block.get("Page", 1) or 1)
                pages.setdefault(b_page, []).append(block)
                
        # Sub-case: Maybe the dict *is* a single block? (Rare, but safety first)
        elif textract_data.get("BlockType"):
            b_page = int(textract_data.get("Page", 1) or 1)
            pages.setdefault(b_page, []).append(textract_data)

    return pages


def _encode_image_bytes(img_bytes: bytes) -> str:
    """Encodes raw image bytes to base64 string for GPT-4o."""
    return f"data:image/png;base64,{base64.b64encode(img_bytes).decode('utf-8')}"


def _get_figure_blocks(blocks_by_page: Dict[int, List[dict]], page_num: int, log=None) -> List[dict]:
    """
    Extract FIGURE/DIAGRAM blocks.
    Enhanced to catch standard Layout Analysis types.
    """
    figures = []
    figure_types_detected = {}
    
    # Target BlockTypes that represent visuals
    # LAYOUT_FIGURE: Standard Textract Layout Analysis for charts/images
    # FIGURE/DIAGRAM: Legacy or other OCR engine outputs
    target_types = {
        "FIGURE", "DIAGRAM", "LAYOUT_FIGURE"
    }
    
    for block in blocks_by_page.get(page_num, []):
        block_type = block.get("BlockType", "UNKNOWN").upper()
        
        if block_type in target_types:
            figures.append(block)
            figure_types_detected[block_type] = figure_types_detected.get(block_type, 0) + 1
        
        # Optional: Check generic LAYOUT_TEXT blocks if they are unusually large or explicit image containers?
        # For now, we stick to explicit figure types to avoid cropping text paragraphs as images.
    
    if log and figures:
        log.debug(f"[vision-blocks] Page {page_num}: Found {len(figures)} figure blocks: {figure_types_detected}")
    
    return figures


def _is_relevant_figure_block(block: dict, page_num: int = None, log=None) -> bool:
    """
    Heuristic filter to skip logos/icons.
    """
    bbox = block.get("Geometry", {}).get("BoundingBox", {})
    w, h = bbox.get("Width", 0), bbox.get("Height", 0)
    area = w * h
    block_id = block.get("Id", "unknown")
    block_type = block.get("BlockType", "UNKNOWN")

    # Relaxed Thresholds:
    # 1. Very small icons (social media icons, bullets) -> Skip
    if area < 0.005:  # Was 0.01 (1% of page). Now 0.5%
        if log:
            log.debug(f"[vision-filter] Page {page_num} Block {block_id}: REJECTED - too small (area={area:.4f})")
        return False
    
    # 2. Extreme aspect ratios (lines or dividers parsed as figures) -> Skip
    # But allow wide headers or tall sidebars if they are explicitly marked as figures.
    if w < 0.02 or h < 0.02: # Was 0.08. Relaxed to capture thin charts.
        if log:
            log.debug(f"[vision-filter] Page {page_num} Block {block_id}: REJECTED - too thin (w={w:.3f}, h={h:.3f})")
        return False

    if log:
        log.debug(f"[vision-filter] Page {page_num} Block {block_id} ({block_type}): ACCEPTED")
    return True


def step_05_render_for_vision(ctx, log):
    """
    (Unchanged) Renders full pages for context or fallback.
    """
    if ctx.last_step in ("vision_rendered", "vision", "unify", "done"):
        log.info("[vision-render] skipping (already processed)")
        return
    
    ctx.save_status("vision_rendered", {"vision_rendered": True})


async def _vision_analyze_figure(async_client, semaphore, img_bytes, fig_meta, retries=2, ctx=None, log=None):
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

                # --- LLM Token Logging ---
                # Instead of logging here, collect usage for aggregation in step_06_vision_async
                usage = getattr(resp, 'usage', None)
                tokens_used = None
                input_size = None
                output_size = None
                if usage:
                    tokens_used = usage.get('total_tokens') or usage.get('total', None)
                    input_size = usage.get('prompt_tokens') or usage.get('prompt', None)
                    output_size = usage.get('completion_tokens') or usage.get('completion', None)
                # Attach usage info to result for aggregation
                llm_usage = {
                    'tokens_used': tokens_used or 0,
                    'input_size': input_size or 0,
                    'output_size': output_size or 0
                }
                return {
                    "page": fig_meta["page"],
                    "bbox": fig_meta["bbox"],
                    "block_id": fig_meta["id"],
                    "image_path": fig_meta.get("image_path"),
                    "analysis": txt,
                    "ok": True,
                    "llm_usage": llm_usage
                }

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
        log.warning("[vision] Textract JSON not ready yet, waiting up to 120s...")
        deadline = time.time() + 120
        while time.time() < deadline:
            textract_json = read_json(ctx.textract_raw_json, {})
            if textract_json:
                log.info("[vision] Textract JSON loaded after wait")
                break
            time.sleep(1)

    if not textract_json:
        log.warning("[vision] No textract json found after wait; skipping vision extraction.")
        return


    async def run():
        async_client = AsyncOpenAI(api_key=OPENAI_API_KEY)
        semaphore = asyncio.Semaphore(min(5, (os.cpu_count() or 4)))

        blocks_by_page = _group_blocks_by_page(textract_json)
        total_blocks = sum(len(b) for b in blocks_by_page.values())
        log.info(f"[vision] Textract blocks grouped into {len(blocks_by_page)} pages (Total blocks: {total_blocks})")
        block_type_dist = {}
        for page_blocks in blocks_by_page.values():
            for block in page_blocks:
                bt = block.get("BlockType", "UNKNOWN")
                block_type_dist[bt] = block_type_dist.get(bt, 0) + 1
        log.info(f"[vision] Block type distribution: {block_type_dist}")
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
        log.info(f"[vision] Scanning {len(pages_to_scan)} pages for figures")
        total_figures = 0
        for page_num in pages_to_scan:
            page_blocks = blocks_by_page.get(page_num, [])
            page_text = " ".join(
                b.get("Text", "") for b in page_blocks if b.get("BlockType") == "LINE"
            ).lower()
            fig_blocks = _get_figure_blocks(blocks_by_page, page_num, log)
            if not fig_blocks and any(k in page_text for k in ("flow chart", "flowchart", "process flow")):
                log.info(f"[vision] Page {page_num}: Text mentions flowchart, adding full page for analysis")
                fig_blocks = [
                    {
                        "BlockType": "FLOWCHART_FALLBACK",
                        "Geometry": {"BoundingBox": {"Left": 0.0, "Top": 0.0, "Width": 1.0, "Height": 1.0}},
                        "Id": f"flowchart_page_{page_num}",
                    }
                ]
            initial_count = len(fig_blocks)
            fig_blocks = [b for b in fig_blocks if _is_relevant_figure_block(b, page_num, log)]
            filtered_out = initial_count - len(fig_blocks)
            if filtered_out > 0:
                log.debug(f"[vision] Page {page_num}: Filtered out {filtered_out} blocks")
            if not fig_blocks:
                continue
            log.info(f"[vision] Page {page_num}: Processing {len(fig_blocks)} figures")
            try:
                page_obj = doc.load_page(page_num - 1)
            except ValueError:
                log.error(f"[vision] Page {page_num} does not exist in PDF. Skipping.")
                continue
            w, h = page_obj.rect.width, page_obj.rect.height
            for block in fig_blocks:
                bbox = (block.get("Geometry", {}) or {}).get("BoundingBox", {}) or {}
                if not bbox:
                    log.warning(f"[vision] Block {block.get('Id')} missing geometry. Skipping.")
                    continue
                rect = fitz.Rect(
                    bbox.get("Left", 0.0) * w,
                    bbox.get("Top", 0.0) * h,
                    (bbox.get("Left", 0.0) + bbox.get("Width", 1.0)) * w,
                    (bbox.get("Top", 0.0) + bbox.get("Height", 1.0)) * h
                )
                pix = page_obj.get_pixmap(clip=rect, dpi=200, colorspace=fitz.csRGB)
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
                tasks.append(_vision_analyze_figure(async_client, semaphore, img_bytes, meta, ctx=ctx, log=log))
                total_figures += 1
        doc.close()
        if not tasks:
            log.info("[vision] No figures found to analyze across all pages.")
            return {"figures": [], "count": 0}
        log.info(f"[vision] Sending {total_figures} figures to GPT-4o for analysis...")
        results = await asyncio.gather(*tasks)
        ok_results = [r for r in results if r.get("ok")]
        failed_results = [r for r in results if not r.get("ok")]
        # Aggregate LLM usage
        agg_tokens_used = 0
        agg_input_size = 0
        agg_output_size = 0
        for r in results:
            if isinstance(r, dict) and 'llm_usage' in r and r['llm_usage']:
                usage = r['llm_usage']
                agg_tokens_used += usage.get('tokens_used', 0) or 0
                agg_input_size += usage.get('input_size', 0) or 0
                agg_output_size += usage.get('output_size', 0) or 0
        # Log a single credit entry for the whole vision step
        if log_llm_credits and ctx and hasattr(ctx, 'db_session'):
            try:
                log.info(f"[vision] Logging total LLM credits for vision step: tokens={agg_tokens_used}, input={agg_input_size}, output={agg_output_size}")
                task_id = log_llm_credits(
                    db=ctx.db_session,
                    user_id=getattr(ctx, 'user_id', 'unknown'),
                    customer_id=getattr(ctx, 'customer_id', 'unknown'),
                    project_id=getattr(ctx, 'project_id', 'unknown'),
                    tenant_id=getattr(ctx, 'tenant_id', 'unknown'),
                    task_type='vision-figure',
                    task_date_time=datetime.now(),
                    input_size=agg_input_size,
                    output_size=agg_output_size,
                    tokens_used=agg_tokens_used,
                    model_used='gpt-4o',
                )
                if not task_id:
                    log.error("[vision] log_llm_credits returned None (failed to log credits)")
                else:
                    log.info(f"[vision] log_llm_credits returned task_id: {task_id}")
            except Exception as e:
                log.error(f"[vision] Exception in log_llm_credits: {e}")
        if failed_results:
            log.warning(f"[vision] {len(failed_results)} figures failed analysis")
        log.info(f"[vision] Analysis complete: {len(ok_results)}/{total_figures} succeeded")
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
    log.info(f"[vision] Step complete - Processed {result.get('count', 0)} figures.")