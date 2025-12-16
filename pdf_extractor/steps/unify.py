from __future__ import annotations

import os
import re
from typing import Dict, List, Any

from ..config import MAX_PAGES, USE_VISION
from ..utils import read_json


# =============================================================================
#  SECTION 1: TEXTRACT TABLE PARSING (FAST & ROBUST)
# =============================================================================

def get_rows_columns_map(table_block: dict, blocks_map: Dict[str, dict]) -> Dict[int, Dict[int, str]]:
    """
    Directly maps Textract CELL blocks to a row/col dictionary.
    Much faster than recursive traversal.
    """
    rows: Dict[int, Dict[int, str]] = {}
    for relationship in table_block.get("Relationships", []) or []:
        if relationship.get("Type") != "CHILD":
            continue
        for child_id in relationship.get("Ids", []) or []:
            cell = blocks_map.get(child_id)
            if not cell or cell.get("BlockType") != "CELL":
                continue
            row_index = cell.get("RowIndex")
            col_index = cell.get("ColumnIndex")
            if not row_index or not col_index:
                continue
            
            # Extract text from the cell
            row = rows.setdefault(row_index, {})
            cell_tokens: List[str] = []
            for cell_rel in cell.get("Relationships", []) or []:
                if cell_rel.get("Type") != "CHILD":
                    continue
                for word_id in cell_rel.get("Ids", []) or []:
                    word = blocks_map.get(word_id)
                    if word and word.get("BlockType") == "WORD" and word.get("Text"):
                        cell_tokens.append(word["Text"].strip())
            row[col_index] = " ".join(token for token in cell_tokens if token).strip()
    return rows


def _rows_to_grid(rows: Dict[int, Dict[int, str]]) -> List[List[str]]:
    """Flattens the dictionary map into a standard 2D list (grid)."""
    if not rows:
        return []
    sorted_row_indices = sorted(rows.keys())
    max_cols = max((max(row.keys(), default=0) for row in rows.values()), default=0)
    if max_cols == 0:
        return []
    grid: List[List[str]] = []
    for row_idx in sorted_row_indices:
        row_data = rows.get(row_idx, {})
        # Fill empty cells to ensure a rectangular grid
        grid.append([row_data.get(c, "") for c in range(1, max_cols + 1)])
    return grid


def table_to_markdown(grid: List[List[str]], headers: List[str] = None) -> str:
    """Converts a grid to a clean Markdown table."""
    if not grid and not headers:
        return ""

    if headers:
        display_headers = headers
        data_rows = grid
    else:
        display_headers = grid[0] if grid else []
        data_rows = grid[1:] if len(grid) > 1 else []

    data_width = max((len(row) for row in data_rows), default=0)
    cols = max(len(display_headers), data_width)
    if cols == 0:
        return ""

    # Normalize headers to ensure uniqueness is not strictly required but helpful
    header_candidates = list(display_headers)
    if len(header_candidates) < cols:
        header_candidates.extend([""] * (cols - len(header_candidates)))
    
    normalized_headers = [
        (str(cell).strip() if isinstance(cell, str) else str(cell)).strip() or f"Col{i+1}"
        for i, cell in enumerate(header_candidates)
    ]

    md_lines = [
        "| " + " | ".join(normalized_headers) + " |",
        "| " + " | ".join(["---"] * len(normalized_headers)) + " |",
    ]

    for row in data_rows:
        padded = row + [""] * (cols - len(row))
        md_lines.append("| " + " | ".join(padded) + " |")
        
    return "\n".join(md_lines)


# =============================================================================
#  SECTION 2: SMART MERGE LOGIC (THE "UPDATE" YOU WANTED)
# =============================================================================

def _normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip().lower()

def _get_column_signature(column_data: List[str]) -> str:
    """Heuristic to check column data types (numeric vs alpha)."""
    sample = [x for x in column_data if x.strip()][:5]
    if not sample: return "empty"
    if all(re.match(r'^[\d\.\,\-]+$', s) for s in sample): return "numeric"
    if any(re.search(r'[a-zA-Z]', s) for s in sample): return "alpha"
    return "mixed"

def _grid_similarity(header_a: List[str], header_b: List[str]) -> float:
    """Jaccard similarity for comparing headers."""
    sa = set(_normalize_text(x) for x in header_a)
    sb = set(_normalize_text(x) for x in header_b)
    if not sa or not sb: return 0.0
    return len(sa & sb) / len(sa | sb)

def _transpose_grid(grid: List[List[str]]) -> List[List[str]]:
    """Pivots a grid (rows become columns)."""
    if not grid: return []
    return [list(x) for x in zip(*grid)]

def merge_tables_contextually(raw_tables: List[dict]) -> List[dict]:
    """
    Smart merging: 
    - Checks strict column counts
    - Checks for transposed (pivoted) tables
    - Checks for reversed column order
    """
    if not raw_tables: return []

    raw_tables.sort(key=lambda x: x.get("page", 0))

    merged = []
    current = None

    for next_t in raw_tables:
        next_grid = next_t.get("grid", [])
        if not next_grid: continue
            
        if current is None:
            current = {
                "type": "table",
                "page_start": next_t["page"],
                "page_end": next_t["page"],
                "header": next_grid[0],
                "grid": next_grid[1:], 
                "col_count": len(next_grid[0]),
                "bbox": next_t.get("bbox")
            }
            continue

        page_diff = next_t["page"] - current["page_end"]
        is_continuous = 0 <= page_diff <= 1
        current_cols = current["col_count"]
        next_cols = len(next_grid[0])
        
        candidate_grid = None
        merge_action = "none"

        if is_continuous:
            if next_cols == current_cols:
                candidate_grid = next_grid
                merge_action = "direct"
            else:
                # Try Pivot
                pivoted = _transpose_grid(next_grid)
                if len(pivoted) > 0 and len(pivoted[0]) == current_cols:
                    candidate_grid = pivoted
                    merge_action = "transposed"
        
        if candidate_grid:
            is_header_repeated = False
            if merge_action == "direct":
                sim = _grid_similarity(current["header"], candidate_grid[0])
                if sim > 0.7:
                    is_header_repeated = True
            elif merge_action == "transposed":
                # Check for reversed columns logic
                curr_sig = _get_column_signature([r[0] for r in current["grid"][-5:]])
                cand_sig = _get_column_signature([r[0] for r in candidate_grid[:5]])
                curr_last_sig = _get_column_signature([r[-1] for r in current["grid"][-5:]])
                
                # If Column 0 of new grid looks like the LAST column of the old grid, reverse it
                if curr_sig != cand_sig and cand_sig == curr_last_sig:
                     candidate_grid = [row[::-1] for row in candidate_grid]

            if is_header_repeated:
                current["grid"].extend(candidate_grid[1:])
            else:
                current["grid"].extend(candidate_grid)
            
            current["page_end"] = next_t["page"]
            
        else:
            merged.append(current)
            current = {
                "type": "table",
                "page_start": next_t["page"],
                "page_end": next_t["page"],
                "header": next_grid[0],
                "grid": next_grid[1:],
                "col_count": len(next_grid[0]),
                "bbox": next_t.get("bbox")
            }

    if current:
        merged.append(current)

    return merged


# =============================================================================
#  SECTION 3: ORCHESTRATOR (TEXT + TABLES + CHARTS)
# =============================================================================

def process_aws_results_smart(blocks: List[dict]) -> List[dict]:
    """Combines Textract Text blocks with the Smart Table Merge logic."""
    if not blocks: return []
    
    blocks_map = {block.get("Id"): block for block in blocks if block.get("Id")}
    
    # 1. Extract & Merge Tables
    tables_raw = []
    table_word_ids = set()
    
    for b in blocks:
        if b.get("BlockType") == "TABLE":
            # Track words inside tables to prevent duplicate printing
            for rel in b.get("Relationships", []) or []:
                if rel["Type"] == "CHILD":
                    for cid in rel["Ids"]:
                        cell = blocks_map.get(cid)
                        if cell:
                            for cr in cell.get("Relationships", []) or []:
                                if cr["Type"] == "CHILD":
                                    table_word_ids.update(cr["Ids"])
            
            # Extract Grid
            rows = get_rows_columns_map(b, blocks_map)
            grid = _rows_to_grid(rows)
            if grid:
                bbox = (b.get("Geometry", {}) or {}).get("BoundingBox", {}) or {}
                tables_raw.append({
                    "page": int(b.get("Page", 1)),
                    "bbox": float(bbox.get("Top", 0.0)),
                    "grid": grid
                })

    final_tables = merge_tables_contextually(tables_raw)
    
    final_items = []
    for t in final_tables:
        final_items.append({
            "page": t["page_start"],
            "top": t["bbox"],
            "type": "table",
            "content": table_to_markdown(t["grid"], headers=t["header"])
        })
        
    # 2. Extract Lines (Text), skipping words that are inside tables
    lines = [b for b in blocks if b.get("BlockType") == "LINE" and (b.get("Text") or "").strip()]
    for line in lines:
        l_ids = []
        for rel in line.get("Relationships", []) or []:
            if rel["Type"] == "CHILD":
                l_ids.extend(rel["Ids"])
        
        # Optimization: Only check overlap if line has content
        if not l_ids: continue
        
        # Overlap Check
        matches = sum(1 for wid in l_ids if wid in table_word_ids)
        if matches > len(l_ids) * 0.9: 
            continue # Line is inside a table
            
        bbox = (line.get("Geometry", {}) or {}).get("BoundingBox", {}) or {}
        final_items.append({
            "page": int(line.get("Page", 1)),
            "top": float(bbox.get("Top", 0.0)),
            "type": "text",
            "content": line.get("Text", "").strip()
        })

    return final_items


# =============================================================================
#  SECTION 4: PIPELINE ENTRY POINT
# =============================================================================

def step_07_unify(ctx, log):
    textract_json = read_json(ctx.textract_raw_json, {}) or {}
    
    # 1. Flatten all blocks (handles multipage Textract JSON)
    blocks = []
    for p in textract_json.get("pages", []) or []:
        blocks.extend(p.get("Blocks", []) or [])

    # 2. Process Text & Tables (Using the Efficient Logic)
    processed_items = process_aws_results_smart(blocks)
    
    # 3. Integrate Vision Data (The Charts/Diagrams from Step 06)
    if USE_VISION:
        vision_json = read_json(ctx.vision_json, {})
        vision_figures = vision_json.get("figures", [])
        
        for fig in vision_figures:
            if not fig.get("ok"): 
                continue
            
            # Create a content block for the chart
            processed_items.append({
                "page": fig.get("page", 1),
                "top": fig.get("bbox", 0.0), # Uses the bbox to slot it in correctly
                "type": "chart_data",
                "content": f"\n\n### Figure Data (Page {fig.get('page')})\n{fig.get('analysis', '')}\n"
            })

    # 4. Global Sort: Page -> Vertical Position (Top)
    # This interleaves Text, Tables, and Charts perfectly
    processed_items.sort(key=lambda item: (item.get("page", 0), item.get("top", 0.0)))
    
    # 5. Generate Final Markdown
    md = [
        f"# File: {os.path.basename(ctx.local_pdf)}",
        f"- File ID: `{ctx.file_id}`",
        f"- Pages: {ctx.page_count}",
        "\n---\n"
    ]

    items_by_page: Dict[int, List[dict]] = {}
    for item in processed_items:
        items_by_page.setdefault(item.get("page", 1), []).append(item)

    all_pages = sorted(list(set(range(1, (ctx.page_count or 0) + 1)) | set(items_by_page.keys())))
    
    for page_num in all_pages:
        if page_num > MAX_PAGES: continue
        
        md.append(f"\n## Page {page_num}\n")
        
        for entry in items_by_page.get(page_num, []):
            c = entry.get("content", "").strip()
            if c:
                # Add extra newline buffer for tables and charts
                if entry.get("type") in ["table", "chart_data"]:
                    md.append("\n" + c + "\n")
                else:
                    md.append(c)

    with open(ctx.final_md, "w", encoding="utf-8") as fh:
        fh.write("\n".join(md))
    
    ctx.save_status("unify")
    log.info("[unify] wrote → %s", ctx.final_md)