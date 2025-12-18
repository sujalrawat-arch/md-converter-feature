from __future__ import annotations
import io
import os
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pypdf import PdfReader, PdfWriter
from dotenv import load_dotenv

# Assuming these are your local imports
from ..aws_clients import textract_client, s3_client
from ..config import BUCKET_TEX, MAX_PAGES
from ..utils import read_json, write_json

load_dotenv()

# Env Variables
PAGES_PER_CHUNK = int(os.getenv("TEXTRACT_PAGES_PER_CHUNK", 5))
MAX_PARALLEL_JOBS = int(os.getenv("TEXTRACT_MAX_PARALLEL_JOBS", 5))
MAX_RETRIES = int(os.getenv("TEXTRACT_MAX_RETRIES", 3))
TEXTRACT_POLL_INTERVAL = int(os.getenv("TEXTRACT_POLL_INTERVAL", 5))

def _split_pdf_to_s3(bucket: str, pdf_key: str, pages_per_chunk: int) -> list[tuple[int, str]]:
    """Download PDF from S3, split into chunks, and upload back with unique IDs.
    Respects MAX_PAGES limit."""
    resp = s3_client().get_object(Bucket=bucket, Key=pdf_key)
    pdf_bytes = resp["Body"].read()
    pdf_reader = PdfReader(io.BytesIO(pdf_bytes))
    total_pages = min(len(pdf_reader.pages), MAX_PAGES)
    
    chunks = []
    run_id = str(uuid.uuid4())[:8] # Prevent filename collisions
    
    for start_page in range(0, total_pages, pages_per_chunk):
        end_page = min(start_page + pages_per_chunk, total_pages)
        chunk_idx = start_page // pages_per_chunk
        
        pdf_writer = PdfWriter()
        for page_num in range(start_page, end_page):
            pdf_writer.add_page(pdf_reader.pages[page_num])
        
        chunk_bytes = io.BytesIO()
        pdf_writer.write(chunk_bytes)
        chunk_bytes.seek(0)
        
        base_key = pdf_key.rsplit(".", 1)[0]
        chunk_key = f"tmp/{base_key}_{run_id}_chunk_{chunk_idx}.pdf"
        
        s3_client().put_object(Bucket=bucket, Key=chunk_key, Body=chunk_bytes.getvalue())
        chunks.append((chunk_idx, chunk_key))
    
    return chunks

def _textract_wait_and_fetch(job_id: str, client, log):
    """Poll Textract and handle full pagination of results."""
    max_attempts = 600 
    attempt = 0
    
    while attempt < max_attempts:
        resp = client.get_document_analysis(JobId=job_id)
        status = resp.get("JobStatus")
        
        if status == "SUCCEEDED":
            log.info(f"[textract-poll] Job {job_id} succeeded. Fetching all pages...")
            all_blocks = []
            next_token = None
            
            # Correct pagination logic
            while True:
                params = {'JobId': job_id}
                if next_token:
                    params['NextToken'] = next_token
                
                page_resp = client.get_document_analysis(**params)
                all_blocks.extend(page_resp.get("Blocks", []))
                
                next_token = page_resp.get("NextToken")
                if not next_token:
                    break
            
            return {"status": "SUCCEEDED", "pages": all_blocks}
            
        elif status == "FAILED":
            msg = resp.get('StatusMessage', 'unknown error')
            log.error(f"[textract-poll] Job {job_id} failed: {msg}")
            return {"status": "FAILED", "error": msg}
        
        attempt += 1
        log.debug(f"[textract-poll] Job {job_id} in progress...")
        time.sleep(TEXTRACT_POLL_INTERVAL)
    
    return {"status": "TIMEOUT"}

def _process_chunk_with_retry(idx, chunk_key, client, log, features):
    """Worker for a single chunk with exponential backoff."""
    attempts = 0
    while attempts < MAX_RETRIES:
        try:
            s3obj = {"S3Object": {"Bucket": BUCKET_TEX, "Name": chunk_key}}
            start = client.start_document_analysis(DocumentLocation=s3obj, FeatureTypes=features)
            
            result = _textract_wait_and_fetch(start["JobId"], client, log)
            
            if result["status"] == "SUCCEEDED":
                return idx, result["pages"]
            else:
                raise Exception(f"Textract job error: {result.get('error')}")

        except Exception as e:
            attempts += 1
            wait_time = 5 * attempts
            log.warning(f"[chunk-{idx}] Attempt {attempts} failed: {e}. Retrying in {wait_time}s...")
            time.sleep(wait_time)
            
    raise RuntimeError(f"Chunk {idx} failed after {MAX_RETRIES} attempts.")

def step_04_textract(ctx, log, vision_callback=None):
    """Main entry point to process the 72-page document."""
    if ctx.last_step == "textract":
        return

    # 1. Prepare Chunks
    status_data = read_json(ctx.status_file, {})
    key = status_data.get("textract_key")
    if not key:
        log.error("No textract_key found in status file.")
        return

    log.info(f"[textract] Splitting {key} into chunks...")
    chunks = _split_pdf_to_s3(BUCKET_TEX, key, PAGES_PER_CHUNK)
    
    # 2. Parallel Processing
    client = textract_client()
    features = ["TABLES", "FORMS", "LAYOUT"]
    results_with_index = []

    log.info(f"[textract] Starting {len(chunks)} parallel jobs...")
    with ThreadPoolExecutor(max_workers=MAX_PARALLEL_JOBS) as executor:
        future_to_idx = {
            executor.submit(_process_chunk_with_retry, idx, c_key, client, log, features): idx 
            for idx, c_key in chunks
        }
        
        for future in as_completed(future_to_idx):
            try:
                idx, pages = future.result()
                results_with_index.append((idx, pages))
            except Exception as exc:
                log.critical(f"[textract] Critical failure in chunk: {exc}")
                raise exc

    # 3. Merge and Sort - ADJUST PAGE NUMBERS PER CHUNK
    results_with_index.sort(key=lambda x: x[0])
    final_pages = []
    
    for chunk_idx, pages in results_with_index:
        # Calculate page offset: chunk 0 = pages 0-14, chunk 1 = pages 15-29, etc.
        page_offset = chunk_idx * PAGES_PER_CHUNK
        
        # Adjust page numbers in blocks to account for chunk offset
        for block in pages:
            if block.get("Page"):
                # Adjust the Page field by adding the offset
                block["Page"] = int(block.get("Page", 1)) + page_offset
        
        final_pages.extend(pages)
        log.info(f"[textract] Chunk {chunk_idx} adjusted pages by offset {page_offset}")

    # 4. Save and Cleanup
    write_json(ctx.textract_raw_json, {"status": "SUCCEEDED", "pages": final_pages})
    
    for _, chunk_key in chunks:
        try:
            s3_client().delete_object(Bucket=BUCKET_TEX, Key=chunk_key)
        except Exception as e:
            log.warning(f"Failed to delete temp chunk {chunk_key}: {e}")

    # 5. Handle Callback (Avoid Daemon if process is about to end)
    if vision_callback:
        log.info("[textract] Triggering vision callback...")
        t = threading.Thread(target=vision_callback)
        t.start()

    ctx.save_status("textract")
    log.info(f"[textract] Successfully merged {len(final_pages)} blocks from {len(chunks)} chunks.")