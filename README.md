# PDF Extractor Pipeline

This repository contains a modular, resumable pipeline that ingests arbitrary office documents from S3, converts them to PDF, classifies and normalizes each page, invokes AWS Textract plus optional OpenAI Vision, and publishes a Markdown knowledge artifact back to S3. The workflow was split into composable steps so it is easier to test, resume, or extend.

## Key Features
- **Multi-format ingestion** – downloads any file type from `AWS_BUCKET_PDF_READER_SOURCE` and converts it to PDF via LibreOffice, ReportLab, or Pillow helpers (`pdf_extractor/convert.py`).
- **Step-wise orchestration** – `pdf_extractor/pipeline.py` sequences numbered steps (download, convert, classify, rotation/normalization, Textract, vision, unify, upload). Each step writes progress so a rerun resumes at the last successful stage.
- **Rich Textract post-processing** – `steps/unify.py` rebuilds structured tables, preserves paragraphs that live outside table bounding boxes, and interleaves per-page Vision summaries directly inside the Markdown output.
- **Vision sidecar** – image-heavy pages are rendered and (optionally) cropped figures are saved as PNGs, summarized asynchronously via OpenAI (if `OPENAI_API_KEY` is set), and merged inline instead of appended at the end of the doc.
- **Testing support** – Pytest suite under `tests/` exercises conversion utilities, pipeline wiring, context handling, and Markdown assembly logic.

## Project Layout
```
pdf_extractor/
  config.py           # settings + env wiring
  context.py          # JobCtx (paths, state persistence, logging)
  convert.py          # multi-format converters + SOFFICE shim
  pipeline.py         # CLI-friendly orchestrator
  steps/              # numbered pipeline stages (download, convert, classify, etc.)
pdf_extractor_reset_table.py  # minimal CLI entry point (run this script)
tests/                         # pytest-based regression coverage
```
The pipeline stores working data under `pdf_extractor/exec/` (created automatically):
- `data/` – raw downloads + converted PDFs
- `textract_output/<file-id>/` – job log, checkpoints (`status.json`), Textract JSON, rendered images, Markdown output, etc.

## Prerequisites
1. **Python** 3.11+ (a virtualenv lives in `venv/`).
2. **Dependencies** – install once:
   ```powershell
   .\venv\Scripts\python -m pip install -r requirements.txt
   ```
3. **LibreOffice** (for DOC/DOCX/PPT/PPTX conversion). If the binary is not on `PATH`, set `SOFFICE_PATH` to `soffice.exe`, e.g.:
   ```powershell
   setx SOFFICE_PATH "C:\Program Files\LibreOffice\program\soffice.exe"
   ```
4. **Tesseract OCR** (for optical character recognition, used by pytessaract):
   - Download and install from [Tesseract GitHub Releases](https://github.com/UB-Mannheim/tesseract/wiki)
   - Recommended: Install to `C:\Program Files\Tesseract-OCR`
   - After installation, set the path in your `.env`:
     ```
     PYTESSERACT_PATH=C:\Program Files\Tesseract-OCR\tesseract.exe
     ```
   - Or set it programmatically in `pdf_extractor/config.py` via:
     ```python
     import pytesseract
     pytesseract.pytesseract.pytesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'
     ```
   - Verify installation:
     ```powershell
     & "C:\Program Files\Tesseract-OCR\tesseract.exe" --version
     ```
5. **AWS configuration** – create a `.env` file (see [Environment Configuration](#environment-configuration-env) section) with the following buckets and credentials:
   - `AWS_BUCKET_PDF_READER_SOURCE` – input documents
   - `AWS_BUCKET_PDF_READER_TEXTRACT` – normalized PDFs for Textract
   - `AWS_BUCKET_PDF_READER_OUTPUT` – final Markdown uploads
   - `AWS_REGION`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
6. **OpenAI (optional)** – set `OPENAI_API_KEY` in `.env` to enable Vision summaries. Without it, the Vision step is skipped gracefully.
7. **Keep Vision crops (optional)** – set `PDF_EXTRACTOR_KEEP_VISION_IMAGES=1` in `.env` to retain cropped figure PNGs written under `exec/textract_output/<file-id>/vision_imgs/` for inspection/debugging.

## Environment Configuration (.env)
Create a `.env` file in the repository root and configure the following variables:
```
# AWS Configuration
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your_access_key_here
AWS_SECRET_ACCESS_KEY=your_secret_key_here

# S3 Buckets
AWS_BUCKET_PDF_READER_SOURCE=dheera-doc-reader
AWS_BUCKET_PDF_READER_TEXTRACT=dheera-doc-reader-textract
AWS_BUCKET_PDF_READER_OUTPUT=dheera-doc-reader-output

# OpenAI (Optional - required for Vision summaries)
OPENAI_API_KEY=sk-your_api_key_here

# LibreOffice (Optional - if not on PATH)
SOFFICE_PATH=C:\Program Files\LibreOffice\program\soffice.exe

# Tesseract OCR (Optional - required for pytessaract OCR functionality)
PYTESSERACT_PATH=C:\Program Files\Tesseract-OCR\tesseract.exe

# Vision Images (Optional - keep cropped figures for debugging)
PDF_EXTRACTOR_KEEP_VISION_IMAGES=1
```

**Notes:**
- The pipeline reads `.env` automatically via the config loader in `pdf_extractor/config.py`
- All AWS credentials and bucket names are **required**
- `OPENAI_API_KEY` is optional; without it, Vision steps are skipped gracefully
- `SOFFICE_PATH` is only needed if LibreOffice is not on your system PATH
- `PYTESSERACT_PATH` is only needed if Tesseract is not on your system PATH; required if using pytessaract for OCR
- `PDF_EXTRACTOR_KEEP_VISION_IMAGES=1` preserves cropped PNGs in `exec/textract_output/<file-id>/vision_imgs/` for inspection

## Running the Pipeline
Use the provided CLI wrapper, which takes a friendly job id plus the full S3 URI of the source document:
```powershell
.\venv\Scripts\python pdf_extractor_reset_table.py --file-id disinfectant-bmr-ipa --s3-path s3://pdfreadersource/Disinfectant_BMR_IPA.docx
```
Behavior notes:
- The download step pulls the original file into `exec/data/<file-id>.source`.
- Conversion writes a PDF into the job directory and updates the checkpoint so reruns skip work already completed.
- `step_04_textract` automatically kicks off Vision rendering/summary threads while Textract processes.
- Vision crops: when enabled, cropped figures (charts/flows) are stored as PNGs in `exec/textract_output/<file-id>/vision_imgs/` and the saved paths are included in `vision_results.json` for traceability.
- `step_07_unify` produces `<file-id>.pdf.md`, fusing Textract paragraphs, tables, and any page-level Vision notes, then `step_08_upload_and_cleanup` pushes the Markdown to the output bucket.

Jobs can be resumed: re-running the same command consults `status.json` and continues from the first incomplete step.

## Testing
Run the full suite from the repo root:
```powershell
.\venv\Scripts\python -m pytest
```
Tests cover conversion dispatch, context persistence, pipeline wiring, and the page-level Markdown assembler (ensuring text outside tables remains visible and Vision summaries land on their respective page sections).

## Troubleshooting
- **`soffice` not found** – install LibreOffice or set `SOFFICE_PATH`. The converter raises a descriptive error if the binary is missing.
- **Vision summaries missing** – ensure `OPENAI_API_KEY` is available. Without it, the Vision step writes `vision_results.json` with `{"skipped": true}`.
- **Textract job failure** – check `pdf_extractor/exec/textract_output/<file-id>/job.log` and `textract_raw.json` for AWS error details, then rerun after fixing the upstream issue.
- **RAG alignment issues** – the current Markdown emitter renders output per page (`## Page N`), followed by `### Text`, `### Tables`, and optional `### Vision` so downstream chunking retains the original reading order.

## Extending the Pipeline
- Add new preprocessing steps by creating a module under `pdf_extractor/steps/` and inserting it into the `run_pipeline` order.
- Customize Textract or Vision behavior by editing `steps/unify.py` (table heuristics, paragraph grouping) or `steps/vision.py` (rendering quality, prompt text).
- Use `JobCtx.save_status()` to persist any additional metadata you want to expose downstream or when resuming.

With these pieces in place you can continuously ingest mixed-format lab notebooks, SOPs, or BMRs, normalize them into Markdown, and feed the results into downstream RAG systems without losing page-level context.
#   m d - c o n v e r t e r - f e a t u r e  
 