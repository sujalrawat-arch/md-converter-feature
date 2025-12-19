# PDF Extractor Pipeline

This repository contains a modular, resumable pipeline that ingests arbitrary office documents from S3, converts them to PDF, classifies and normalizes each page, invokes AWS Textract plus optional OpenAI Vision, and publishes a Markdown knowledge artifact back to S3. The workflow was split into composable steps so it is easier to test, resume, or extend.

## Pipeline Flow Diagram

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                         PDF EXTRACTOR PIPELINE                               │
└──────────────────────────────────────────────────────────────────────────────┘

  Step 01               Step 02               Step 03               Step 04
   DOWNLOAD      →       CONVERT        →      CLASSIFY      →     TEXTRACT
     │                    │                     │                    │
  S3 Source      LibreOffice/        Page rotation/         AWS Textract
  Bucket         Pillow/ReportLab    classification        JSON extraction
     │                    │                     │                    │
     └────────────────────┴─────────────────────┴────────────────────┘
                              │
                              ↓
                     Step 05: VISION (optional)
                              │
                    OpenAI Vision API
                    Image analysis
                              │
                              ↓
                     Step 06: UNIFY
                              │
                   Combine Textract +
                   Vision + Metadata
                              │
                              ↓
                   Step 07: UPLOAD
                              │
                    Push Markdown to
                   S3 Output Bucket
                              │
                              ↓
                   ✓ COMPLETE & RESUME-SAFE

Status persisted after each step → allows resuming from last checkpoint
```

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

## Developer Setup Guide

### Step 1: Clone and Navigate to Repository
```powershell
cd path/to/your/workspace
git clone <repository-url>
cd pdf_extractor_project
```

### Step 2: Set Up Python Virtual Environment
Create and activate a Python 3.11+ virtual environment:
.\venv\Scripts\Activate.ps1

# If you get an execution policy error, run:
# Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
```

### Step 3: Install Dependencies
```powershell
# Upgrade pip, setuptools, wheel
.\venv\Scripts\python -m pip install --upgrade pip setuptools wheel

# Install project dependencies
.\venv\Scripts\python -m pip install -r requirements.txt
```

### Step 4: Install System Dependencies

#### LibreOffice Installation
- **Windows**: Download and install from [LibreOffice Official Site](https://www.libreoffice.org/download/)
- **Default Path**: `C:\Program Files\LibreOffice`
- **Verify Installation**:
  ```powershell
  & "C:\Program Files\LibreOffice\program\soffice.exe" --version
  ```

#### Tesseract OCR Installation
- Download from [Tesseract GitHub](https://github.com/UB-Mannheim/tesseract)
- Run the installer (e.g., `tesseract-ocr-w64-setup-v5.x.x.exe`)
- Default installation path: `C:\Program Files\Tesseract-OCR`
- **Verify Installation**:
  ```powershell
  & "C:\Program Files\Tesseract-OCR\tesseract.exe" --version
  ```

### Step 5: Configure Environment Variables

Create a `.env` file in the project root (copy from example below):
```powershell
# Create .env file
New-Item -Path .\.env -ItemType File

# Edit with your credentials (use Notepad or your editor)
notepad .env
```

Add the following to `.env`:
```
# AWS Configuration (REQUIRED)
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your_access_key_here
AWS_SECRET_ACCESS_KEY=your_secret_key_here

# S3 Buckets (REQUIRED)
AWS_BUCKET_PDF_READER_SOURCE=dheera-doc-reader
AWS_BUCKET_PDF_READER_TEXTRACT=dheera-doc-reader-textract
AWS_BUCKET_PDF_READER_OUTPUT=dheera-doc-reader-output

# OpenAI (OPTIONAL - for Vision features)
OPENAI_API_KEY=sk-your_api_key_here

# LibreOffice Path (OPTIONAL - only if not in PATH)
SOFFICE_PATH=C:\Program Files\LibreOffice\program\soffice.exe

# Tesseract Path (OPTIONAL - only if not in PATH)

### Step 6: Verify Installation
.\venv\Scripts\python --version

# Test imports
.\venv\Scripts\python -c "import pdf_extractor; print('PDF Extractor imported successfully')"

# Run basic tests
.\venv\Scripts\python -m pytest tests/ -v --tb=short
```

### Step 7: Quick Test Run (Optional)
To test the pipeline with a sample file:
```powershell
# Run with a test document (ensure file exists in S3 source bucket)
.\venv\Scripts\python pdf_extractor_reset_table.py `
  --file-id test-document `
  --s3-path s3://your-bucket/sample.pdf
```

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

PYTESSERACT_PATH=C:\Program Files\Tesseract-OCR\tesseract.exe

# Vision Images (Optional - keep cropped figures for debugging)
```

**Notes:**
- All AWS credentials and bucket names are **required**
- `OPENAI_API_KEY` is optional; without it, Vision steps are skipped gracefully
- `SOFFICE_PATH` is only needed if LibreOffice is not on your system PATH
- `PYTESSERACT_PATH` is only needed if Tesseract is not on your system PATH; required if using pytessaract for OCR
- `PDF_EXTRACTOR_KEEP_VISION_IMAGES=1` preserves cropped PNGs in `exec/textract_output/<file-id>/vision_imgs/` for inspection

## Running the Pipeline

### Basic Usage
Use the provided CLI wrapper, which takes a friendly job id plus the full S3 URI of the source document:
```powershell
.\venv\Scripts\python pdf_extractor_reset_table.py --file-id disinfectant-bmr-ipa --s3-path s3://pdfreadersource/Disinfectant_BMR_IPA.docx
```

- `--file-id` (required): A unique identifier for the job (used for tracking and output naming)
- `--s3-path` (required): Full S3 URI to the source document (e.g., `s3://bucket-name/path/to/document.docx`)
- `--verbose` (optional): Enable detailed logging output

### Execution Flow
Behavior notes:
- The download step pulls the original file into `exec/data/<file-id>.source`.
- Conversion writes a PDF into the job directory and updates the checkpoint so reruns skip work already completed.
- `step_04_textract` automatically kicks off Vision rendering/summary threads while Textract processes.
- Vision crops: when enabled, cropped figures (charts/flows) are stored as PNGs in `exec/textract_output/<file-id>/vision_imgs/` and the saved paths are included in `vision_results.json` for traceability.
- `step_07_unify` produces `<file-id>.pdf.md`, fusing Textract paragraphs, tables, and any page-level Vision notes, then `step_08_upload_and_cleanup` pushes the Markdown to the output bucket.

### Resume Failed Jobs
Jobs can be resumed: re-running the same command consults `status.json` and continues from the first incomplete step.
```powershell
# This will pick up where the pipeline left off
.\venv\Scripts\python pdf_extractor_reset_table.py --file-id disinfectant-bmr-ipa --s3-path s3://pdfreadersource/Disinfectant_BMR_IPA.docx
```

### Expected Output Structure
After a successful run, the output is organized as:
```
exec/
  data/
    <file-id>.source              # Original downloaded file
    <file-id>.pdf                 # Converted PDF
  textract_output/<file-id>/
    status.json                   # Step completion status
    textract_raw.json             # Raw AWS Textract response
    <file-id>.pdf.md              # Final Markdown output
    vision_imgs/                  # Cropped images (if enabled)
    vision_results.json           # Vision analysis results
    job.log                       # Execution log
```

## Testing
Run the full suite from the repo root:
```powershell
.\venv\Scripts\python -m pytest
```

### Running Specific Test Suites
```powershell
# Run tests with verbose output
.\venv\Scripts\python -m pytest -v

# Run specific test file
.\venv\Scripts\python -m pytest tests/test_convert_module.py -v

# Run tests matching a pattern
.\venv\Scripts\python -m pytest -k "test_conversion" -v

# Run with coverage report
.\venv\Scripts\python -m pytest --cov=pdf_extractor tests/
```

Tests cover conversion dispatch, context persistence, pipeline wiring, and the page-level Markdown assembler (ensuring text outside tables remains visible and Vision summaries land on their respective page sections).

### Test Coverage
- `test_context.py` – JobCtx initialization, status persistence, path handling
- `test_convert_module.py` – PDF conversion logic for various formats
- `test_convert_step.py` – Pipeline conversion step execution
- `test_pipeline.py` – Full pipeline orchestration and step sequencing
- `test_utils.py` – Utility functions and helper methods

## Troubleshooting

### Common Issues and Solutions

#### Environment & Setup Issues
- **Virtual environment not activating** – ensure execution policy allows script execution:
  ```powershell
  Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
  ```
- **`ModuleNotFoundError` after installing requirements** – verify virtual environment is activated:
  ```powershell
  .\venv\Scripts\Activate.ps1
  .\venv\Scripts\python -m pip install -r requirements.txt
  ```

#### Conversion Issues
- **`soffice` not found** – install LibreOffice or set `SOFFICE_PATH`. The converter raises a descriptive error if the binary is missing.
  ```powershell
  setx SOFFICE_PATH "C:\Program Files\LibreOffice\program\soffice.exe"
  ```
- **`tesseract` command not found** – ensure Tesseract OCR is installed and set `PYTESSERACT_PATH`:
  ```powershell
  setx PYTESSERACT_PATH "C:\Program Files\Tesseract-OCR\tesseract.exe"
  ```
- **PDF conversion timeout** – large files may take longer; check `job.log` for details

#### AWS, SQS & S3 Issues

- **AWS credentials not found** – verify `.env` file exists in project root with valid credentials:
  ```powershell
  Test-Path .\.env  # Should return True
  ```
- **S3 bucket access denied** – check IAM permissions and ensure bucket names are correct in `.env`
- **No such key error** – verify source file exists in S3 source bucket at the specified path
- **SQS_QUEUE_URL not set** – ensure your `.env` contains a valid `SQS_QUEUE_URL` for worker mode
- **SQS message not processed** – check worker logs for errors; failed jobs remain in the queue for retry
- **Messages stuck in queue** – ensure at least one worker is running and has access to AWS credentials
- **Messages in DLQ (Dead Letter Queue)** – use `python Prod/sqs_utils.py check-dlq` to inspect failed jobs; investigate and requeue as needed

#### Vision & Textract Issues
- **Vision summaries missing** – ensure `OPENAI_API_KEY` is available. Without it, the Vision step writes `vision_results.json` with `{"skipped": true}`.
- **Textract job failure** – check `pdf_extractor/exec/textract_output/<file-id>/job.log` and `textract_raw.json` for AWS error details, then rerun after fixing the upstream issue.
- **Vision API rate limits** – the pipeline uses exponential backoff; check logs for retry details

#### Output Issues
- **RAG alignment issues** – the current Markdown emitter renders output per page (`## Page N`), followed by `### Text`, `### Tables`, and optional `### Vision` so downstream chunking retains the original reading order.
- **Missing tables in output** – check `textract_raw.json` in the output directory to verify Textract extracted table data
- **Incomplete Markdown** – verify pipeline completed all steps in `status.json`

### SQS Worker Debugging
For SQS worker-specific debugging:
- Check worker logs for each process (look for errors, tracebacks, or AWS issues)
- Ensure `SQS_VISIBILITY_TIMEOUT` is set high enough for long jobs (default 300s)
- Use `python Prod/sqs_utils.py get-queue-status` to monitor queue length
- Use `python Prod/sqs_utils.py check-dlq` to inspect dead-lettered jobs

### Debug Mode
Enable detailed logging by setting environment variable:
```powershell
$env:LOG_LEVEL='DEBUG'
.\venv\Scripts\python pdf_extractor_reset_table.py --file-id test-doc --s3-path s3://bucket/file.pdf
```

### Checking Pipeline Status
View the status of a previous run:
```powershell
# Check completion status
Get-Content "exec/textract_output/<file-id>/status.json" | ConvertFrom-Json

# View execution log
Get-Content "exec/textract_output/<file-id>/job.log" -Tail 50  # Last 50 lines
```

## Frequently Asked Questions (SQS/Worker Mode)

### Q: How do I scale up processing?
**A:** Start more worker processes (on the same or different machines/servers). Each worker polls the same SQS queue and processes jobs independently.

### Q: What happens if a worker crashes?
**A:** The SQS message is not deleted and will be retried by another worker after the visibility timeout. Use a process supervisor to auto-restart workers.

### Q: How do I reprocess failed jobs?
**A:** Inspect the DLQ using `python Prod/sqs_utils.py check-dlq`, fix the root cause, and requeue the message if needed.

### Q: Can I run both SQS and CLI modes?
**A:** Yes. SQS mode is for production/batch/distributed use; CLI mode is for debugging or ad hoc jobs.

---

## Extending the Pipeline

### Adding New Pipeline Steps
To add a new preprocessing or postprocessing step:

1. **Create a new module** under `pdf_extractor/steps/`:
   ```python
   # pdf_extractor/steps/custom_step.py
   from pdf_extractor.context import JobCtx
   
   def process_step(ctx: JobCtx):
       """Your custom processing logic here"""
       ctx.logger.info("Running custom step...")
       # Your code
       return True  # Return False to halt pipeline
   ```

2. **Register the step** in `pdf_extractor/pipeline.py`:
   ```python
   from pdf_extractor.steps.custom_step import process_step
   
   # Add to run_pipeline function
   steps = [
       ("step_01_download", download),
       # ... existing steps ...
       ("step_XX_custom", process_step),  # Insert in sequence
   ]
   ```

3. **Update status tracking** in `JobCtx` if needed:
   ```python
   ctx.save_status()  # Persist progress after your step
   ```

### Customizing Textract Processing
Edit [steps/unify.py](steps/unify.py) to:
- Adjust table heuristics and cell merging logic
- Modify paragraph grouping and boundary detection
- Change Markdown output formatting

### Customizing Vision Analysis
Edit [steps/vision.py](steps/vision.py) to:
- Adjust image rendering quality and resolution
- Modify the OpenAI Vision prompt for different analysis types
- Change cropped figure dimensions or selection criteria

### Adding Custom Configuration
Extend [pdf_extractor/config.py](pdf_extractor/config.py):
```python
# Add new settings
import os
CUSTOM_SETTING = os.getenv('CUSTOM_SETTING', 'default_value')
```

Then use in your code:
```python
from pdf_extractor.config import CUSTOM_SETTING
```

## Architecture Overview

### Core Components

**[pdf_extractor/config.py](pdf_extractor/config.py)**
- Environment variable loading and validation
- AWS client configuration
- Default parameter settings

**[pdf_extractor/context.py](pdf_extractor/context.py)**
- `JobCtx` class: maintains job state, paths, and checkpoint data
- Status persistence and resumption logic
- Unified logging interface

**[pdf_extractor/convert.py](pdf_extractor/convert.py)**
- Multi-format document conversion (DOCX, PPTX, XLS, images, etc.)
- PDF generation via LibreOffice, ReportLab, and Pillow
- Format auto-detection

**[pdf_extractor/pipeline.py](pdf_extractor/pipeline.py)**
- Step orchestration and sequencing
- CLI argument parsing
- Checkpoint management and resumption

### Pipeline Steps

| Step | Module | Purpose |
|------|--------|---------|
| 01 | [download.py](pdf_extractor/steps/download.py) | Download source document from S3 |
| 02 | [convert_pdf.py](pdf_extractor/steps/convert_pdf.py) | Convert to standard PDF format |
| 03 | [rotation.py](pdf_extractor/steps/rotation.py) | Auto-correct page rotation & classification |
| 04 | [textract_run.py](pdf_extractor/steps/textract_run.py) | Submit PDF to AWS Textract |
| 05 | [vision.py](pdf_extractor/steps/vision.py) | Optional: analyze images with OpenAI Vision |
| 06 | [unify.py](pdf_extractor/steps/unify.py) | Combine Textract + Vision into Markdown |
| 07 | [upload.py](pdf_extractor/steps/upload.py) | Upload final Markdown to output bucket |

### Data Flow
```
S3 Source → Download → Convert → Classify → Textract ↘
                                           → Vision ↘
                                                    Unify → S3 Output
```

## Frequently Asked Questions

### Q: Can I skip certain pipeline steps?
**A:** Not directly, but you can modify the `.env` to control optional features:
- Skip Vision: omit `OPENAI_API_KEY`
- Skip image cropping: don't set `PDF_EXTRACTOR_KEEP_VISION_IMAGES`

### Q: What if my document is very large?
**A:** The pipeline handles large PDFs by:
- Processing pages in batches
- Using async threads for Vision analysis
- Streaming Textract responses
- Consider splitting files over 500 pages for faster processing

### Q: Can I integrate this with a web service?
**A:** Yes! `pdf_extractor/pipeline.py` exports core functions that can be wrapped in:
- FastAPI/Flask endpoints
- Lambda functions
- Docker containers (include Dockerfile with dependencies)

### Q: How do I handle failed Textract jobs?
**A:** Re-run the same command:
```powershell
.\venv\Scripts\python pdf_extractor_reset_table.py --file-id xxx --s3-path s3://...
```
The pipeline checks `status.json` and resumes from the failed step.

---

With these pieces in place you can continuously ingest mixed-format lab notebooks, SOPs, or BMRs, normalize them into Markdown, and feed the results into downstream RAG systems without losing page-level context.
#   m d - c o n v e r t e r - f e a t u r e 
 
 