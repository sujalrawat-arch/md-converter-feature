"""Concurrent SQS worker service for CPU-heavy PDF processing.

This service runs a configurable number of workers in parallel (default 5). Each worker:
- Long-polls an AWS SQS queue (WaitTimeSeconds=20)
- Receives at most 1 message (MaxNumberOfMessages=1)
- Processes the message synchronously via `process_pdf(message_body)`
- Deletes the message only on successful processing
- Leaves the message in the queue on failure (for SQS retry)
- AUTOMATICALLY RESTARTS if the process crashes (Supervisor Pattern)

Assumptions:
- AWS credentials/region are configured (env/instance profile)
- Queue URL is available via env var `SQS_QUEUE_URL`
- A function `process_pdf(message_body)` is importable

No external rate limiter; concurrency is enforced via `multiprocessing`.

Run:
    python -m Prod.sqs_worker_concurrent
"""

from __future__ import annotations

import logging
import os
import sys
import time
from multiprocessing import Process
from typing import Any, Optional

from dotenv import load_dotenv

# Load .env if present
load_dotenv()

import boto3
from botocore.exceptions import BotoCoreError, ClientError

# Use the existing pdf_extractor pipeline for processing
try:
    from pdf_extractor.pipeline import run_pipeline  # type: ignore
except Exception:
    run_pipeline = None  # type: ignore


# Logging configuration
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(processName)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


WAIT_TIME_SECONDS = 20
MAX_NUMBER_OF_MESSAGES = 1
VISIBILITY_TIMEOUT = int(os.getenv("SQS_VISIBILITY_TIMEOUT", "300"))  # seconds


def _create_sqs_client() -> Any:
    """Create and return a boto3 SQS client.

    Respects `AWS_REGION` if set, else boto3 resolves region via environment/config.
    """
    aws_region = os.getenv("AWS_REGION")
    if aws_region:
        return boto3.client("sqs", region_name=aws_region)
    return boto3.client("sqs")


def _receive_one_message(sqs_client: Any, queue_url: str) -> Optional[dict[str, Any]]:
    """Receive a single message using long polling.

    Returns the message dict if available, else None.
    """
    try:
        resp = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=MAX_NUMBER_OF_MESSAGES,
            WaitTimeSeconds=WAIT_TIME_SECONDS,
            VisibilityTimeout=VISIBILITY_TIMEOUT,
        )
        messages = resp.get("Messages", [])
        if messages:
            return messages[0]
        return None
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "Unknown")
        logger.error("Failed to receive message (error=%s): %s", code, e)
        raise


def _delete_message(sqs_client: Any, queue_url: str, receipt_handle: Optional[str], message_id: str) -> None:
    """Delete a processed message from SQS."""
    if not receipt_handle:
        raise ValueError(f"Cannot delete message {message_id}: missing ReceiptHandle")
    try:
        sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        logger.debug("Deleted message from SQS: id=%s", message_id)
    except ClientError as e:
        code = e.response.get("Error", {}).get("Code", "Unknown")
        logger.error("Failed to delete message id=%s (error=%s): %s", message_id, code, e)
        raise


def _parse_payload(body: str) -> dict[str, Any]:
    """Parse and validate SQS message body for the pipeline.

    Expected JSON structure (all strings unless noted):
    {
        "user_id": "...",
        "tenant_id": "...",
        "file_id": "...",
        "filename": "...",
        "version": <int>,
        "s3_path": "...",
        "message": "...",
        "customer_id": "..." (optional),
        "project_id": "..." (optional)
    }
    """
    import json

    payload = json.loads(body)
    required = ["user_id", "tenant_id", "file_id", "filename", "version", "s3_path"]
    missing = [f for f in required if f not in payload]
    if missing:
        raise ValueError(f"Missing required fields: {missing}")

    # basic normalization
    payload["file_id"] = str(payload["file_id"]).strip()
    payload["s3_path"] = str(payload["s3_path"]).strip()
    payload["filename"] = str(payload["filename"]).strip()
    payload["user_id"] = str(payload["user_id"]).strip()
    payload["tenant_id"] = str(payload["tenant_id"]).strip()
    try:
        payload["version"] = int(payload.get("version", 1) or 1)
    except Exception:
        payload["version"] = 1
    return payload


def worker_loop(queue_url: str) -> None:
    """Worker process loop: poll, process, delete on success.

    Each iteration processes at most one message, synchronously.
    """
    # Create SQS client inside the process
    sqs_client = _create_sqs_client()

    if run_pipeline is None:
        logger.error("pdf_extractor.pipeline.run_pipeline is not importable. Exiting worker.")
        sys.exit(2)

    logger.info("Worker started; polling queue: %s", queue_url)

    try:
        while True:
            try:
                msg = _receive_one_message(sqs_client, queue_url)
            except (BotoCoreError, ClientError) as e:
                logger.error("AWS error during receive: %s. Backing off 5s...", e)
                time.sleep(5)
                continue

            if not msg:
                # No message; loop back to long-poll
                continue

            message_id = msg.get("MessageId", "unknown")
            receipt_handle = msg.get("ReceiptHandle")
            body = msg.get("Body", "")

            logger.info("Message received: id=%s", message_id)
            logger.info("Processing start: id=%s", message_id)

            try:
                # Parse payload and run pipeline synchronously (CPU-heavy)
                payload = _parse_payload(body)
                run_pipeline(payload)  # raises on failure

                # On success, delete message
                _delete_message(sqs_client, queue_url, receipt_handle, message_id)
                logger.info("Processing success: id=%s (deleted)", message_id)
            except Exception as e:
                # Do not delete the message; allow SQS to retry
                logger.exception("Processing failure: id=%s; leaving for retry. Error: %s", message_id, e)
                # Optional: small delay to avoid tight failure loops
                time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Worker interrupted; exiting.")
    except Exception as e:
        logger.exception("Worker fatal error: %s", e)
        sys.exit(1)


def main() -> None:
    # Validate queue URL
    queue_url = os.getenv("SQS_QUEUE_URL")
    if not queue_url:
        logger.error("SQS_QUEUE_URL environment variable is required.")
        sys.exit(1)

    # Configuration
    try:
        worker_count = int(os.getenv("WORKER_CONCURRENCY", "5"))
    except ValueError:
        logger.warning("Invalid WORKER_CONCURRENCY; defaulting to 5.")
        worker_count = 5

    logger.info("Initializing service with %d workers...", worker_count)

    # Spawn workers
    workers: list[Process] = []
    for i in range(worker_count):
        p = Process(target=worker_loop, name=f"Worker-{i+1}", args=(queue_url,))
        p.daemon = False  # ensure proper lifecycle; allow clean joins
        p.start()
        workers.append(p)
        logger.info("Worker-%d started (pid=%s)", i+1, p.pid)

    logger.info("All workers started. Entering Supervisor Loop.")

    try:
        # SUPERVISOR LOOP
        # Instead of just joining, we monitor and respawn.
        while True:
            # Check health every second
            time.sleep(1.0)
            
            for i, p in enumerate(workers):
                if not p.is_alive():
                    logger.warning("Worker-%d (pid=%s) died unexpectedly. Respawning...", i+1, p.pid)
                    
                    # Create replacement worker
                    new_worker = Process(target=worker_loop, name=f"Worker-{i+1}", args=(queue_url,))
                    new_worker.daemon = False
                    new_worker.start()
                    
                    # Update list with new process
                    workers[i] = new_worker
                    logger.info("Respawned Worker-%d (new pid=%s)", i+1, new_worker.pid)

    except KeyboardInterrupt:
        logger.info("Main interrupted; terminating workers...")
        for w in workers:
            try:
                if w.is_alive():
                    w.terminate()
            except Exception:
                pass
        
        # Give them a moment to die gracefully
        for w in workers:
            try:
                w.join(timeout=2)
            except Exception:
                pass
        logger.info("Shutdown complete.")


if __name__ == "__main__":  # pragma: no cover
    main()