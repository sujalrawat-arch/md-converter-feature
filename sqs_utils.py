#!/usr/bin/env python3
"""
Utility script for testing and managing the SQS PDF extraction worker.

Usage:
    python sqs_utils.py send-message --job-id job-001 --bucket my-bucket --key path/to/file.pdf
    python sqs_utils.py get-queue-status
    python sqs_utils.py check-dlq
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Optional

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv

# Load environment variables from .env file if present
load_dotenv()

def get_sqs_queue_url() -> str:
    """Get SQS queue URL from environment or config."""
    import os

    queue_url = os.getenv("SQS_QUEUE_URL")
    if not queue_url:
        raise ValueError("SQS_QUEUE_URL environment variable is required")
    return queue_url


def get_sqs_client(region: Optional[str] = None) -> None:
    """Create SQS client with AWS credentials."""
    import os

    if region is None:
        region = os.getenv("AWS_REGION", "ap-south-1")

    return boto3.client(
        "sqs",
        region_name=region,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    )


def send_message(
    job_id: str, s3_bucket: str, s3_key: str, queue_url: Optional[str] = None
) -> None:
    """Send a test message to the SQS queue.

    Args:
        job_id: Unique job identifier
        s3_bucket: S3 bucket name
        s3_key: S3 object key
        queue_url: SQS queue URL (uses env var if not provided)
    """
    if not queue_url:
        queue_url = get_sqs_queue_url()

    sqs = get_sqs_client()

    payload = {"job_id": job_id, "s3_bucket": s3_bucket, "s3_key": s3_key}

    try:
        response = sqs.send_message(QueueUrl=queue_url, MessageBody=json.dumps(payload))
        print(f"✓ Message sent successfully!")
        print(f"  MessageId: {response['MessageId']}")
        print(f"  Job ID: {job_id}")
        print(f"  S3 Path: s3://{s3_bucket}/{s3_key}")
    except ClientError as e:
        print(f"✗ Failed to send message: {e}")
        sys.exit(1)


def get_queue_status(queue_url: Optional[str] = None) -> None:
    """Display queue statistics.

    Args:
        queue_url: SQS queue URL (uses env var if not provided)
    """
    if not queue_url:
        queue_url = get_sqs_queue_url()

    sqs = get_sqs_client()

    try:
        response = sqs.get_queue_attributes(
            QueueUrl=queue_url,
            AttributeNames=[
                "ApproximateNumberOfMessages",
                "ApproximateNumberOfMessagesNotVisible",
                "ApproximateNumberOfMessagesDelayed",
                "VisibilityTimeout",
                "MessageRetentionPeriod",
                "ReceiveMessageWaitTimeSeconds",
            ],
        )

        attrs = response["Attributes"]
        print(f"Queue Status: {queue_url}")
        print(f"  Available Messages: {attrs.get('ApproximateNumberOfMessages', 'N/A')}")
        print(
            f"  Processing (Not Visible): {attrs.get('ApproximateNumberOfMessagesNotVisible', 'N/A')}"
        )
        print(f"  Delayed: {attrs.get('ApproximateNumberOfMessagesDelayed', 'N/A')}")
        print(f"  Visibility Timeout: {attrs.get('VisibilityTimeout', 'N/A')}s")
        print(f"  Message Retention: {attrs.get('MessageRetentionPeriod', 'N/A')}s")
        print(
            f"  Long Poll Timeout: {attrs.get('ReceiveMessageWaitTimeSeconds', 'N/A')}s"
        )

    except ClientError as e:
        print(f"✗ Failed to get queue status: {e}")
        sys.exit(1)


def check_dlq(queue_url: Optional[str] = None) -> None:
    """Check if a DLQ is configured and display its status.

    Args:
        queue_url: SQS queue URL (uses env var if not provided)
    """
    if not queue_url:
        queue_url = get_sqs_queue_url()

    sqs = get_sqs_client()

    try:
        response = sqs.get_queue_attributes(
            QueueUrl=queue_url, AttributeNames=["RedrivePolicy", "All"]
        )

        attrs = response["Attributes"]
        redrive_policy = attrs.get("RedrivePolicy")

        if not redrive_policy:
            print("No DLQ configured for this queue.")
            return

        policy = json.loads(redrive_policy)
        dlq_arn = policy.get("deadLetterTargetArn")
        max_receive = policy.get("maxReceiveCount")

        print(f"DLQ Configuration:")
        print(f"  Target ARN: {dlq_arn}")
        print(f"  Max Receive Count: {max_receive}")

        # Extract DLQ name from ARN and get its status
        if dlq_arn:
            dlq_name = dlq_arn.split(":")[-1]
            dlq_response = sqs.get_queue_url(QueueName=dlq_name)
            dlq_url = dlq_response["QueueUrl"]

            dlq_attrs = sqs.get_queue_attributes(
                QueueUrl=dlq_url, AttributeNames=["ApproximateNumberOfMessages"]
            )

            dlq_messages = dlq_attrs["Attributes"].get(
                "ApproximateNumberOfMessages", "0"
            )
            print(f"\nDLQ Status:")
            print(f"  Queue: {dlq_name}")
            print(f"  Failed Messages: {dlq_messages}")

            if int(dlq_messages) > 0:
                print(
                    f"  ⚠ {dlq_messages} message(s) in DLQ - check worker logs for errors"
                )

    except ClientError as e:
        print(f"✗ Failed to check DLQ: {e}")
        sys.exit(1)


def purge_queue(queue_url: Optional[str] = None, confirm: bool = False) -> None:
    """Delete all messages from the queue (DANGEROUS).

    Args:
        queue_url: SQS queue URL (uses env var if not provided)
        confirm: Skip confirmation prompt
    """
    if not queue_url:
        queue_url = get_sqs_queue_url()

    if not confirm:
        response = input(
            f"⚠ Delete all messages from {queue_url}? (yes/no): "
        ).strip()
        if response.lower() not in ("yes", "y"):
            print("Cancelled.")
            return

    sqs = get_sqs_client()

    try:
        sqs.purge_queue(QueueUrl=queue_url)
        print("✓ Queue purged successfully!")
    except ClientError as e:
        print(f"✗ Failed to purge queue: {e}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Utility script for SQS PDF extraction worker"
    )
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # Send message command
    send_parser = subparsers.add_parser("send-message", help="Send a test message")
    send_parser.add_argument("--job-id", required=True, help="Job ID")
    send_parser.add_argument("--bucket", required=True, help="S3 bucket name")
    send_parser.add_argument("--key", required=True, help="S3 object key")

    # Queue status command
    subparsers.add_parser("get-queue-status", help="Display queue statistics")

    # DLQ status command
    subparsers.add_parser("check-dlq", help="Check DLQ configuration and status")

    # Purge queue command
    purge_parser = subparsers.add_parser("purge-queue", help="Delete all messages")
    purge_parser.add_argument(
        "--yes", action="store_true", help="Skip confirmation prompt"
    )

    args = parser.parse_args()

    try:
        if args.command == "send-message":
            send_message(args.job_id, args.bucket, args.key)
        elif args.command == "get-queue-status":
            get_queue_status()
        elif args.command == "check-dlq":
            check_dlq()
        elif args.command == "purge-queue":
            purge_queue(confirm=args.yes)
        else:
            parser.print_help()
    except ValueError as e:
        print(f"✗ Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
