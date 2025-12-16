from __future__ import annotations

import boto3

from .config import AWS_KEY, AWS_REGION, AWS_SEC


def s3_client():
    return boto3.client(
        "s3",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SEC,
    )


def textract_client():
    return boto3.client(
        "textract",
        region_name=AWS_REGION,
        aws_access_key_id=AWS_KEY,
        aws_secret_access_key=AWS_SEC,
    )
