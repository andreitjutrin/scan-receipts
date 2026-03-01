"""
Upload Lambda
Creates a pending receipt record and returns a presigned S3 URL
so the browser/phone can upload the photo directly to S3.
"""

import json
import os
import uuid
import boto3
from datetime import datetime, timezone

IMAGES_BUCKET    = os.environ["IMAGES_BUCKET"]
PRESIGNED_EXPIRY = int(os.environ.get("PRESIGNED_EXPIRY", "300"))

import sys
sys.path.insert(0, "/var/task")
import dynamo_client as db
from models import Receipt, ProcessingStatus

s3 = boto3.client("s3")


def lambda_handler(event, context):
    try:
        body = json.loads(event.get("body") or "{}")
    except json.JSONDecodeError:
        return _error(400, "Invalid JSON body")

    filename     = body.get("filename", "receipt.jpg")
    content_type = body.get("content_type", "image/jpeg")
    store_id     = body.get("store_id", "unknown")

    receipt_id = str(uuid.uuid4())
    ext        = filename.rsplit(".", 1)[-1].lower() if "." in filename else "jpg"
    s3_key     = f"receipts/{receipt_id}/original.{ext}"

    now = datetime.now(timezone.utc).isoformat()
    receipt = Receipt(
        receipt_id=receipt_id,
        retailer_id=store_id,
        s3_key=s3_key,
        status=ProcessingStatus.PENDING,
        created_at=now,
        updated_at=now
    )
    db.save_receipt(receipt.model_dump())

    upload_url = s3.generate_presigned_url(
        "put_object",
        Params={
            "Bucket":      IMAGES_BUCKET,
            "Key":         s3_key,
            "ContentType": content_type,
        },
        ExpiresIn=PRESIGNED_EXPIRY
    )

    return _ok({
        "receipt_id":  receipt_id,
        "upload_url":  upload_url,
        "s3_key":      s3_key,
        "expires_in":  PRESIGNED_EXPIRY,
        "store_id":    store_id
    })


def _ok(data):
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type":                "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type,X-Api-Key"
        },
        "body": json.dumps({"success": True, **data})
    }

def _error(status, message):
    return {
        "statusCode": status,
        "headers": {
            "Content-Type":                "application/json",
            "Access-Control-Allow-Origin": "*"
        },
        "body": json.dumps({"success": False, "error": message})
    }
