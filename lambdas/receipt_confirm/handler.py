import os
"""
Receipts Lambda — CRUD for receipts and line items.
"""

import json
import os
import sys
sys.path.insert(0, "/var/task")

import dynamo_client as db
from models import ProcessingStatus


def lambda_handler(event, context):
    method     = event.get("httpMethod", "")
    path       = event.get("path", "")
    params     = event.get("pathParameters") or {}
    receipt_id = params.get("receipt_id")

    try:
        if method == "GET" and "/receipts" in path and not receipt_id:
            return _list_receipts(event)
        elif method == "GET" and receipt_id and "confirm" not in path:
            return _get_receipt(receipt_id)
        elif method == "POST" and receipt_id and "confirm" in path:
            return _confirm_receipt(receipt_id, event)
        elif method == "DELETE" and receipt_id:
            return _delete_receipt(receipt_id)
        else:
            return _error(404, "Not found")
    except Exception as e:
        import traceback; traceback.print_exc()
        return _error(500, str(e))


def _list_receipts(event):
    qp        = event.get("queryStringParameters") or {}
    receipts  = db.list_receipts(date_from=qp.get("from"), date_to=qp.get("to"))
    return _ok({"receipts": receipts, "count": len(receipts)})


def _get_receipt(receipt_id):
    receipt = db.get_receipt(receipt_id)
    if not receipt:
        return _error(404, f"Receipt {receipt_id} not found")
    items = db.get_items(receipt_id)
    items.sort(key=lambda x: x.get("item_seq", ""))
    return _ok({"receipt": receipt, "items": items})


def _confirm_receipt(receipt_id, event):
    try:
        body = json.loads(event.get("body") or "{}")
    except json.JSONDecodeError:
        return _error(400, "Invalid JSON")

    corrections = body.get("items", [])
    if not corrections:
        return _error(400, "No items provided")

    receipt = db.get_receipt(receipt_id)
    if not receipt:
        return _error(404, f"Receipt {receipt_id} not found")

    store_id  = receipt.get("retailer_id", "unknown")
    corrected = 0

    for correction in corrections:
        item_seq = correction.get("item_seq")
        category = correction.get("category")
        if not item_seq or not category:
            continue

        db.update_item_category(receipt_id, item_seq, category)

        items = db.get_items(receipt_id)
        item  = next((i for i in items if i.get("item_seq") == item_seq), None)
        if item:
            normalized = item.get("normalized_name", "")
            if normalized:
                db.save_correction(
                    store_id=store_id,
                    normalized_name=normalized,
                    category=category
                )
        corrected += 1

    all_items  = db.get_items(receipt_id)
    still_open = sum(1 for i in all_items if i.get("needs_review") and not i.get("confirmed"))
    new_status = (ProcessingStatus.COMPLETED.value if still_open == 0
                  else ProcessingStatus.NEEDS_REVIEW.value)
    db.update_receipt(receipt_id, status=new_status, needs_review_count=still_open)

    # Auto-export when all items are validated
    exported = False
    if still_open == 0 and db.receipt_export_ready(receipt_id):
        exports_bucket = os.environ.get("EXPORTS_BUCKET", "")
        if exports_bucket:
            result = db.export_receipt_to_excel(receipt_id, exports_bucket)
            exported = result is not None

    return _ok({"receipt_id": receipt_id, "corrected": corrected,
                "still_open": still_open, "status": new_status, "exported": exported})


def _delete_receipt(receipt_id):
    receipt = db.get_receipt(receipt_id)
    if not receipt:
        return _error(404, f"Receipt {receipt_id} not found")
    db.delete_items(receipt_id)
    db.delete_receipt(receipt_id)
    return _ok({"deleted": receipt_id})


def _ok(data):
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Headers": "Content-Type,X-Api-Key"},
        "body": json.dumps({"success": True, **data}, default=str)
    }

def _error(status, message):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"success": False, "error": message})
    }
