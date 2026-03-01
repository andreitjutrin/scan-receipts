"""
Receipts Lambda — CRUD for receipts and line items.

Endpoints:
  GET  /receipts                    — list all receipts (newest first)
  GET  /receipts/{receipt_id}       — get receipt + all line items
  POST /receipts/{receipt_id}/confirm — save category corrections
  DELETE /receipts/{receipt_id}     — delete receipt + items

Corrections flow:
  User fixes a mis-categorised item → save correction to items table
  AND write it back to the mappings table (store-scoped if store known,
  else global). Manual corrections are immediately trusted.
"""

import json
import os
import sys
sys.path.insert(0, "/var/task")

import dynamo_client as db
from models import ProcessingStatus

def lambda_handler(event, context):
    method  = event.get("httpMethod", "")
    path    = event.get("path", "")
    params  = event.get("pathParameters") or {}

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


# ---------------------------------------------------------------------------
# LIST RECEIPTS
# ---------------------------------------------------------------------------
def _list_receipts(event):
    qp        = event.get("queryStringParameters") or {}
    date_from = qp.get("from")
    date_to   = qp.get("to")

    receipts = db.list_receipts(date_from=date_from, date_to=date_to)
    return _ok({"receipts": receipts, "count": len(receipts)})


# ---------------------------------------------------------------------------
# GET RECEIPT + ITEMS
# ---------------------------------------------------------------------------
def _get_receipt(receipt_id: str):
    receipt = db.get_receipt(receipt_id)
    if not receipt:
        return _error(404, f"Receipt {receipt_id} not found")

    items   = db.get_items(receipt_id)
    # Sort items by item_seq
    items.sort(key=lambda x: x.get("item_seq", ""))

    return _ok({"receipt": receipt, "items": items})


# ---------------------------------------------------------------------------
# CONFIRM / CORRECT ITEMS
# ---------------------------------------------------------------------------
def _confirm_receipt(receipt_id: str, event):
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

    store_id = receipt.get("retailer_id", "unknown")

    # Process each correction
    corrected = 0
    for correction in corrections:
        item_seq = correction.get("item_seq")
        category = correction.get("category")
        if not item_seq or not category:
            continue

        # Update the line item
        db.update_item_category(receipt_id, item_seq, category)

        # Find the item to get its normalized_name
        items = db.get_items(receipt_id)
        item  = next((i for i in items if i.get("item_seq") == item_seq), None)

        if item:
            normalized = item.get("normalized_name", "")
            if normalized:
                # Write correction back to mappings table
                # Store-scoped if store is known, global if unknown
                db.save_correction(
                    store_id=store_id,
                    normalized_name=normalized,
                    category=category
                )

        corrected += 1

    # Update receipt status
    # If all flagged items are now confirmed, mark as completed
    all_items  = db.get_items(receipt_id)
    still_open = sum(1 for i in all_items
                     if i.get("needs_review") and not i.get("confirmed"))

    new_status = (ProcessingStatus.COMPLETED.value if still_open == 0
                  else ProcessingStatus.NEEDS_REVIEW.value)
    db.update_receipt(receipt_id,
                      status=new_status,
                      needs_review_count=still_open)

    return _ok({
        "receipt_id":  receipt_id,
        "corrected":   corrected,
        "still_open":  still_open,
        "status":      new_status
    })


# ---------------------------------------------------------------------------
# DELETE RECEIPT
# ---------------------------------------------------------------------------
def _delete_receipt(receipt_id: str):
    receipt = db.get_receipt(receipt_id)
    if not receipt:
        return _error(404, f"Receipt {receipt_id} not found")

    db.delete_items(receipt_id)
    db.delete_receipt(receipt_id)
    return _ok({"deleted": receipt_id})


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _ok(data: dict) -> dict:
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type":                "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers":"Content-Type,X-Api-Key"
        },
        "body": json.dumps({"success": True, **data}, default=str)
    }

def _error(status: int, message: str) -> dict:
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json",
                    "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"success": False, "error": message})
    }
