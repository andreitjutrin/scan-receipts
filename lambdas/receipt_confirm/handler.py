"""
Receipts Lambda — CRUD for receipts and line items.
"""

import json
import os
import sys
import boto3

import dynamo_client as db
from models import ProcessingStatus

IMAGES_BUCKET  = os.environ["IMAGES_BUCKET"]
EXPORTS_BUCKET = os.environ["EXPORTS_BUCKET"]


def lambda_handler(event, context):
    method     = event.get("httpMethod", "")
    path       = event.get("path", "")
    params     = event.get("pathParameters") or {}
    receipt_id = params.get("receipt_id")
    item_seq   = params.get("item_seq")

    try:
        if method == "GET" and path == "/expenses":
            return _list_expenses()
        elif method == "GET" and path.endswith("/receipts/summary"):
            return _get_receipts_summary()
        elif method == "GET" and "/receipts" in path and not receipt_id:
            return _list_receipts(event)
        elif method == "GET" and receipt_id and "confirm" not in path:
            return _get_receipt(receipt_id)
        elif method == "POST" and receipt_id and "confirm" in path:
            return _confirm_receipt(receipt_id, event)
        elif method == "POST" and receipt_id and item_seq and "expense" in path:
            return _flag_expense(receipt_id, item_seq, event)
        elif method == "POST" and receipt_id and item_seq and "split" in path:
            return _split_item(receipt_id, item_seq, event)
        elif method == "PATCH" and receipt_id and item_seq:
            return _update_item_price(receipt_id, item_seq, event)
        elif method == "DELETE" and receipt_id and item_seq:
            return _delete_item(receipt_id, item_seq)
        elif method == "DELETE" and receipt_id:
            return _delete_receipt(receipt_id)
        elif method == "PATCH" and receipt_id and "total" in path:
            return _update_total(receipt_id, event)
        else:
            return _error(404, "Not found")
    except Exception as e:
        import traceback; traceback.print_exc()
        return _error(500, str(e))


def _get_receipts_summary():
    summary = db.get_receipts_summary()
    return _ok(summary)


def _list_receipts(event):
    qp     = event.get("queryStringParameters") or {}
    before = qp.get("before")
    days   = min(max(int(qp.get("days", 3)), 1), 30)
    result = db.list_receipts_page(before=before, days=days)
    return _ok(result)


def _get_receipt(receipt_id):
    receipt = db.get_receipt(receipt_id)
    if not receipt:
        return _error(404, f"Receipt {receipt_id} not found")
    items = db.get_items(receipt_id)
    items.sort(key=lambda x: x.get("item_seq", ""))

    # Include presigned image URL so the review screen can show the receipt photo
    image_url = None
    s3_key = receipt.get("s3_key", "")
    if s3_key:
        try:
            image_url = db.get_image_presigned_url(s3_key)
        except Exception:
            pass  # not critical if this fails

    return _ok({"receipt": receipt, "items": items, "image_url": image_url})


def _confirm_receipt(receipt_id, event):
    try:
        body = json.loads(event.get("body") or "{}")
    except json.JSONDecodeError:
        return _error(400, "Invalid JSON")

    corrections = body.get("items", [])

    receipt = db.get_receipt(receipt_id)
    if not receipt:
        return _error(404, f"Receipt {receipt_id} not found")

    store_id  = receipt.get("retailer_id", "unknown")
    corrected = 0

    # Load item types once for resolving category from item_type_id
    all_item_types = {it["item_type_id"]: it for it in db.load_item_types()}

    for correction in corrections:
        item_seq     = correction.get("item_seq")
        category     = correction.get("category")
        item_type_id = correction.get("item_type_id")
        if not item_seq or not category:
            continue

        # If item_type_id provided, resolve category from it (two-hop)
        if item_type_id and item_type_id in all_item_types:
            category = all_item_types[item_type_id].get("category_id", category)
        elif item_type_id:
            # Unknown item_type_id ' treat item_type_id as the category key
            pass

        db.update_item_category(receipt_id, item_seq, category, item_type_id=item_type_id)

        items = db.get_items(receipt_id)
        item  = next((i for i in items if i.get("item_seq") == item_seq), None)
        if item:
            normalized = item.get("normalized_name", "")
            if normalized:
                db.save_correction(
                    store_id=store_id,
                    normalized_name=normalized,
                    item_type_id=item_type_id or category,
                    category=category
                )
        corrected += 1

    all_items  = db.get_items(receipt_id)
    still_open = sum(1 for i in all_items if i.get("needs_review") and not i.get("confirmed"))
    new_status = (ProcessingStatus.COMPLETED.value if still_open == 0
                  else ProcessingStatus.NEEDS_REVIEW.value)
    db.update_receipt(receipt_id, status=new_status, needs_review_count=still_open)

    # Confirm = explicit user approval — always export to Excel
    result   = db.export_receipt_to_excel(receipt_id, EXPORTS_BUCKET)
    exported = result is not None

    return _ok({"receipt_id": receipt_id, "corrected": corrected,
                "still_open": still_open, "status": new_status, "exported": exported})


def _update_item_price(receipt_id, item_seq, event):
    try:
        body = json.loads(event.get("body") or "{}")
    except json.JSONDecodeError:
        return _error(400, "Invalid JSON")
    price = body.get("price")
    if price is None:
        return _error(400, "price is required")
    if not db.get_receipt(receipt_id):
        return _error(404, f"Receipt {receipt_id} not found")
    db.update_item_price(receipt_id, item_seq, str(price))
    return _ok({"receipt_id": receipt_id, "item_seq": item_seq, "price": price})


def _delete_item(receipt_id, item_seq):
    receipt = db.get_receipt(receipt_id)
    if not receipt:
        return _error(404, f"Receipt {receipt_id} not found")

    db.delete_item(receipt_id, item_seq)

    all_items  = db.get_items(receipt_id)
    item_count = len(all_items)
    still_open = sum(1 for i in all_items if i.get("needs_review") and not i.get("confirmed"))
    new_status = (ProcessingStatus.COMPLETED.value if still_open == 0
                  else ProcessingStatus.NEEDS_REVIEW.value)
    db.update_receipt(receipt_id, status=new_status,
                      needs_review_count=still_open, item_count=item_count)

    return _ok({"receipt_id": receipt_id, "item_seq": item_seq,
                "item_count": item_count, "still_open": still_open,
                "status": new_status})


def _delete_receipt(receipt_id):
    receipt = db.get_receipt(receipt_id)
    if not receipt:
        return _error(404, f"Receipt {receipt_id} not found")

    db.delete_items(receipt_id)
    db.delete_receipt(receipt_id)

    s3_key = receipt.get("s3_key")
    if s3_key:
        try:
            boto3.client("s3").delete_object(Bucket=IMAGES_BUCKET, Key=s3_key)
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning("Could not delete S3 image %s: %s", s3_key, e)

    try:
        db.remove_receipt_from_excel(receipt_id, EXPORTS_BUCKET)
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning("Could not remove from Excel: %s", e)

    return _ok({"deleted": receipt_id})


def _update_total(receipt_id, event):
    try:
        body = json.loads(event.get("body") or "{}")
    except json.JSONDecodeError:
        return _error(400, "Invalid JSON")
    total = body.get("total_amount")
    if total is None:
        return _error(400, "total_amount is required")
    if not db.get_receipt(receipt_id):
        return _error(404, f"Receipt {receipt_id} not found")
    db.update_receipt(receipt_id, total_amount=str(total))
    return _ok({"receipt_id": receipt_id, "total_amount": total})


def _flag_expense(receipt_id, item_seq, event):
    try:
        body = json.loads(event.get("body") or "{}")
    except json.JSONDecodeError:
        return _error(400, "Invalid JSON")

    is_expense = bool(body.get("is_expense", False))
    receipt = db.get_receipt(receipt_id)
    if not receipt:
        return _error(404, f"Receipt {receipt_id} not found")

    db.set_item_expense(receipt_id, item_seq, is_expense)

    # If this receipt was already exported to Excel, refresh it so the
    # expense exclusion (or re-inclusion) takes effect immediately.
    items = db.get_items(receipt_id)
    already_exported = any(i.get("exported_to_excel") for i in items)
    if already_exported:
        db.remove_receipt_from_excel(receipt_id, EXPORTS_BUCKET)
        # Re-export only if there are still non-expense items remaining
        if any(not i.get("is_expense") for i in items):
            db.export_receipt_to_excel(receipt_id, EXPORTS_BUCKET)

    return _ok({"receipt_id": receipt_id, "item_seq": item_seq, "is_expense": is_expense})


def _split_item(receipt_id, item_seq, event):
    try:
        body = json.loads(event.get("body") or "{}")
    except json.JSONDecodeError:
        return _error(400, "Invalid JSON")

    import re
    name1  = (body.get("name1") or "").strip()
    price1 = str(body.get("price1") or "0.00")
    name2  = (body.get("name2") or "").strip()
    price2 = str(body.get("price2") or "0.00")

    if not name1 or not name2:
        return _error(400, "name1 and name2 are required")

    receipt = db.get_receipt(receipt_id)
    if not receipt:
        return _error(404, f"Receipt {receipt_id} not found")

    def _norm(s):
        s = s.lower().strip()
        s = re.sub(r'\b\d+\s*(g|kg|ml|l|oz|lb|pack|pk|x\d+)\b', '', s, flags=re.I)
        return re.sub(r'\s+', ' ', s).strip()

    db.replace_item(receipt_id, {
        "item_seq": item_seq, "raw_name": name1, "normalized_name": _norm(name1),
        "price": price1, "price_raw": price1, "category": "unknown",
        "needs_review": True, "confirmed": False, "match_source": "unknown",
        "match_confidence": 0.0, "quantity": "1", "exported_to_excel": False,
    })

    new_seq = item_seq + "s"
    db.replace_item(receipt_id, {
        "item_seq": new_seq, "raw_name": name2, "normalized_name": _norm(name2),
        "price": price2, "price_raw": price2, "category": "unknown",
        "needs_review": True, "confirmed": False, "match_source": "unknown",
        "match_confidence": 0.0, "quantity": "1", "exported_to_excel": False,
    })

    all_items  = db.get_items(receipt_id)
    still_open = sum(1 for i in all_items if i.get("needs_review") and not i.get("confirmed"))
    db.update_receipt(receipt_id, needs_review_count=still_open,
                      item_count=len(all_items),
                      status=ProcessingStatus.NEEDS_REVIEW.value)

    return _ok({"receipt_id": receipt_id, "split_seq": item_seq, "new_seq": new_seq})


def _list_expenses():
    items = db.list_expense_items()
    return _ok({"expenses": items, "count": len(items)})


_ORIGIN = os.environ.get("FRONTEND_ORIGIN", "*")

def _ok(data):
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": _ORIGIN,
                    "Access-Control-Allow-Headers": "Content-Type,Authorization"},
        "body": json.dumps({"success": True, **data}, default=str)
    }

def _error(status, message):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": _ORIGIN},
        "body": json.dumps({"success": False, "error": message})
    }
