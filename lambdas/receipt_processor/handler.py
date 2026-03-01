"""
Processor Lambda — triggered automatically by S3 when a photo lands.
"""

import json
import os
import re
import sys
import boto3
from datetime import datetime, timezone

sys.path.insert(0, "/var/task")

from rapidfuzz import fuzz, process as rf_process
import dynamo_client as db
from models import (
    Receipt, ReceiptItem, ProcessingStatus, MatchSource, TrustLevel,
    THRESHOLD_SILENT, THRESHOLD_REVIEW, THRESHOLD_GUESS, MAX_PROCESS_COUNT
)

IMAGES_BUCKET = os.environ["IMAGES_BUCKET"]
textract = boto3.client("textract")

_mapping_cache: dict = {}


def _load_cache(store_id: str):
    for sid in [store_id, "global"]:
        if sid and sid != "unknown" and sid not in _mapping_cache:
            mappings = db.load_store_mappings(sid)
            _mapping_cache[sid] = {m["normalized_name"]: m for m in mappings}


def _get_from_cache(store_id: str, normalized: str):
    hit = _mapping_cache.get(store_id, {}).get(normalized)
    return hit or _mapping_cache.get("global", {}).get(normalized)


def _all_keywords(store_id: str) -> list:
    store  = list(_mapping_cache.get(store_id, {}).keys())
    glob   = list(_mapping_cache.get("global",   {}).keys())
    seen   = set(store)
    return store + [k for k in glob if k not in seen]


_WEIGHT_RE = re.compile(r"\b\d+\s*(g|kg|ml|l|oz|lb|pack|pk|x\d+)\b", re.I)
_SPACE_RE  = re.compile(r"\s+")


def normalize(raw: str, strip_prefixes: list = None) -> str:
    text = raw.lower().strip()
    if strip_prefixes:
        for p in strip_prefixes:
            if text.startswith(p.lower()):
                text = text[len(p):].strip()
    text = _WEIGHT_RE.sub("", text)
    text = _SPACE_RE.sub(" ", text).strip()
    return text


def match_item(normalized: str, store_id: str, strip_prefixes: list):
    hit = _get_from_cache(store_id, normalized)
    if hit:
        src = (MatchSource.STORE_EXACT if hit.get("store_id") == store_id
               else MatchSource.GLOBAL_EXACT)
        return hit["category"], src, 1.0, TrustLevel.TRUSTED, False

    keywords = _all_keywords(store_id)

    best_partial, best_kw = 0, None
    for kw in keywords:
        score = fuzz.partial_ratio(kw, normalized)
        if score > best_partial:
            best_partial, best_kw = score, kw

    if best_partial == 100 and best_kw:
        mapping = _get_from_cache(store_id, best_kw)
        if mapping:
            conf = 0.95
            return mapping["category"], MatchSource.FUZZY, conf, TrustLevel.CONFIDENT, conf < THRESHOLD_SILENT

    if keywords:
        cutoff = int(THRESHOLD_GUESS * 100)
        result = rf_process.extractOne(
            normalized, keywords,
            scorer=fuzz.token_sort_ratio,
            score_cutoff=cutoff
        )
        if result:
            best_kw, score, _ = result
            mapping = _get_from_cache(store_id, best_kw)
            if mapping:
                conf  = round(score / 100, 4)
                trust = TrustLevel.CONFIDENT if conf >= THRESHOLD_SILENT else TrustLevel.TENTATIVE
                return mapping["category"], MatchSource.FUZZY, conf, trust, conf < THRESHOLD_SILENT

    return "other", MatchSource.UNKNOWN, 0.0, TrustLevel.TENTATIVE, True


def run_textract(bucket: str, s3_key: str) -> dict:
    return textract.analyze_expense(
        Document={"S3Object": {"Bucket": bucket, "Name": s3_key}}
    )


def parse_textract(resp: dict):
    date, total = None, None
    header_parts, items = [], []

    for doc in resp.get("ExpenseDocuments", []):
        for field in doc.get("SummaryFields", []):
            ftype = field.get("Type", {}).get("Text", "").upper()
            value = field.get("ValueDetection", {}).get("Text", "").strip()
            if ftype in ("INVOICE_RECEIPT_DATE", "DATE") and not date:
                date = value
            if ftype == "TOTAL" and not total:
                total = value
            if value:
                header_parts.append(value)

        for group in doc.get("LineItemGroups", []):
            for line in group.get("LineItems", []):
                name = price = qty = ""
                for field in line.get("LineItemExpenseFields", []):
                    ftype = field.get("Type", {}).get("Text", "").upper()
                    value = field.get("ValueDetection", {}).get("Text", "").strip()
                    if ftype == "ITEM":     name  = value
                    elif ftype in ("UNIT_PRICE", "PRICE"): price = value
                    elif ftype == "QUANTITY": qty   = value
                if name:
                    items.append({"name": name, "price": price or "0.00", "quantity": qty or "1"})

    return date, total, " ".join(header_parts[:15]), items


def process_receipt(receipt_id: str, bucket: str, s3_key: str, store_id: str = "unknown"):
    allowed = db.check_and_increment_process_count(receipt_id)
    if not allowed:
        db.update_receipt(receipt_id,
                          status=ProcessingStatus.FAILED.value,
                          error_message=f"Max reprocess limit ({MAX_PROCESS_COUNT}) reached")
        return

    db.update_receipt(receipt_id, status=ProcessingStatus.PROCESSING.value)

    try:
        resp = run_textract(bucket, s3_key)
        date, total, header_text, lines = parse_textract(resp)

        if store_id == "unknown":
            store_id, identified = db.detect_retailer(header_text)
        else:
            identified = True

        retailer       = db.get_retailer(store_id) if store_id != "unknown" else {}
        strip_prefixes = (retailer or {}).get("strip_prefixes", [])
        skip_patterns  = [p.lower() for p in (retailer or {}).get("skip_patterns", [])]

        _load_cache(store_id)

        receipt_items, needs_review_count = [], 0

        for i, line in enumerate(lines):
            raw = line["name"]
            if any(p in raw.lower() for p in skip_patterns):
                continue
            norm = normalize(raw, strip_prefixes)
            if not norm:
                continue

            category, source, conf, trust, needs_review = match_item(norm, store_id, strip_prefixes)
            if needs_review:
                needs_review_count += 1

            item = ReceiptItem(
                item_seq=str(i).zfill(4),
                raw_name=raw,
                normalized_name=norm,
                category=category,
                price=line["price"],
                quantity=line["quantity"],
                match_confidence=conf,
                match_source=source,
                trust=trust,
                needs_review=needs_review,
                confirmed=False
            )
            receipt_items.append(item)

            if source == MatchSource.FUZZY and conf >= THRESHOLD_REVIEW and identified:
                db.write_learned_mapping(store_id, norm, category, conf, "fuzzy")
                _mapping_cache.setdefault(store_id, {})[norm] = {
                    "normalized_name": norm, "store_id": store_id, "category": category
                }

            if source in (MatchSource.STORE_EXACT, MatchSource.GLOBAL_EXACT):
                prefix = store_id if source == MatchSource.STORE_EXACT else "global"
                db.promote_mapping_if_ready(f"{prefix}#{norm}")

        db.delete_items(receipt_id)
        db.save_items(receipt_id, [i.model_dump() for i in receipt_items])

        status = (ProcessingStatus.NEEDS_REVIEW if needs_review_count > 0
                  else ProcessingStatus.COMPLETED)

        db.update_receipt(
            receipt_id,
            retailer_id=store_id,
            retailer_name=(retailer or {}).get("name", "Unknown"),
            receipt_date=date or datetime.now(timezone.utc).date().isoformat(),
            total_amount=total,
            status=status.value,
            item_count=len(receipt_items),
            needs_review_count=needs_review_count
        )

    except Exception as e:
        import traceback; traceback.print_exc()
        db.update_receipt(receipt_id,
                          status=ProcessingStatus.FAILED.value,
                          error_message=str(e))
        raise


def lambda_handler(event, context):
    if "Records" in event:
        for record in event["Records"]:
            if record.get("eventSource") == "aws:s3":
                bucket = record["s3"]["bucket"]["name"]
                s3_key = record["s3"]["object"]["key"]
                parts      = s3_key.split("/")
                receipt_id = parts[1] if len(parts) >= 2 else None
                if receipt_id:
                    receipt  = db.get_receipt(receipt_id) or {}
                    store_id = receipt.get("retailer_id", "unknown")
                    process_receipt(receipt_id, bucket, s3_key, store_id)
        return {"statusCode": 200, "body": "ok"}

    try:
        params     = event.get("pathParameters") or {}
        receipt_id = params.get("receipt_id")
        body       = json.loads(event.get("body") or "{}")
        store_id   = body.get("store_id", "unknown")

        if not receipt_id:
            return _error(400, "receipt_id required")

        receipt = db.get_receipt(receipt_id)
        if not receipt:
            return _error(404, f"Receipt {receipt_id} not found")

        s3_key   = receipt.get("s3_key", "")
        store_id = store_id or receipt.get("retailer_id", "unknown")
        process_receipt(receipt_id, IMAGES_BUCKET, s3_key, store_id)

        updated = db.get_receipt(receipt_id)
        return _ok({"receipt_id": receipt_id, "status": updated.get("status")})

    except Exception as e:
        import traceback; traceback.print_exc()
        return _error(500, str(e))


def _ok(data):
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"success": True, **data}, default=str)
    }

def _error(status, message):
    return {
        "statusCode": status,
        "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({"success": False, "error": message})
    }
