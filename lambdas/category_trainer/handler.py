"""
Reference Lambda ' categories, retailers reference data.
"""

import json
import os

import dynamo_client as db


def lambda_handler(event, context):
    method = event.get("httpMethod", "")
    path   = event.get("path", "")
    params = event.get("pathParameters") or {}

    try:
        if method == "GET" and "/categories" in path:
            categories = db.get_all_categories()
            return _ok({"categories": categories, "count": len(categories)})

        elif method == "GET" and "/retailers" in path:
            retailers = db.get_all_retailers()
            clean = [{"retailer_id": r["retailer_id"], "name": r["name"]}
                     for r in retailers if r["retailer_id"] != "unknown"]
            return _ok({"retailers": clean, "count": len(clean)})

        elif method == "POST" and "/retailers" in path:
            body = json.loads(event.get("body") or "{}")
            retailer_id = body.get("retailer_id") or body.get("name", "").lower().replace(" ", "_")
            if not retailer_id or not body.get("name"):
                return _error(400, "retailer_id and name are required")
            retailer = {
                "retailer_id":     retailer_id,
                "name":            body.get("name"),
                "header_patterns": body.get("header_patterns", []),
                "aliases":         body.get("aliases", []),
                "strip_prefixes":  body.get("strip_prefixes", []),
                "skip_patterns":   body.get("skip_patterns", [])
            }
            db.save_retailer(retailer)
            return _ok({"retailer_id": retailer_id, "created": True})

        # -------------------------------------------------------------------
        # ITEM TYPES ' curated list of item_type_id -> category_id
        # -------------------------------------------------------------------
        elif method == "GET" and "/item-types" in path:
            item_types = db.load_item_types()
            return _ok({"item_types": item_types, "count": len(item_types)})

        elif method == "POST" and "/item-types" in path:
            body         = json.loads(event.get("body") or "{}")
            item_type_id = body.get("item_type_id", "").strip().lower().replace(" ", "_")
            category_id  = body.get("category_id", "").strip()
            label        = body.get("label", "").strip()
            if not item_type_id or not category_id:
                return _error(400, "item_type_id and category_id are required")
            item_type = db.save_item_type(item_type_id, category_id, label or item_type_id)
            return _ok({"item_type": item_type, "created": True})

        elif method == "PUT" and "/item-types/" in path:
            item_type_id = params.get("item_type_id", "")
            if not item_type_id:
                return _error(400, "item_type_id is required in path")
            body        = json.loads(event.get("body") or "{}")
            category_id = body.get("category_id", "").strip()
            label       = body.get("label", "").strip()
            if not category_id:
                return _error(400, "category_id is required")
            existing = db.get_item_type(item_type_id)
            if not existing:
                return _error(404, f"item_type_id '{item_type_id}' not found")
            item_type = db.save_item_type(item_type_id, category_id,
                                          label or existing.get("label", item_type_id))
            return _ok({"item_type": item_type, "updated": True})

        # -------------------------------------------------------------------
        # MAPPINGS ' view existing OCR->item_type mappings
        # -------------------------------------------------------------------
        elif method == "GET" and "/mappings" in path:
            qp       = event.get("queryStringParameters") or {}
            store_id = qp.get("store_id")
            mappings = db.list_all_mappings(store_id)
            return _ok({"mappings": mappings, "count": len(mappings)})

        else:
            return _error(404, "Not found")

    except Exception as e:
        import traceback; traceback.print_exc()
        return _error(500, str(e))


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
