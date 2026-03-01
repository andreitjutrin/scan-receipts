"""
Reference Lambda — categories, retailers reference data.
"""

import json
import os
import sys
sys.path.insert(0, "/var/task")

import dynamo_client as db


def lambda_handler(event, context):
    method = event.get("httpMethod", "")
    path   = event.get("path", "")

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

        else:
            return _error(404, "Not found")

    except Exception as e:
        import traceback; traceback.print_exc()
        return _error(500, str(e))


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
