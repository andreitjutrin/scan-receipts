"""
Admin Lambda — backup management.
"""

import json
import os

import dynamo_client as db

EXPORTS_BUCKET = os.environ["EXPORTS_BUCKET"]


def lambda_handler(event, context):
    method = event.get("httpMethod", "")
    path   = event.get("path", "")

    try:
        if method == "GET" and "backups" in path:
            backups = db.list_backups(EXPORTS_BUCKET)
            return _ok({"backups": backups, "count": len(backups)})

        elif method == "POST" and "restore-backup" in path:
            body     = json.loads(event.get("body") or "{}")
            filename = body.get("backup_filename")
            if not filename:
                return _error(400, "backup_filename is required")
            success = db.restore_backup(EXPORTS_BUCKET, filename)
            return _ok({"restored": filename}) if success else _error(500, f"Failed to restore {filename}")

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
