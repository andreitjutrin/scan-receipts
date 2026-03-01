"""
Admin Lambda — backup management.
"""

import json
import os
import sys
sys.path.insert(0, "/var/task")

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
