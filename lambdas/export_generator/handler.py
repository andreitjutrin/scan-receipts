"""
Export Generator - deprecated, functionality merged into receipt_processor.
"""
import json

def lambda_handler(event, context):
    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps({"success": True, "message": "exports handled by processor"})
    }
