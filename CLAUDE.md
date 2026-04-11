# Grocery Receipt Scanner — Claude Instructions

## Approval Required Before Code Changes

**Always propose changes first and wait for explicit user approval before modifying any code files.**

This applies to all Lambda handlers, frontend HTML, infrastructure templates, deployment scripts, and shared/common modules — whether targeting dev or prod. Do not implement until the user confirms.

## General Project Notes

- AWS SAM project, eu-west-2, Python 3.12 x86_64 Lambdas
- Single HTML frontend (`receipt-scanner-app.html`)
- Shared layer at `lambdas/common/` (all Lambdas import from here — no local dynamo_client.py or models.py)
- Every update must be deployed to **both dev and prod**
- No hardcoded secrets in frontend code
- No auto-commit — always ask before committing
