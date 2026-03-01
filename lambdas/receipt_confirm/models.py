"""
Shared data models used by all Lambda functions.

Confidence thresholds (agreed v4):
  >= 0.92  → auto-categorise silently, no review needed
  0.75–0.91 → auto-categorise but flag for review, promoted to trusted after 3× confirmations
  0.50–0.74 → always flag for review, category shown as best guess
  < 0.50   → Unknown, user picks category from scratch

Trust levels:
  tentative → fuzzy score 0.75–0.91, shown in review list
  confident → score >= 0.92 OR confirmed 3× by user
  trusted   → seeded / manually corrected / confirmed 5× — never overwritten silently
"""

from __future__ import annotations
import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional
from pydantic import BaseModel, Field, field_validator


# ---------------------------------------------------------------------------
# THRESHOLDS — single source of truth, referenced throughout codebase
# ---------------------------------------------------------------------------
THRESHOLD_SILENT   = 0.92   # >= this: auto-categorise, no review
THRESHOLD_REVIEW   = 0.75   # >= this: auto-categorise, flag for review
THRESHOLD_GUESS    = 0.50   # >= this: flag for review, show as best guess
# < THRESHOLD_GUESS → Unknown, no guess made, user picks from scratch

TRUST_PROMOTE_CONFIDENT = 3   # confirmations to reach confident
TRUST_PROMOTE_TRUSTED   = 5   # confirmations to reach trusted
MAX_PROCESS_COUNT       = 3   # max Textract calls per receipt (cost safeguard)


# ---------------------------------------------------------------------------
# ENUMS
# ---------------------------------------------------------------------------
class ProcessingStatus(str, Enum):
    PENDING       = "pending"        # record created, photo not yet in S3
    PROCESSING    = "processing"     # Textract running
    COMPLETED     = "completed"      # all items matched, none flagged
    NEEDS_REVIEW  = "needs_review"   # processing done, some items need attention
    FAILED        = "failed"         # Textract error or max reprocess attempts reached
    REPROCESSING  = "reprocessing"   # user triggered reprocess, running again


class MatchSource(str, Enum):
    STORE_EXACT  = "store_exact"    # exact hit in store-scoped mappings table
    GLOBAL_EXACT = "global_exact"   # exact hit in global mappings table
    FUZZY        = "fuzzy"          # fuzzy match via RapidFuzz (in-memory)
    MANUAL       = "manual"         # user corrected
    UNKNOWN      = "unknown"        # nothing matched above 50%, user must pick


class TrustLevel(str, Enum):
    TENTATIVE = "tentative"   # fuzzy score 0.75–0.91, shown in review list
    CONFIDENT = "confident"   # score >= 0.92 or confirmed 3+ times
    TRUSTED   = "trusted"     # seeded, manually confirmed, or confirmed 5+ times


# ---------------------------------------------------------------------------
# RECEIPT LINE ITEM
# ---------------------------------------------------------------------------
class ReceiptItem(BaseModel):
    item_seq:         str          = Field(default_factory=lambda: str(uuid.uuid4())[:8])
    raw_name:         str                        # exactly as Textract returned it
    normalized_name:  str                        # cleaned, lowercase, no weights/brand
    category:         str          = "unknown"   # "unknown" until matched or user picks
    price:            str          = "0.00"      # string to avoid DynamoDB Decimal issues
    quantity:         str          = "1"
    match_confidence: float        = 0.0
    match_source:     MatchSource  = MatchSource.UNKNOWN
    trust:            TrustLevel   = TrustLevel.TENTATIVE
    needs_review:     bool         = False       # true if confidence < THRESHOLD_SILENT
    confirmed:        bool         = False       # true once user has reviewed

    @field_validator("match_confidence")
    @classmethod
    def clamp(cls, v):
        return max(0.0, min(1.0, float(v)))

    @property
    def review_reason(self) -> Optional[str]:
        """Human-readable reason why this item needs review — shown in PWA."""
        if self.match_source == MatchSource.UNKNOWN:
            return "No match found — please select a category"
        if self.match_confidence < THRESHOLD_GUESS:
            return "Very low confidence — please select a category"
        if self.match_confidence < THRESHOLD_REVIEW:
            return f"Low confidence ({self.match_confidence:.0%}) — please confirm"
        if self.match_confidence < THRESHOLD_SILENT:
            return f"Moderate confidence ({self.match_confidence:.0%}) — please confirm"
        return None


# ---------------------------------------------------------------------------
# RECEIPT HEADER
# ---------------------------------------------------------------------------
class Receipt(BaseModel):
    receipt_id:          str               = Field(default_factory=lambda: str(uuid.uuid4()))
    user_id:             str               = "default"
    retailer_id:         str               = "unknown"
    retailer_name:       Optional[str]     = None
    receipt_date:        str               = Field(
                             default_factory=lambda: datetime.now(timezone.utc).date().isoformat()
                         )
    total_amount:        Optional[str]     = None
    status:              ProcessingStatus  = ProcessingStatus.PENDING
    s3_key:              str               = ""
    item_count:          int               = 0
    needs_review_count:  int               = 0
    process_count:       int               = 0    # safety: incremented each Textract call
                                                  # Lambda refuses to run if >= MAX_PROCESS_COUNT
    error_message:       Optional[str]     = None
    created_at:          str               = Field(
                             default_factory=lambda: datetime.now(timezone.utc).isoformat()
                         )
    updated_at:          str               = Field(
                             default_factory=lambda: datetime.now(timezone.utc).isoformat()
                         )


# ---------------------------------------------------------------------------
# CATEGORY MAPPING — the hybrid learning store
# ---------------------------------------------------------------------------
class CategoryMapping(BaseModel):
    mapping_key:     str                    # "{store_id}#{normalized_name}"
    store_id:        str                    # "tesco" | "global"
    normalized_name: str
    category:        str
    confidence:      str          = "0.50"
    match_count:     int          = 0
    trust:           TrustLevel   = TrustLevel.TENTATIVE
    source:          str          = "fuzzy"
    created_at:      str          = Field(
                         default_factory=lambda: datetime.now(timezone.utc).isoformat()
                     )
    last_seen:       str          = Field(
                         default_factory=lambda: datetime.now(timezone.utc).isoformat()
                     )

    @staticmethod
    def make_key(store_id: str, normalized_name: str) -> str:
        return f"{store_id}#{normalized_name}"

    @property
    def confidence_float(self) -> float:
        return float(self.confidence)

    @property
    def is_trusted(self) -> bool:
        return self.trust == TrustLevel.TRUSTED

    def next_trust_level(self) -> Optional[TrustLevel]:
        if self.trust == TrustLevel.TENTATIVE and self.match_count >= TRUST_PROMOTE_CONFIDENT:
            return TrustLevel.CONFIDENT
        if self.trust == TrustLevel.CONFIDENT and self.match_count >= TRUST_PROMOTE_TRUSTED:
            return TrustLevel.TRUSTED
        return None


# ---------------------------------------------------------------------------
# API REQUEST / RESPONSE SHAPES
# ---------------------------------------------------------------------------

class UploadUrlRequest(BaseModel):
    filename:     str
    store_id:     str                  # required — user selects store before scanning
    content_type: str = "image/jpeg"


class UploadUrlResponse(BaseModel):
    receipt_id:  str
    upload_url:  str
    expires_in:  int = 300


class ReprocessRequest(BaseModel):
    receipt_id: str


class ConfirmItemRequest(BaseModel):
    item_seq: str
    category: str


class ConfirmReceiptRequest(BaseModel):
    items: list[ConfirmItemRequest]


class RestoreBackupRequest(BaseModel):
    backup_filename: str


class ApiResponse(BaseModel):
    success: bool          = True
    data:    Optional[Any] = None
    error:   Optional[str] = None

    @classmethod
    def ok(cls, data=None) -> "ApiResponse":
        return cls(success=True, data=data)

    @classmethod
    def err(cls, message: str) -> "ApiResponse":
        return cls(success=False, error=message)

    def to_response(self, status: int = 200) -> dict:
        return {
            "statusCode": status,
            "headers": {
                "Content-Type":                "application/json",
                "Access-Control-Allow-Origin":  "*",
                "Access-Control-Allow-Headers": "Content-Type,X-Api-Key"
            },
            "body": self.model_dump_json()
        }
