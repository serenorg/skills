from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def normalize_asset(value: str | None) -> str:
    if not value:
        return ""
    token = str(value).strip().upper()
    return token.replace(" ", "").replace("/", "-")


def parse_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    raw = str(value).strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


@dataclass(slots=True)
class NormalizedTransaction:
    transaction_id: str
    timestamp: datetime | None
    transaction_type: str
    sub_type: str = ""
    asset_acquired: str = ""
    quantity_acquired: float = 0.0
    asset_disposed: str = ""
    quantity_disposed: float = 0.0
    fiat_value: float = 0.0
    fiat_currency: str = ""
    fee: float = 0.0
    fee_currency: str = ""
    jurisdiction: str = ""
    casp_name: str = ""
    source_format: str = ""
    raw_data: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "transaction_id": self.transaction_id,
            "timestamp": self.timestamp.isoformat() if self.timestamp else "",
            "transaction_type": self.transaction_type,
            "sub_type": self.sub_type,
            "asset_acquired": self.asset_acquired,
            "quantity_acquired": float(self.quantity_acquired),
            "asset_disposed": self.asset_disposed,
            "quantity_disposed": float(self.quantity_disposed),
            "fiat_value": float(self.fiat_value),
            "fiat_currency": self.fiat_currency,
            "fee": float(self.fee),
            "fee_currency": self.fee_currency,
            "jurisdiction": self.jurisdiction,
            "casp_name": self.casp_name,
            "source_format": self.source_format,
            "raw_data": dict(self.raw_data),
        }
