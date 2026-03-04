from __future__ import annotations

TRANSFER_TYPES = {
    "transfer",
    "relevant_transfer",
    "wallet_transfer",
    "exchange_to_exchange",
    "exchange_to_wallet",
}


def is_transfer_type(value: str | None) -> bool:
    token = (value or "").strip().lower().replace("-", "_")
    return token in TRANSFER_TYPES or "transfer" in token
