from __future__ import annotations

EXCHANGE_TYPES = {
    "exchange",
    "crypto_to_fiat",
    "crypto_to_crypto",
    "crypto_to_goods_services",
}

CARF_SUB_TYPE_DESCRIPTIONS = {
    "CARF401": "staking_reward",
    "CARF402": "crypto_backed_loan",
    "CARF403": "token_wrapping",
    "CARF404": "collateral_liquidation",
}


def canonical_exchange_type(value: str | None) -> str:
    token = (value or "exchange").strip().lower().replace("-", "_")
    if token in EXCHANGE_TYPES:
        return token
    if "transfer" in token:
        return "transfer"
    return "exchange"
