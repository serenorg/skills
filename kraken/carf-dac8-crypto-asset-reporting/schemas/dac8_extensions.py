from __future__ import annotations

DAC8_EMONEY_TOKENS = {
    "EURC",
    "EUROC",
    "USDE",
    "EURI",
    "USDT",
    "USDC",
    "DAI",
    "BUSD",
    "TUSD",
    "FRAX",
    "GUSD",
    "USDP",
    "PYUSD",
}


def is_high_value_nft(*, asset: str, fiat_value: float, threshold_eur: float = 50_000.0) -> bool:
    token = (asset or "").upper()
    return ("NFT" in token) and fiat_value >= threshold_eur


def is_emoney_asset(asset: str) -> bool:
    token = (asset or "").upper()
    return token in DAC8_EMONEY_TOKENS or token.startswith("E-")
