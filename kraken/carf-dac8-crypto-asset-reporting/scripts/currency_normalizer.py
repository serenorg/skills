from __future__ import annotations

from typing import Any

# Offline-friendly default rates to USD for deterministic local execution.
# For production, these can be replaced by cached ECB/Fed feed snapshots.
USD_RATES = {
    "USD": 1.0,
    "EUR": 1.08,
    "GBP": 1.27,
    "JPY": 0.0067,
    "CAD": 0.74,
    "AUD": 0.66,
    "SGD": 0.74,
}


def _usd_per(currency: str) -> float | None:
    code = (currency or "").upper().strip()
    return USD_RATES.get(code)


def _convert(value: float, from_ccy: str, to_ccy: str) -> tuple[float, float | None]:
    src = _usd_per(from_ccy)
    dst = _usd_per(to_ccy)
    if src is None or dst is None or dst == 0:
        return value, None
    usd_value = value * src
    return usd_value / dst, src / dst


def normalize_fiat_values(
    *,
    rows: list[dict[str, Any]],
    home_currency: str,
) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    home = (home_currency or "USD").upper().strip()

    for row in rows:
        updated = dict(row)
        raw_data = dict(updated.get("raw_data") or {})

        fiat_value = float(updated.get("fiat_value", 0.0) or 0.0)
        fiat_currency = str(updated.get("fiat_currency", "") or home).upper()

        value_home, rate = _convert(fiat_value, fiat_currency, home)
        updated["fiat_value_home"] = round(value_home, 8)
        updated["home_currency"] = home

        if rate is None:
            raw_data["currency_conversion_warning"] = (
                f"missing_fx_rate:{fiat_currency}->{home}; used raw fiat value"
            )
            updated["currency_conversion_missing"] = True
        else:
            updated["fx_rate_used"] = round(rate, 12)
            updated["currency_conversion_missing"] = False

        updated["raw_data"] = raw_data
        normalized.append(updated)

    return normalized
