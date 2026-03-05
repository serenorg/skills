#!/usr/bin/env python3
"""Fetch raw trade and ledger history from the Kraken REST API.

Uses HMAC-SHA512 authentication with read-only API keys.
Only requires Python standard library — no external dependencies.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
import time
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional

from common import parse_dt, to_float, write_json


KRAKEN_API_BASE = "https://api.kraken.com"


def _kraken_signature(urlpath: str, data: Dict[str, Any], secret: str) -> str:
    """Compute Kraken API signature (HMAC-SHA512)."""
    postdata = urllib.parse.urlencode(data)
    encoded = (str(data["nonce"]) + postdata).encode("utf-8")
    message = urlpath.encode("utf-8") + hashlib.sha256(encoded).digest()
    mac = hmac.new(base64.b64decode(secret), message, hashlib.sha512)
    return base64.b64encode(mac.digest()).decode("utf-8")


def _kraken_request(
    path: str,
    api_key: str,
    api_secret: str,
    params: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Make an authenticated POST request to the Kraken API."""
    url = KRAKEN_API_BASE + path
    data = params.copy() if params else {}
    data["nonce"] = str(int(time.time() * 1000))

    sig = _kraken_signature(path, data, api_secret)
    postdata = urllib.parse.urlencode(data).encode("utf-8")

    req = urllib.request.Request(url, data=postdata, method="POST")
    req.add_header("API-Key", api_key)
    req.add_header("API-Sign", sig)
    req.add_header("Content-Type", "application/x-www-form-urlencoded")

    with urllib.request.urlopen(req, timeout=30) as resp:
        payload = json.loads(resp.read().decode("utf-8"))

    errors = payload.get("error", [])
    if errors:
        raise RuntimeError(f"Kraken API error: {errors}")

    return payload.get("result", {})


def fetch_trades(
    api_key: str,
    api_secret: str,
    start: Optional[int] = None,
    end: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Fetch all closed trades, paginating through results."""
    all_trades: List[Dict[str, Any]] = []
    offset = 0

    while True:
        params: Dict[str, Any] = {"ofs": offset}
        if start is not None:
            params["start"] = start
        if end is not None:
            params["end"] = end

        result = _kraken_request(
            "/0/private/TradesHistory", api_key, api_secret, params
        )
        trades = result.get("trades", {})
        if not trades:
            break

        for trade_id, trade_data in trades.items():
            trade_data["trade_id"] = trade_id
            all_trades.append(trade_data)

        count = result.get("count", 0)
        offset += len(trades)
        if offset >= count:
            break

        time.sleep(1)  # Rate limit: respect Kraken's API limits

    return all_trades


def fetch_ledger(
    api_key: str,
    api_secret: str,
    start: Optional[int] = None,
    end: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """Fetch all ledger entries, paginating through results."""
    all_entries: List[Dict[str, Any]] = []
    offset = 0

    while True:
        params: Dict[str, Any] = {"ofs": offset}
        if start is not None:
            params["start"] = start
        if end is not None:
            params["end"] = end

        result = _kraken_request(
            "/0/private/Ledgers", api_key, api_secret, params
        )
        ledger = result.get("ledger", {})
        if not ledger:
            break

        for entry_id, entry_data in ledger.items():
            entry_data["ledger_id"] = entry_id
            all_entries.append(entry_data)

        count = result.get("count", 0)
        offset += len(ledger)
        if offset >= count:
            break

        time.sleep(1)

    return all_entries


def _parse_pair(pair: str) -> tuple:
    """Split a Kraken trading pair into base and quote assets.

    Kraken pairs use X/Z prefixes for crypto/fiat (e.g., XXBTZUSD).
    Also handles newer format without prefixes (e.g., BTCUSD).
    """
    fiat_codes = {"USD", "EUR", "GBP", "CAD", "JPY", "AUD", "CHF"}

    # Try common suffixes
    for fiat in fiat_codes:
        for suffix in [fiat, f"Z{fiat}"]:
            if pair.endswith(suffix):
                base = pair[: -len(suffix)]
                # Strip X prefix from crypto
                if base.startswith("X") and len(base) > 1:
                    base = base[1:]
                # Normalize well-known Kraken symbols
                base = {"XBT": "BTC", "XETH": "ETH", "ETH": "ETH"}.get(base, base)
                return (base, fiat)

    # Fallback: split in half
    mid = len(pair) // 2
    return (pair[:mid], pair[mid:])


def normalize_trades(raw_trades: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Normalize Kraken trades into the canonical schema for reconciliation."""
    normalized: List[Dict[str, Any]] = []
    for trade in raw_trades:
        pair = trade.get("pair", "")
        base, quote = _parse_pair(pair)
        trade_type = trade.get("type", "")  # "buy" or "sell"
        vol = to_float(trade.get("vol"))
        cost = to_float(trade.get("cost"))  # Total cost/proceeds in quote currency
        fee = to_float(trade.get("fee"))
        trade_time = trade.get("time")

        # Convert Unix timestamp to ISO format
        disposed_at = None
        if trade_time is not None:
            try:
                from datetime import datetime, timezone

                dt = datetime.fromtimestamp(float(trade_time), tz=timezone.utc)
                disposed_at = dt.isoformat()
            except (ValueError, TypeError, OSError):
                disposed_at = parse_dt(str(trade_time))

        # Only sells are dispositions for tax purposes
        if trade_type == "sell":
            normalized.append(
                {
                    "trade_id": trade.get("trade_id"),
                    "asset": base,
                    "quantity": vol,
                    "disposed_at": disposed_at,
                    "proceeds_usd": cost if quote == "USD" else None,
                    "fee_usd": fee if quote == "USD" else None,
                    "pair": pair,
                    "trade_type": trade_type,
                    "quote_currency": quote,
                    "raw": trade,
                }
            )

    return normalized


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch raw trade history from Kraken API"
    )
    parser.add_argument("--api-key", required=True, help="Kraken API key")
    parser.add_argument("--api-secret", required=True, help="Kraken API secret")
    parser.add_argument("--output", required=True, help="Output JSON path")
    parser.add_argument(
        "--tax-year",
        type=int,
        default=None,
        help="Filter trades to a specific tax year (e.g., 2025)",
    )
    args = parser.parse_args()

    start_ts = None
    end_ts = None
    if args.tax_year:
        from datetime import datetime, timezone

        start_ts = int(
            datetime(args.tax_year, 1, 1, tzinfo=timezone.utc).timestamp()
        )
        end_ts = int(
            datetime(args.tax_year + 1, 1, 1, tzinfo=timezone.utc).timestamp()
        )

    print(f"Fetching trades from Kraken API...")
    raw_trades = fetch_trades(
        args.api_key, args.api_secret, start=start_ts, end=end_ts
    )
    print(f"  Fetched {len(raw_trades)} raw trades")

    print(f"Fetching ledger entries from Kraken API...")
    raw_ledger = fetch_ledger(
        args.api_key, args.api_secret, start=start_ts, end=end_ts
    )
    print(f"  Fetched {len(raw_ledger)} ledger entries")

    normalized = normalize_trades(raw_trades)
    print(f"  Normalized {len(normalized)} sell dispositions")

    write_json(
        args.output,
        {
            "source": "kraken_api",
            "trade_count": len(raw_trades),
            "ledger_count": len(raw_ledger),
            "disposition_count": len(normalized),
            "dispositions": normalized,
            "raw_trades": raw_trades,
            "raw_ledger": raw_ledger,
        },
    )
    print(f"Output written to {args.output}")


if __name__ == "__main__":
    main()
