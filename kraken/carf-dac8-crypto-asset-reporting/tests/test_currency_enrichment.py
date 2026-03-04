from __future__ import annotations

from currency_normalizer import normalize_fiat_values
from enrichment import enrich_tax_treatments, resolve_cost_basis


def test_currency_normalization_converts_to_home_currency() -> None:
    rows = [
        {
            "transaction_id": "tx-eur",
            "fiat_value": 9000.0,
            "fiat_currency": "EUR",
            "raw_data": {},
        }
    ]
    normalized = normalize_fiat_values(rows=rows, home_currency="USD")
    assert normalized[0]["fiat_value_home"] == 9720.0
    assert normalized[0]["currency_conversion_missing"] is False


def test_enrichment_sets_tax_treatment_and_cost_basis() -> None:
    rows = [
        {
            "transaction_id": "buy-1",
            "timestamp": "2026-01-01T00:00:00+00:00",
            "transaction_type": "exchange",
            "sub_type": "CARF401",
            "asset_acquired": "BTC",
            "quantity_acquired": 1.0,
            "asset_disposed": "USD",
            "quantity_disposed": 0.0,
            "fiat_value_home": 10000.0,
            "raw_data": {},
        },
        {
            "transaction_id": "sell-1",
            "timestamp": "2026-02-15T00:00:00+00:00",
            "transaction_type": "exchange",
            "sub_type": "CARF404",
            "asset_acquired": "USD",
            "quantity_acquired": 0.0,
            "asset_disposed": "BTC",
            "quantity_disposed": 0.5,
            "fiat_value_home": 6000.0,
            "raw_data": {},
        },
    ]
    treated = enrich_tax_treatments(rows)
    assert treated[0]["tax_treatment"] == "income_fmv_at_receipt"
    assert treated[1]["tax_treatment"] == "forced_disposition_capital_event"

    resolved = resolve_cost_basis(treated, method="fifo")
    sell = next(item for item in resolved if item["transaction_id"] == "sell-1")
    assert sell["cost_basis_home"] == 5000.0
    assert sell["gain_loss_home"] == 1000.0
    assert sell["holding_period_days"] == 45
