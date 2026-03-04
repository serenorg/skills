from __future__ import annotations

from reconciliation_engine import ToleranceConfig, reconcile_transactions


def test_reconcile_transactions_mixed_results() -> None:
    carf = [
        {
            "transaction_id": "c1",
            "timestamp": "2026-01-10T10:00:00+00:00",
            "transaction_type": "exchange",
            "asset_disposed": "BTC",
            "quantity_disposed": 0.25,
            "fiat_value": 9000.0,
            "fee": 12.0,
        },
        {
            "transaction_id": "c2",
            "timestamp": "2026-01-11T10:00:00+00:00",
            "transaction_type": "exchange",
            "asset_disposed": "ETH",
            "quantity_disposed": 2.0,
            "fiat_value": 5000.0,
            "fee": 5.0,
        },
        {
            "transaction_id": "c3",
            "timestamp": "2026-01-12T10:00:00+00:00",
            "transaction_type": "exchange",
            "asset_disposed": "SOL",
            "quantity_disposed": 15.0,
            "fiat_value": 3000.0,
            "fee": 3.0,
        },
    ]

    user = [
        {
            "transaction_id": "u1",
            "timestamp": "2026-01-10T10:00:30+00:00",
            "transaction_type": "exchange",
            "asset_disposed": "BTC",
            "quantity_disposed": 0.25,
            "fiat_value": 9001.0,
            "fee": 12.0,
        },
        {
            "transaction_id": "u2",
            "timestamp": "2026-01-11T10:00:00+00:00",
            "transaction_type": "exchange",
            "asset_disposed": "ETH",
            "quantity_disposed": 1.95,
            "fiat_value": 4800.0,
            "fee": 5.0,
        },
    ]

    result = reconcile_transactions(
        carf_records=carf,
        user_records=user,
        tolerance=ToleranceConfig(
            timestamp_tolerance_seconds=24 * 60 * 60,
            quantity_tolerance_pct=0.5,
            fiat_tolerance_pct=1.0,
        ),
        materiality_threshold_usd=500.0,
    )

    summary = result["summary"]
    assert summary["matched_count"] >= 1
    assert summary["discrepancy_count"] >= 1
    assert summary["unmatched_carf_count"] >= 1
    assert summary["cpa_escalation_count"] >= 1
