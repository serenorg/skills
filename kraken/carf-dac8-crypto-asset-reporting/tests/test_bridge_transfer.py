from __future__ import annotations

from bridge_1099da import merge_bridge_records
from transfer_tracker import reconcile_transfers


def test_bridge_dedup_does_not_reuse_primary_record() -> None:
    primary = [
        {
            "transaction_id": "carf-1",
            "timestamp": "2026-01-10T10:00:00+00:00",
            "asset_disposed": "BTC",
            "quantity_disposed": 0.25,
            "source_format": "CARF_XML",
            "raw_data": {},
        }
    ]
    bridge = [
        {
            "transaction_id": "1099-1",
            "timestamp": "2026-01-10T10:00:10+00:00",
            "asset_disposed": "BTC",
            "quantity_disposed": 0.25,
            "source_format": "1099DA_CSV",
            "raw_data": {},
        },
        {
            "transaction_id": "1099-2",
            "timestamp": "2026-01-10T10:00:20+00:00",
            "asset_disposed": "BTC",
            "quantity_disposed": 0.25,
            "source_format": "1099DA_CSV",
            "raw_data": {},
        },
    ]

    merged, stats = merge_bridge_records(
        primary_records=primary,
        bridge_records=bridge,
        timestamp_tolerance_seconds=120,
        quantity_tolerance_pct=0.5,
    )
    assert stats["dual_reported"] == 1
    assert stats["bridge_added"] == 1
    assert len(merged) == 2


def test_transfer_tracker_flags_wash_sale_and_misclassification() -> None:
    carf_records = [
        {
            "transaction_id": "carf-transfer-1",
            "timestamp": "2026-01-10T00:00:00+00:00",
            "transaction_type": "relevant_transfer",
            "asset_disposed": "ETH",
            "quantity_disposed": 1.0,
            "asset_acquired": "",
            "quantity_acquired": 0.0,
        }
    ]
    user_records = [
        {
            "transaction_id": "user-sell-1",
            "timestamp": "2026-01-10T01:00:00+00:00",
            "transaction_type": "exchange",
            "asset_disposed": "ETH",
            "quantity_disposed": 1.0,
            "asset_acquired": "USD",
            "quantity_acquired": 3000.0,
        },
        {
            "transaction_id": "user-buy-1",
            "timestamp": "2026-01-20T01:00:00+00:00",
            "transaction_type": "exchange",
            "asset_disposed": "USD",
            "quantity_disposed": 3200.0,
            "asset_acquired": "ETH",
            "quantity_acquired": 1.0,
        },
    ]
    summary = reconcile_transfers(
        carf_records=carf_records,
        user_records=user_records,
        timestamp_tolerance_seconds=3600,
        quantity_tolerance_pct=0.5,
    )
    assert summary["unmatched_transfer_ids"] == ["carf-transfer-1"]
    assert summary["potential_transfer_as_disposition_ids"] == ["carf-transfer-1"]
    assert summary["potential_wash_sale_ids"] == ["carf-transfer-1"]
