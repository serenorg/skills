from __future__ import annotations

from pathlib import Path

from csv_normalizer import parse_user_csv


def test_parse_user_csv_records_unparseable_numeric_warning(tmp_path: Path) -> None:
    csv_path = tmp_path / "user.csv"
    csv_path.write_text(
        "\n".join(
            [
                "transaction_id,timestamp,transaction_type,asset_disposed,quantity_disposed,fiat_value,fiat_currency",
                'u1,2026-01-10T10:00:00Z,exchange,BTC,N/A,"$9,000.50",USD',
            ]
        ),
        encoding="utf-8",
    )

    rows = parse_user_csv(csv_path)
    assert len(rows) == 1
    warnings = rows[0]["raw_data"]["parse_warnings"]
    assert any("quantity_disposed" in item for item in warnings)
