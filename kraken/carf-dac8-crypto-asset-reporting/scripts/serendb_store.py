#!/usr/bin/env python3
"""Optional SerenDB persistence layer for CARF/DAC8 reconciliation."""

from __future__ import annotations

import json
from typing import Any

try:
    import psycopg
except ImportError:  # pragma: no cover
    psycopg = None


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS reconciliation_sessions (
    id SERIAL PRIMARY KEY,
    session_id TEXT UNIQUE NOT NULL,
    status TEXT NOT NULL,
    config JSONB NOT NULL,
    summary JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS carf_raw_reports (
    id SERIAL PRIMARY KEY,
    session_id TEXT NOT NULL,
    report_id TEXT,
    casp_name TEXT,
    casp_jurisdiction TEXT,
    report_format TEXT,
    total_records INT,
    raw_metadata JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS carf_transactions (
    id SERIAL PRIMARY KEY,
    session_id TEXT NOT NULL,
    transaction_id TEXT,
    timestamp TIMESTAMPTZ,
    transaction_type TEXT,
    sub_type TEXT,
    asset_acquired TEXT,
    quantity_acquired NUMERIC,
    asset_disposed TEXT,
    quantity_disposed NUMERIC,
    fiat_value NUMERIC,
    fiat_currency TEXT,
    fee NUMERIC,
    fee_currency TEXT,
    jurisdiction TEXT,
    casp_name TEXT,
    source_format TEXT,
    raw_data JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS user_transactions (
    id SERIAL PRIMARY KEY,
    session_id TEXT NOT NULL,
    transaction_id TEXT,
    timestamp TIMESTAMPTZ,
    transaction_type TEXT,
    asset_acquired TEXT,
    quantity_acquired NUMERIC,
    asset_disposed TEXT,
    quantity_disposed NUMERIC,
    fiat_value NUMERIC,
    fiat_currency TEXT,
    fee NUMERIC,
    fee_currency TEXT,
    source TEXT,
    raw_data JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS reconciliation_results (
    id SERIAL PRIMARY KEY,
    session_id TEXT NOT NULL,
    carf_transaction_id TEXT,
    user_transaction_id TEXT,
    match_status TEXT NOT NULL,
    match_confidence NUMERIC,
    match_method TEXT,
    delta_quantity NUMERIC,
    delta_fiat_value NUMERIC,
    delta_timestamp_seconds INT,
    discrepancy_type TEXT,
    resolution TEXT,
    resolution_notes TEXT,
    payload JSONB,
    created_at TIMESTAMPTZ DEFAULT NOW()
);
"""


class SerenDBStore:
    def __init__(self, dsn: str | None) -> None:
        self.dsn = (dsn or "").strip()
        self.conn = None

    @property
    def enabled(self) -> bool:
        return bool(self.dsn) and psycopg is not None

    def connect(self) -> None:
        if not self.enabled:
            return
        if self.conn is None:
            self.conn = psycopg.connect(self.dsn)

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def ensure_schema(self) -> bool:
        if not self.enabled:
            return False
        self.connect()
        assert self.conn is not None
        with self.conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
        self.conn.commit()
        return True

    def create_session(self, session_id: str, config: dict[str, Any]) -> None:
        if not self.enabled:
            return
        self.connect()
        assert self.conn is not None
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO reconciliation_sessions (session_id, status, config)
                VALUES (%s, %s, %s)
                ON CONFLICT (session_id) DO NOTHING
                """,
                (session_id, "in_progress", json.dumps(config)),
            )
        self.conn.commit()

    def close_session(self, session_id: str, status: str, summary: dict[str, Any]) -> None:
        if not self.enabled:
            return
        self.connect()
        assert self.conn is not None
        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE reconciliation_sessions
                SET status = %s, summary = %s, completed_at = NOW()
                WHERE session_id = %s
                """,
                (status, json.dumps(summary), session_id),
            )
        self.conn.commit()

    def persist_raw_report(self, session_id: str, metadata: dict[str, Any]) -> None:
        if not self.enabled:
            return
        self.connect()
        assert self.conn is not None
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO carf_raw_reports (
                    session_id, report_id, casp_name, casp_jurisdiction,
                    report_format, total_records, raw_metadata
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    session_id,
                    metadata.get("report_id"),
                    metadata.get("casp_name"),
                    metadata.get("casp_jurisdiction"),
                    metadata.get("report_format"),
                    metadata.get("total_records"),
                    json.dumps(metadata),
                ),
            )
        self.conn.commit()

    def persist_carf_transactions(self, session_id: str, rows: list[dict[str, Any]]) -> None:
        if not self.enabled or not rows:
            return
        self.connect()
        assert self.conn is not None
        with self.conn.cursor() as cur:
            for row in rows:
                cur.execute(
                    """
                    INSERT INTO carf_transactions (
                        session_id, transaction_id, timestamp, transaction_type, sub_type,
                        asset_acquired, quantity_acquired, asset_disposed, quantity_disposed,
                        fiat_value, fiat_currency, fee, fee_currency, jurisdiction,
                        casp_name, source_format, raw_data
                    ) VALUES (
                        %(session_id)s, %(transaction_id)s, %(timestamp)s, %(transaction_type)s, %(sub_type)s,
                        %(asset_acquired)s, %(quantity_acquired)s, %(asset_disposed)s, %(quantity_disposed)s,
                        %(fiat_value)s, %(fiat_currency)s, %(fee)s, %(fee_currency)s, %(jurisdiction)s,
                        %(casp_name)s, %(source_format)s, %(raw_data)s
                    )
                    """,
                    {
                        **row,
                        "session_id": session_id,
                        "raw_data": json.dumps(row.get("raw_data", {})),
                        "timestamp": row.get("timestamp") or None,
                    },
                )
        self.conn.commit()

    def persist_user_transactions(self, session_id: str, rows: list[dict[str, Any]]) -> None:
        if not self.enabled or not rows:
            return
        self.connect()
        assert self.conn is not None
        with self.conn.cursor() as cur:
            for row in rows:
                cur.execute(
                    """
                    INSERT INTO user_transactions (
                        session_id, transaction_id, timestamp, transaction_type,
                        asset_acquired, quantity_acquired, asset_disposed, quantity_disposed,
                        fiat_value, fiat_currency, fee, fee_currency, source, raw_data
                    ) VALUES (
                        %(session_id)s, %(transaction_id)s, %(timestamp)s, %(transaction_type)s,
                        %(asset_acquired)s, %(quantity_acquired)s, %(asset_disposed)s, %(quantity_disposed)s,
                        %(fiat_value)s, %(fiat_currency)s, %(fee)s, %(fee_currency)s, %(source)s, %(raw_data)s
                    )
                    """,
                    {
                        **row,
                        "session_id": session_id,
                        "source": row.get("source_format", row.get("source", "user_csv")),
                        "raw_data": json.dumps(row.get("raw_data", {})),
                        "timestamp": row.get("timestamp") or None,
                    },
                )
        self.conn.commit()

    def persist_reconciliation_results(self, session_id: str, rows: list[dict[str, Any]]) -> None:
        if not self.enabled or not rows:
            return
        self.connect()
        assert self.conn is not None
        with self.conn.cursor() as cur:
            for row in rows:
                cur.execute(
                    """
                    INSERT INTO reconciliation_results (
                        session_id, carf_transaction_id, user_transaction_id, match_status,
                        match_confidence, match_method, delta_quantity, delta_fiat_value,
                        delta_timestamp_seconds, discrepancy_type, resolution, resolution_notes, payload
                    ) VALUES (
                        %(session_id)s, %(carf_transaction_id)s, %(user_transaction_id)s, %(match_status)s,
                        %(match_confidence)s, %(match_method)s, %(delta_quantity)s, %(delta_fiat_value)s,
                        %(delta_timestamp_seconds)s, %(discrepancy_type)s, %(resolution)s, %(resolution_notes)s,
                        %(payload)s
                    )
                    """,
                    {
                        **row,
                        "session_id": session_id,
                        "payload": json.dumps(row),
                    },
                )
        self.conn.commit()
