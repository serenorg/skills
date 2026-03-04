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
CREATE TABLE IF NOT EXISTS carf_raw_reports (
    id SERIAL PRIMARY KEY,
    report_id TEXT UNIQUE NOT NULL,
    casp_name TEXT NOT NULL,
    casp_jurisdiction TEXT NOT NULL,
    reporting_year INT NOT NULL,
    report_format TEXT NOT NULL,
    user_tin_hash TEXT,
    total_records INT,
    imported_at TIMESTAMPTZ DEFAULT NOW(),
    raw_metadata JSONB
);

CREATE TABLE IF NOT EXISTS carf_transactions (
    id SERIAL PRIMARY KEY,
    report_id TEXT REFERENCES carf_raw_reports(report_id),
    session_id TEXT NOT NULL,
    transaction_id TEXT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    transaction_type TEXT NOT NULL,
    sub_type TEXT,
    asset_acquired TEXT,
    quantity_acquired NUMERIC,
    asset_disposed TEXT,
    quantity_disposed NUMERIC,
    fiat_value NUMERIC,
    fiat_currency TEXT,
    fee NUMERIC,
    fee_currency TEXT,
    casp_name TEXT,
    jurisdiction TEXT,
    source_format TEXT,
    raw_data JSONB,
    UNIQUE(session_id, report_id, transaction_id)
);

CREATE TABLE IF NOT EXISTS user_transactions (
    id SERIAL PRIMARY KEY,
    session_id TEXT NOT NULL,
    transaction_id TEXT,
    timestamp TIMESTAMPTZ NOT NULL,
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
    source TEXT,
    raw_data JSONB
);

CREATE TABLE IF NOT EXISTS reconciliation_results (
    id SERIAL PRIMARY KEY,
    session_id TEXT NOT NULL,
    carf_transaction_id INT REFERENCES carf_transactions(id),
    user_transaction_id INT REFERENCES user_transactions(id),
    match_status TEXT NOT NULL,
    match_confidence NUMERIC,
    match_method TEXT,
    delta_quantity NUMERIC,
    delta_fiat_value NUMERIC,
    delta_timestamp_seconds INT,
    discrepancy_type TEXT,
    resolution TEXT,
    resolution_notes TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS reconciliation_sessions (
    id SERIAL PRIMARY KEY,
    session_id TEXT UNIQUE NOT NULL,
    user_id TEXT,
    started_at TIMESTAMPTZ DEFAULT NOW(),
    completed_at TIMESTAMPTZ,
    status TEXT DEFAULT 'in_progress',
    total_carf_records INT,
    total_user_records INT,
    matched_count INT,
    unmatched_carf_count INT,
    unmatched_user_count INT,
    discrepancy_count INT,
    auto_resolved_count INT,
    needs_review_count INT,
    cpa_escalation_count INT,
    jurisdictions JSONB,
    report_formats JSONB,
    summary JSONB
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

    def create_session(self, session_id: str, config: dict[str, Any], user_id: str = "") -> None:
        if not self.enabled:
            return
        self.connect()
        assert self.conn is not None
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO reconciliation_sessions (session_id, user_id, status, summary)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (session_id) DO NOTHING
                """,
                (session_id, user_id or None, "in_progress", json.dumps({"config": config})),
            )
        self.conn.commit()

    def close_session(
        self,
        session_id: str,
        status: str,
        summary: dict[str, Any],
        *,
        jurisdictions: dict[str, Any],
        report_formats: list[str],
    ) -> None:
        if not self.enabled:
            return
        self.connect()
        assert self.conn is not None

        with self.conn.cursor() as cur:
            cur.execute(
                """
                UPDATE reconciliation_sessions
                SET completed_at = NOW(),
                    status = %s,
                    total_carf_records = %s,
                    total_user_records = %s,
                    matched_count = %s,
                    unmatched_carf_count = %s,
                    unmatched_user_count = %s,
                    discrepancy_count = %s,
                    auto_resolved_count = %s,
                    needs_review_count = %s,
                    cpa_escalation_count = %s,
                    jurisdictions = %s,
                    report_formats = %s,
                    summary = %s
                WHERE session_id = %s
                """,
                (
                    status,
                    int(summary.get("total_carf_records", 0) or 0),
                    int(summary.get("total_user_records", 0) or 0),
                    int(summary.get("matched_count", 0) or 0),
                    int(summary.get("unmatched_carf_count", 0) or 0),
                    int(summary.get("unmatched_user_count", 0) or 0),
                    int(summary.get("discrepancy_count", 0) or 0),
                    int(summary.get("auto_resolved_count", 0) or 0),
                    int(summary.get("needs_review_count", 0) or 0),
                    int(summary.get("cpa_escalation_count", 0) or 0),
                    json.dumps(jurisdictions),
                    json.dumps(report_formats),
                    json.dumps(summary),
                    session_id,
                ),
            )
        self.conn.commit()

    def persist_raw_report(self, metadata: dict[str, Any]) -> None:
        if not self.enabled:
            return
        self.connect()
        assert self.conn is not None

        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO carf_raw_reports (
                    report_id, casp_name, casp_jurisdiction, reporting_year,
                    report_format, user_tin_hash, total_records, raw_metadata
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (report_id) DO UPDATE SET
                    casp_name = EXCLUDED.casp_name,
                    casp_jurisdiction = EXCLUDED.casp_jurisdiction,
                    reporting_year = EXCLUDED.reporting_year,
                    report_format = EXCLUDED.report_format,
                    user_tin_hash = EXCLUDED.user_tin_hash,
                    total_records = EXCLUDED.total_records,
                    raw_metadata = EXCLUDED.raw_metadata
                """,
                (
                    str(metadata.get("report_id") or ""),
                    str(metadata.get("casp_name") or "unknown_casp"),
                    str(metadata.get("casp_jurisdiction") or "UNKNOWN"),
                    int(metadata.get("reporting_year") or 2026),
                    str(metadata.get("report_format") or "CARF_XML"),
                    str(metadata.get("user_tin_hash") or "") or None,
                    int(metadata.get("total_records") or 0),
                    json.dumps(metadata),
                ),
            )
        self.conn.commit()

    def persist_carf_transactions(self, session_id: str, rows: list[dict[str, Any]]) -> dict[str, int]:
        if not self.enabled or not rows:
            return {}
        self.connect()
        assert self.conn is not None

        payloads: list[tuple[Any, ...]] = []
        for row in rows:
            payloads.append(
                (
                    str(row.get("report_id") or ""),
                    session_id,
                    str(row.get("transaction_id") or ""),
                    row.get("timestamp") or "1970-01-01T00:00:00+00:00",
                    str(row.get("transaction_type") or "exchange"),
                    str(row.get("sub_type") or "") or None,
                    str(row.get("asset_acquired") or "") or None,
                    float(row.get("quantity_acquired") or 0.0),
                    str(row.get("asset_disposed") or "") or None,
                    float(row.get("quantity_disposed") or 0.0),
                    float(row.get("fiat_value") or 0.0),
                    str(row.get("fiat_currency") or "") or None,
                    float(row.get("fee") or 0.0),
                    str(row.get("fee_currency") or "") or None,
                    str(row.get("casp_name") or "") or None,
                    str(row.get("jurisdiction") or "") or None,
                    str(row.get("source_format") or "") or None,
                    json.dumps(row.get("raw_data", {})),
                )
            )

        with self.conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO carf_transactions (
                    report_id, session_id, transaction_id, timestamp, transaction_type,
                    sub_type, asset_acquired, quantity_acquired, asset_disposed,
                    quantity_disposed, fiat_value, fiat_currency, fee, fee_currency,
                    casp_name, jurisdiction, source_format, raw_data
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s
                )
                ON CONFLICT (session_id, report_id, transaction_id) DO UPDATE SET
                    timestamp = EXCLUDED.timestamp,
                    transaction_type = EXCLUDED.transaction_type,
                    sub_type = EXCLUDED.sub_type,
                    asset_acquired = EXCLUDED.asset_acquired,
                    quantity_acquired = EXCLUDED.quantity_acquired,
                    asset_disposed = EXCLUDED.asset_disposed,
                    quantity_disposed = EXCLUDED.quantity_disposed,
                    fiat_value = EXCLUDED.fiat_value,
                    fiat_currency = EXCLUDED.fiat_currency,
                    fee = EXCLUDED.fee,
                    fee_currency = EXCLUDED.fee_currency,
                    casp_name = EXCLUDED.casp_name,
                    jurisdiction = EXCLUDED.jurisdiction,
                    source_format = EXCLUDED.source_format,
                    raw_data = EXCLUDED.raw_data
                """,
                payloads,
            )
            cur.execute(
                """
                SELECT id, transaction_id
                FROM carf_transactions
                WHERE session_id = %s
                """,
                (session_id,),
            )
            rows_map = {str(transaction_id): int(row_id) for row_id, transaction_id in cur.fetchall()}

        self.conn.commit()
        return rows_map

    def persist_user_transactions(self, session_id: str, rows: list[dict[str, Any]]) -> dict[str, int]:
        if not self.enabled or not rows:
            return {}
        self.connect()
        assert self.conn is not None

        payloads: list[tuple[Any, ...]] = []
        for row in rows:
            payloads.append(
                (
                    session_id,
                    str(row.get("transaction_id") or ""),
                    row.get("timestamp") or "1970-01-01T00:00:00+00:00",
                    str(row.get("transaction_type") or "exchange"),
                    str(row.get("sub_type") or "") or None,
                    str(row.get("asset_acquired") or "") or None,
                    float(row.get("quantity_acquired") or 0.0),
                    str(row.get("asset_disposed") or "") or None,
                    float(row.get("quantity_disposed") or 0.0),
                    float(row.get("fiat_value") or 0.0),
                    str(row.get("fiat_currency") or "") or None,
                    float(row.get("fee") or 0.0),
                    str(row.get("fee_currency") or "") or None,
                    str(row.get("source_format", row.get("source", "user_csv"))),
                    json.dumps(row.get("raw_data", {})),
                )
            )

        with self.conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO user_transactions (
                    session_id, transaction_id, timestamp, transaction_type, sub_type,
                    asset_acquired, quantity_acquired, asset_disposed, quantity_disposed,
                    fiat_value, fiat_currency, fee, fee_currency, source, raw_data
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s
                )
                """,
                payloads,
            )
            cur.execute(
                """
                SELECT id, transaction_id
                FROM user_transactions
                WHERE session_id = %s
                """,
                (session_id,),
            )
            rows_map = {str(transaction_id): int(row_id) for row_id, transaction_id in cur.fetchall()}

        self.conn.commit()
        return rows_map

    def persist_reconciliation_results(
        self,
        session_id: str,
        rows: list[dict[str, Any]],
        *,
        carf_id_map: dict[str, int],
        user_id_map: dict[str, int],
    ) -> None:
        if not self.enabled or not rows:
            return
        self.connect()
        assert self.conn is not None

        payloads: list[tuple[Any, ...]] = []
        for row in rows:
            payloads.append(
                (
                    session_id,
                    carf_id_map.get(str(row.get("carf_transaction_id", ""))),
                    user_id_map.get(str(row.get("user_transaction_id", ""))),
                    str(row.get("match_status") or ""),
                    float(row.get("match_confidence") or 0.0),
                    str(row.get("match_method") or ""),
                    float(row.get("delta_quantity") or 0.0),
                    float(row.get("delta_fiat_value") or 0.0),
                    int(row.get("delta_timestamp_seconds") or 0),
                    str(row.get("discrepancy_type") or "") or None,
                    str(row.get("resolution") or "") or None,
                    str(row.get("resolution_notes") or "") or None,
                )
            )

        with self.conn.cursor() as cur:
            cur.executemany(
                """
                INSERT INTO reconciliation_results (
                    session_id, carf_transaction_id, user_transaction_id, match_status,
                    match_confidence, match_method, delta_quantity, delta_fiat_value,
                    delta_timestamp_seconds, discrepancy_type, resolution, resolution_notes
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s
                )
                """,
                payloads,
            )
        self.conn.commit()
