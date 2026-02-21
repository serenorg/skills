"""SerenDB persistence for Kraken Money Mode Router."""

from __future__ import annotations

import json
from typing import Any, Dict, List

import psycopg


class SerenDBStore:
    """Stores router sessions, answers, recommendations, and events in SerenDB."""

    def __init__(self, connection_string: str):
        self.connection_string = connection_string

    def _connect(self):
        return psycopg.connect(self.connection_string)

    def ensure_schema(self) -> None:
        ddl = """
        CREATE TABLE IF NOT EXISTS kraken_skill_sessions (
            session_id UUID PRIMARY KEY,
            profile_name TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS kraken_skill_answers (
            id BIGSERIAL PRIMARY KEY,
            session_id UUID NOT NULL REFERENCES kraken_skill_sessions(session_id) ON DELETE CASCADE,
            question_key TEXT NOT NULL,
            answer_value TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS kraken_skill_recommendations (
            id BIGSERIAL PRIMARY KEY,
            session_id UUID NOT NULL REFERENCES kraken_skill_sessions(session_id) ON DELETE CASCADE,
            rank_index INTEGER NOT NULL,
            mode_id TEXT NOT NULL,
            score NUMERIC NOT NULL,
            label TEXT NOT NULL,
            summary TEXT NOT NULL,
            reasons JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS kraken_skill_actions (
            id BIGSERIAL PRIMARY KEY,
            session_id UUID NOT NULL REFERENCES kraken_skill_sessions(session_id) ON DELETE CASCADE,
            mode_id TEXT NOT NULL,
            step_index INTEGER NOT NULL,
            action_text TEXT NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );

        CREATE TABLE IF NOT EXISTS kraken_skill_events (
            id BIGSERIAL PRIMARY KEY,
            session_id UUID NOT NULL REFERENCES kraken_skill_sessions(session_id) ON DELETE CASCADE,
            event_type TEXT NOT NULL,
            payload JSONB NOT NULL,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(ddl)
            conn.commit()

    def create_session(self, session_id: str, profile_name: str) -> None:
        query = """
        INSERT INTO kraken_skill_sessions (session_id, profile_name)
        VALUES (%s::uuid, %s);
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (session_id, profile_name))
            conn.commit()

    def save_answers(self, session_id: str, answers: Dict[str, str]) -> None:
        query = """
        INSERT INTO kraken_skill_answers (session_id, question_key, answer_value)
        VALUES (%s::uuid, %s, %s);
        """
        rows = [(session_id, key, value) for key, value in answers.items()]
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.executemany(query, rows)
            conn.commit()

    def save_recommendations(self, session_id: str, recommendations: List[Dict[str, Any]]) -> None:
        query = """
        INSERT INTO kraken_skill_recommendations (
            session_id,
            rank_index,
            mode_id,
            score,
            label,
            summary,
            reasons
        )
        VALUES (%s::uuid, %s, %s, %s, %s, %s, %s::jsonb);
        """
        rows = []
        for idx, rec in enumerate(recommendations, start=1):
            rows.append(
                (
                    session_id,
                    idx,
                    rec["mode_id"],
                    rec["score"],
                    rec["label"],
                    rec["summary"],
                    json.dumps(rec["reasons"]),
                )
            )

        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.executemany(query, rows)
            conn.commit()

    def save_actions(self, session_id: str, mode_id: str, actions: List[str]) -> None:
        query = """
        INSERT INTO kraken_skill_actions (session_id, mode_id, step_index, action_text)
        VALUES (%s::uuid, %s, %s, %s);
        """
        rows = [(session_id, mode_id, idx, text) for idx, text in enumerate(actions, start=1)]
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.executemany(query, rows)
            conn.commit()

    def save_event(self, session_id: str, event_type: str, payload: Dict[str, Any]) -> None:
        query = """
        INSERT INTO kraken_skill_events (session_id, event_type, payload)
        VALUES (%s::uuid, %s, %s::jsonb);
        """
        with self._connect() as conn:
            with conn.cursor() as cur:
                cur.execute(query, (session_id, event_type, json.dumps(payload)))
            conn.commit()
