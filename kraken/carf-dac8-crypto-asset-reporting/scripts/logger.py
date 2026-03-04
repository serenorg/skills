from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


class AuditLogger:
    def __init__(self, log_dir: str) -> None:
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)

    def _write(self, filename: str, payload: dict[str, Any]) -> None:
        path = self.log_dir / filename
        body = {
            "ts": datetime.now(tz=timezone.utc).isoformat(),
            **payload,
        }
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(body, sort_keys=True) + "\n")

    def log_event(self, event: str, payload: dict[str, Any]) -> None:
        self._write("events.jsonl", {"event": event, **payload})

    def log_error(self, stage: str, message: str, payload: dict[str, Any] | None = None) -> None:
        self._write(
            "errors.jsonl",
            {
                "stage": stage,
                "message": message,
                "payload": payload or {},
            },
        )
