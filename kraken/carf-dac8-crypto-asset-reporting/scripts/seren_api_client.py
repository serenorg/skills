#!/usr/bin/env python3
"""Seren API key bootstrap and validation helper."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

try:
    import requests
except ImportError:  # pragma: no cover
    requests = None


class SerenAPIError(RuntimeError):
    pass


class SerenAPIKeyManager:
    def __init__(
        self,
        *,
        api_base_url: str = "https://api.serendb.com",
        env_file: str = ".env",
        timeout_seconds: int = 20,
    ) -> None:
        self.api_base_url = api_base_url.rstrip("/")
        self.env_file = Path(env_file)
        self.timeout_seconds = timeout_seconds

    def _read_key_from_env_file(self) -> str | None:
        if not self.env_file.exists():
            return None
        for line in self.env_file.read_text(encoding="utf-8").splitlines():
            if line.startswith("SEREN_API_KEY="):
                return line.split("=", 1)[1].strip()
        return None

    def _persist_key(self, key: str) -> None:
        lines: list[str] = []
        if self.env_file.exists():
            lines = self.env_file.read_text(encoding="utf-8").splitlines()

        replaced = False
        for idx, line in enumerate(lines):
            if line.startswith("SEREN_API_KEY="):
                lines[idx] = f"SEREN_API_KEY={key}"
                replaced = True
                break

        if not replaced:
            lines.append(f"SEREN_API_KEY={key}")

        self.env_file.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")

    def validate_existing_key(self, api_key: str) -> bool:
        if requests is None:
            return bool(api_key.strip())
        url = f"{self.api_base_url}/api/keys/validate"
        headers = {"Authorization": f"Bearer {api_key}"}
        try:
            response = requests.get(url, headers=headers, timeout=self.timeout_seconds)
        except requests.RequestException:
            return bool(api_key.strip())
        if response.status_code >= 400:
            return False
        try:
            body = response.json()
        except ValueError:
            return True
        if isinstance(body, dict) and isinstance(body.get("valid"), bool):
            return bool(body["valid"])
        return True

    def create_api_key(self) -> str:
        if requests is None:
            raise SerenAPIError("requests is required for SEREN_API_KEY auto-registration")

        url = f"{self.api_base_url}/api/keys"
        headers = {"Content-Type": "application/json"}
        token = (
            os.getenv("SEREN_BOOTSTRAP_TOKEN")
            or os.getenv("SEREN_AUTH_TOKEN")
            or ""
        ).strip()
        if token:
            headers["Authorization"] = f"Bearer {token}"

        payload = {
            "name": "carf-dac8-crypto-asset-reporting",
            "source": "skill-auto-register",
        }
        try:
            response = requests.post(
                url,
                headers=headers,
                data=json.dumps(payload),
                timeout=self.timeout_seconds,
            )
        except requests.RequestException as exc:
            raise SerenAPIError(f"Failed to call Seren API key endpoint: {exc}") from exc

        if response.status_code >= 400:
            raise SerenAPIError(
                "Seren API key auto-registration failed. "
                "Set SEREN_BOOTSTRAP_TOKEN or provide SEREN_API_KEY manually. "
                f"status={response.status_code} body={response.text[:200]}"
            )

        try:
            body: Any = response.json()
        except ValueError as exc:
            raise SerenAPIError("Seren API key endpoint returned invalid JSON") from exc

        candidates: list[str | None] = []
        if isinstance(body, dict):
            candidates.extend(
                [
                    body.get("api_key"),
                    body.get("key"),
                    body.get("token"),
                    body.get("value"),
                ]
            )
            nested = body.get("data")
            if isinstance(nested, dict):
                candidates.extend(
                    [
                        nested.get("api_key"),
                        nested.get("key"),
                        nested.get("token"),
                        nested.get("value"),
                    ]
                )

        for candidate in candidates:
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()

        raise SerenAPIError("Could not parse SEREN_API_KEY from Seren API response")

    def ensure_api_key(self, auto_register: bool = True) -> str:
        existing = (os.getenv("SEREN_API_KEY") or "").strip() or self._read_key_from_env_file()
        if existing and self.validate_existing_key(existing):
            os.environ["SEREN_API_KEY"] = existing
            return existing

        if not auto_register:
            raise SerenAPIError("SEREN_API_KEY missing or invalid and auto-registration disabled")

        created = self.create_api_key()
        self._persist_key(created)
        os.environ["SEREN_API_KEY"] = created
        return created
