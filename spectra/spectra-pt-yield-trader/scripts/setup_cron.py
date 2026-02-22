#!/usr/bin/env python3
"""Create/manage seren-cron jobs for the Spectra PT Yield Trader."""

from __future__ import annotations

import argparse
import json
import os
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


DEFAULT_API_BASE = "https://api.serendb.com"


class PublisherError(Exception):
    pass


class SerenPublisherClient:
    def __init__(self, api_key: str, base_url: str = DEFAULT_API_BASE):
        self.api_key = api_key
        normalized = base_url.rstrip("/")
        for suffix in ("/v1/publishers", "/publishers"):
            if normalized.endswith(suffix):
                normalized = normalized[: -len(suffix)]
        self.base_url = normalized.rstrip("/")

    def _request(
        self,
        *,
        method: str,
        path: str,
        body: dict[str, Any],
    ) -> dict[str, Any]:
        normalized_path = path if path.startswith("/") else f"/{path}"
        url = f"{self.base_url}{normalized_path}"
        method_upper = method.upper()

        data: bytes | None = None
        if method_upper != "GET":
            data = json.dumps(body or {}).encode("utf-8")

        request = Request(
            url=url,
            data=data,
            method=method_upper,
            headers={
                "Authorization": f"Bearer {self.api_key}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )

        try:
            with urlopen(request, timeout=30) as response:
                raw = response.read().decode("utf-8")
        except HTTPError as exc:
            details = exc.read().decode("utf-8", errors="replace")
            raise PublisherError(f"HTTP {exc.code} on {normalized_path}: {details}") from exc
        except URLError as exc:
            raise PublisherError(f"Connection failed on {normalized_path}: {exc}") from exc

        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise PublisherError(f"Invalid JSON from {normalized_path}: {raw[:200]}") from exc
        if not isinstance(parsed, dict):
            raise PublisherError(f"Response from {normalized_path} was not an object")
        return parsed

    def call(self, publisher: str, method: str, path: str, body: dict[str, Any]) -> dict[str, Any]:
        cleaned_path = path.strip()
        if cleaned_path in {"", "/"}:
            normalized_path = ""
        else:
            normalized_path = cleaned_path if cleaned_path.startswith("/") else f"/{cleaned_path}"

        try:
            return self._request(
                method=method,
                path=f"/publishers/{publisher}{normalized_path}",
                body=body,
            )
        except PublisherError as exc:
            raise PublisherError(f"{publisher} {exc}") from exc


def _build_client() -> SerenPublisherClient:
    api_key = os.environ.get("SEREN_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError("SEREN_API_KEY is required in the environment.")
    base_url = os.environ.get("SEREN_API_BASE_URL", DEFAULT_API_BASE)
    return SerenPublisherClient(api_key=api_key, base_url=base_url)


def create_job(
    client: SerenPublisherClient,
    *,
    name: str,
    schedule: str,
    url: str,
    method: str,
) -> dict:
    return client.call(
        publisher="seren-cron",
        method="POST",
        path="/api/v1/jobs",
        body={
            "name": name,
            "schedule": schedule,
            "url": url,
            "method": method,
        },
    )


def list_jobs(client: SerenPublisherClient) -> dict:
    return client.call(
        publisher="seren-cron",
        method="GET",
        path="/api/v1/jobs",
        body={},
    )


def pause_job(client: SerenPublisherClient, job_id: str) -> dict:
    return client.call(
        publisher="seren-cron",
        method="POST",
        path=f"/api/v1/jobs/{job_id}/pause",
        body={},
    )


def resume_job(client: SerenPublisherClient, job_id: str) -> dict:
    return client.call(
        publisher="seren-cron",
        method="POST",
        path=f"/api/v1/jobs/{job_id}/resume",
        body={},
    )


def delete_job(client: SerenPublisherClient, job_id: str) -> dict:
    return client.call(
        publisher="seren-cron",
        method="DELETE",
        path=f"/api/v1/jobs/{job_id}",
        body={},
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Manage seren-cron jobs for spectra-pt-yield-trader."
    )
    sub = parser.add_subparsers(dest="command", required=True)

    create = sub.add_parser("create", help="Create a cron job.")
    create.add_argument("--url", required=True, help="Trigger URL, e.g. http://localhost:8080/run")
    create.add_argument("--schedule", default="*/30 * * * *", help="Cron schedule expression.")
    create.add_argument("--name", default="spectra-pt-yield-trader", help="Cron job name.")
    create.add_argument("--method", default="POST", help="HTTP method (default: POST).")

    sub.add_parser("list", help="List cron jobs.")

    pause = sub.add_parser("pause", help="Pause a cron job.")
    pause.add_argument("--job-id", required=True, help="Cron job id.")

    resume = sub.add_parser("resume", help="Resume a paused cron job.")
    resume.add_argument("--job-id", required=True, help="Cron job id.")

    delete = sub.add_parser("delete", help="Delete a cron job.")
    delete.add_argument("--job-id", required=True, help="Cron job id.")

    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        client = _build_client()
        if args.command == "create":
            result = create_job(
                client,
                name=args.name,
                schedule=args.schedule,
                url=args.url,
                method=args.method.upper(),
            )
        elif args.command == "list":
            result = list_jobs(client)
        elif args.command == "pause":
            result = pause_job(client, args.job_id)
        elif args.command == "resume":
            result = resume_job(client, args.job_id)
        elif args.command == "delete":
            result = delete_job(client, args.job_id)
        else:  # pragma: no cover - argparse guards this
            raise RuntimeError(f"Unknown command: {args.command}")
    except (RuntimeError, PublisherError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}))
        return 1

    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
