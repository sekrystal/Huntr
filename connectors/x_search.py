from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

import requests

from core.config import get_settings
from core.logging import get_logger
from core.models import SourceQuery


logger = get_logger(__name__)


def canonicalize_x_url(author_handle: str | None, post_id: str | int) -> str:
    handle = (author_handle or "unknown").lstrip("@").strip() or "unknown"
    return f"https://x.com/{handle}/status/{post_id}"


def live_x_status_url(post_id: str | int) -> str:
    return f"https://x.com/i/web/status/{post_id}"

MOCK_QUERY_LIBRARY = {
    "hiring founding ops": [
        {
            "url": canonicalize_x_url("@julesbuilds", "1001"),
            "author_handle": "@julesbuilds",
            "text": "We are hiring our first founding ops lead at Cursor in SF. If you love building systems from zero, reach out.",
            "published_at": (datetime.now(timezone.utc) - timedelta(hours=8)).isoformat(),
            "query_text": "hiring founding ops",
        }
    ],
    "deployment strategist hiring": [
        {
            "url": canonicalize_x_url("@mercor_team", "1002"),
            "author_handle": "@mercor_team",
            "text": "Mercor is hiring a deployment strategist to work with customers and product on live AI rollouts. Remote in the US.",
            "published_at": (datetime.now(timezone.utc) - timedelta(hours=12)).isoformat(),
            "query_text": "deployment strategist hiring",
        }
    ],
    "hiring chief of staff": [
        {
            "url": canonicalize_x_url("@ari_startups", "1003"),
            "author_handle": "@ari_startups",
            "text": "A stealth AI infra startup in NYC is hiring a chief of staff / bizops partner. DM if you have scaled early teams.",
            "published_at": (datetime.now(timezone.utc) - timedelta(hours=18)).isoformat(),
            "query_text": "hiring chief of staff",
        }
    ],
    "first ops hire": [
        {
            "url": canonicalize_x_url("@stealthbuilder", "1005"),
            "author_handle": "@stealthbuilder",
            "text": "Looking for a first ops hire for a stealth fintech team in SF. No job post yet, but this will be a real company-building role.",
            "published_at": (datetime.now(timezone.utc) - timedelta(hours=14)).isoformat(),
            "query_text": "first ops hire",
        }
    ],
    "systems builder startup hiring": [
        {
            "url": canonicalize_x_url("@opscraft", "1004"),
            "author_handle": "@opscraft",
            "text": "Hiring a business rhythm architect for an early-stage AI team. This is really an operations systems role for someone who loves turning chaos into repeatability.",
            "published_at": (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat(),
            "query_text": "systems builder startup hiring",
        }
    ],
}


class XSearchConnector:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.last_error: str | None = None

    def fetch(self, queries: list[SourceQuery], require_live: bool = False) -> tuple[list[dict], bool]:
        query_texts = [query.query_text for query in queries if query.status != "archived"]
        if self.settings.x_bearer_token:
            try:
                self.last_error = None
                return self._fetch_live(query_texts), True
            except Exception as exc:
                self.last_error = str(exc)
                logger.warning("Falling back to mock X signals: %s", exc)
                if require_live or not self.settings.demo_mode:
                    raise
        elif require_live or not self.settings.demo_mode:
            raise RuntimeError("No X bearer token configured for live mode.")
        self.last_error = self.last_error or "Live X unavailable; using demo signals."
        return self._fetch_mock(query_texts), False

    def _fetch_mock(self, query_texts: list[str]) -> list[dict]:
        results: dict[str, dict] = {}
        for query_text in query_texts:
            for library_query, signals in MOCK_QUERY_LIBRARY.items():
                if library_query in query_text.lower() or query_text.lower() in library_query:
                    for signal in signals:
                        payload = dict(signal)
                        payload["source_platform"] = "x_demo"
                        results[payload["url"]] = payload
        return list(results.values())

    def _fetch_live(self, queries: list[str]) -> list[dict]:
        headers = {"Authorization": f"Bearer {self.settings.x_bearer_token}"}
        results: list[dict] = []
        for query in queries[:8]:
            response = requests.get(
                "https://api.x.com/2/tweets/search/recent",
                params={
                    "query": query,
                    "max_results": 10,
                    "tweet.fields": "created_at,author_id",
                    "expansions": "author_id",
                    "user.fields": "username",
                },
                headers=headers,
                timeout=15,
            )
            response.raise_for_status()
            payload = response.json()
            users = payload.get("includes", {}).get("users", [])
            usernames_by_id = {
                user["id"]: user.get("username")
                for user in users
                if user.get("id")
            }
            for item in payload.get("data", []):
                username = usernames_by_id.get(item.get("author_id"))
                handle = f"@{username}" if username else None
                url = canonicalize_x_url(handle, item["id"]) if username else live_x_status_url(item["id"])
                results.append(
                    {
                        "url": url,
                        "author_handle": handle,
                        "text": item["text"],
                        "published_at": item.get("created_at"),
                        "query_text": query,
                        "source_platform": "x",
                    }
                )
        return results
