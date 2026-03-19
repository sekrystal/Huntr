from __future__ import annotations

import json
import subprocess
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import requests

from core.config import get_settings
from core.logging import get_logger


logger = get_logger(__name__)

MOCK_GREENHOUSE_JOBS = [
    {
        "id": 2001,
        "title": "Founding Operations Lead",
        "absolute_url": "https://boards.greenhouse.io/cursor/jobs/2001",
        "location": {"name": "San Francisco, CA"},
        "updated_at": (datetime.now(timezone.utc) - timedelta(days=2)).isoformat(),
        "content": "Cursor is looking for a founding operations lead to build recruiting, finance, and planning systems for a fast-growing early-stage AI product.",
        "company_name": "Cursor",
        "company_domain": "cursor.com/ai",
    },
    {
        "id": 2002,
        "title": "Strategic Operations Lead",
        "absolute_url": "https://boards.greenhouse.io/linear/jobs/2002",
        "location": {"name": "Remote, US"},
        "updated_at": (datetime.now(timezone.utc) - timedelta(days=3)).isoformat(),
        "content": "Own planning cadences, internal systems, and cross-functional operating rhythm for a product-led developer tools company.",
        "company_name": "Linear",
        "company_domain": "linear.app/developer-tools",
    },
    {
        "id": 2003,
        "title": "Growth Intern",
        "absolute_url": "https://boards.greenhouse.io/startup/jobs/2003",
        "location": {"name": "New York, NY"},
        "updated_at": (datetime.now(timezone.utc) - timedelta(days=1)).isoformat(),
        "content": "Campus hiring for a summer growth intern. Entry level.",
        "company_name": "Launch Labs",
        "company_domain": "launchlabs.ai",
    },
    {
        "id": 2004,
        "title": "Rocket Propulsion Engineer",
        "absolute_url": "https://boards.greenhouse.io/orbit/jobs/2004",
        "location": {"name": "Los Angeles, CA"},
        "updated_at": (datetime.now(timezone.utc) - timedelta(days=2)).isoformat(),
        "content": "Design propulsion systems and test hardware. Specialized aerospace role.",
        "company_name": "Orbit Labs",
        "company_domain": "orbitlabs.space",
    },
    {
        "id": 2005,
        "title": "Chief of Staff",
        "absolute_url": "https://boards.greenhouse.io/archive/jobs/2005",
        "location": {"name": "San Francisco, CA"},
        "updated_at": (datetime.now(timezone.utc) - timedelta(days=45)).isoformat(),
        "content": "This position has been filled. Job no longer available.",
        "page_text": "position has been filled archived no longer accepting applications",
        "company_name": "ArchiveCo",
        "company_domain": "archiveco.ai",
    },
]


class GreenhouseConnector:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.last_error: str | None = None
        self.last_failure_classification: str | None = None
        self.last_quarantine_count: int = 0
        self.last_item_count: int = 0
        self.last_board_counts: dict[str, int] = {}

    def fetch(self, require_live: bool = False) -> tuple[list[dict], bool]:
        self.last_quarantine_count = 0
        self.last_item_count = 0
        self.last_failure_classification = None
        self.last_board_counts = {}
        if self.settings.greenhouse_tokens:
            try:
                self.last_error = None
                jobs = self._fetch_live(self.settings.greenhouse_tokens)
                self.last_item_count = len(jobs)
                return jobs, True
            except GreenhouseFetchError as exc:
                self.last_error = str(exc)
                self.last_failure_classification = exc.classification
                logger.warning("Falling back to mock Greenhouse data: %s", exc)
                if require_live or not self.settings.demo_mode:
                    raise
        elif require_live or not self.settings.demo_mode:
            raise GreenhouseFetchError("config_error", "No Greenhouse board tokens configured for live mode.")
        self.last_error = self.last_error or "Live Greenhouse boards unavailable; using demo listings."
        self.last_failure_classification = self.last_failure_classification or "demo_fallback"
        self.last_item_count = len(MOCK_GREENHOUSE_JOBS)
        return MOCK_GREENHOUSE_JOBS, False

    def _fetch_live(self, board_tokens: list[str]) -> list[dict]:
        jobs: list[dict] = []
        partial_failures: list[str] = []
        for token in board_tokens:
            try:
                board_jobs = self._fetch_board_with_retry(token)
                self.last_board_counts[token] = len(board_jobs)
                logger.info("Greenhouse board %s returned %s jobs before normalization.", token, len(board_jobs))
                for job in board_jobs:
                    normalized = self._sanitize_live_job(job, token)
                    if normalized is None:
                        self.last_quarantine_count += 1
                        continue
                    jobs.append(normalized)
            except GreenhouseFetchError as exc:
                partial_failures.append(f"{token}:{exc.classification}")
                logger.warning("Greenhouse board %s failed with %s", token, exc.classification)
        if not jobs and partial_failures:
            raise GreenhouseFetchError(
                "transient_network" if any("transient" in item for item in partial_failures) else "source_unavailable",
                f"Greenhouse live fetch failed for all boards: {', '.join(partial_failures)}",
            )
        if not jobs:
            self.last_error = "Greenhouse returned zero jobs across configured boards."
            self.last_failure_classification = "source_empty"
            return []
        if partial_failures:
            self.last_error = f"Partial Greenhouse fetch failures: {', '.join(partial_failures)}"
            self.last_failure_classification = "partial_failure"
        elif self.last_quarantine_count:
            self.last_error = f"Quarantined {self.last_quarantine_count} malformed live Greenhouse rows."
            self.last_failure_classification = "quarantined_rows"
        else:
            self.last_error = None
            self.last_failure_classification = None
        return jobs

    def _fetch_board_with_retry(self, token: str) -> list[dict]:
        last_error: GreenhouseFetchError | None = None
        for attempt in range(3):
            try:
                return self._fetch_board(token)
            except GreenhouseFetchError as exc:
                last_error = exc
                if not exc.retryable or attempt == 2:
                    break
                time.sleep(0.5 * (2**attempt))
        raise last_error or GreenhouseFetchError("unknown", f"Unknown Greenhouse failure for {token}")

    def _fetch_board(self, token: str) -> list[dict]:
        try:
            response = requests.get(
                f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs",
                params={"content": "true"},
                timeout=(5, 20),
            )
        except requests.Timeout as exc:
            return self._fetch_board_via_curl(token, f"{token} timed out via requests")
        except requests.RequestException as exc:
            return self._fetch_board_via_curl(token, f"{token} network error via requests: {exc}")

        if response.status_code == 429:
            raise GreenhouseFetchError("rate_limited", f"{token} rate limited by Greenhouse", retryable=True)
        if response.status_code in {401, 403}:
            raise GreenhouseFetchError("auth_error", f"{token} rejected authentication", retryable=False)
        if response.status_code == 404:
            raise GreenhouseFetchError("source_not_found", f"{token} board was not found", retryable=False)
        if 500 <= response.status_code < 600:
            raise GreenhouseFetchError("transient_network", f"{token} returned HTTP {response.status_code}", retryable=True)
        if response.status_code >= 400:
            raise GreenhouseFetchError("source_error", f"{token} returned HTTP {response.status_code}", retryable=False)

        try:
            payload = response.json()
        except ValueError as exc:
            raise GreenhouseFetchError("parsing_error", f"{token} returned invalid JSON", retryable=False) from exc

        jobs = payload.get("jobs")
        if not isinstance(jobs, list):
            raise GreenhouseFetchError("schema_drift", f"{token} payload missing jobs list", retryable=False)
        return jobs

    def _fetch_board_via_curl(self, token: str, request_failure: str) -> list[dict]:
        url = f"https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true"
        try:
            result = subprocess.run(
                ["curl", "-sS", "--max-time", "20", url],
                capture_output=True,
                text=True,
                timeout=25,
                check=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise GreenhouseFetchError("transient_network", f"{token} curl fallback timed out after request failure", retryable=True) from exc

        if result.returncode != 0:
            raise GreenhouseFetchError(
                "transient_network",
                f"{request_failure}; curl fallback failed for {token}: {result.stderr.strip() or result.returncode}",
                retryable=True,
            )

        try:
            payload = json.loads(result.stdout)
        except json.JSONDecodeError as exc:
            raise GreenhouseFetchError("parsing_error", f"{token} curl fallback returned invalid JSON", retryable=False) from exc

        jobs = payload.get("jobs")
        if not isinstance(jobs, list):
            raise GreenhouseFetchError("schema_drift", f"{token} curl fallback payload missing jobs list", retryable=False)

        self.last_error = f"Recovered {token} via curl fallback after requests transport failure."
        self.last_failure_classification = "recovering_transport"
        return jobs

    def _sanitize_live_job(self, job: dict, token: str) -> dict | None:
        title = (job.get("title") or "").strip()
        company_name = (job.get("company_name") or token.replace("-", " ").title()).strip()
        absolute_url = canonicalize_greenhouse_url(job.get("absolute_url"), token, job.get("id"))
        first_published = job.get("first_published") or job.get("updated_at")
        if not title or not company_name or not absolute_url or not first_published:
            return None
        if not absolute_url.startswith("http"):
            return None
        normalized = dict(job)
        normalized["title"] = title
        normalized["company_name"] = company_name
        normalized["absolute_url"] = absolute_url
        normalized["first_published"] = first_published
        normalized["source_board_token"] = token
        return normalized


class GreenhouseFetchError(RuntimeError):
    def __init__(self, classification: str, message: str, retryable: bool = True) -> None:
        super().__init__(message)
        self.classification = classification
        self.retryable = retryable


def canonicalize_greenhouse_url(url: str | None, board_token: str, job_id: str | int | None) -> str | None:
    if not url:
        return f"https://job-boards.greenhouse.io/{board_token}/jobs/{job_id}" if job_id else None

    parsed = urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        return f"https://job-boards.greenhouse.io/{board_token}/jobs/{job_id}" if job_id else None

    if parsed.netloc.endswith("stripe.com"):
        kept_query = urlencode([(key, value) for key, value in parse_qsl(parsed.query) if key == "gh_jid"])
        return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", kept_query, ""))

    return urlunparse((parsed.scheme, parsed.netloc, parsed.path, "", parsed.query, ""))
