from __future__ import annotations

from datetime import datetime, timedelta, timezone

import requests

from core.config import get_settings
from core.logging import get_logger


logger = get_logger(__name__)

MOCK_ASHBY_JOBS = [
    {
        "id": "ashby-3001",
        "title": "Chief of Staff",
        "jobUrl": "https://jobs.ashbyhq.com/Granola/ashby-3001",
        "publishedDate": (datetime.now(timezone.utc) - timedelta(days=1)).isoformat(),
        "descriptionPlain": "Partner with the founders on strategic planning, recruiting coordination, and internal operating rhythm at an early-stage AI startup.",
        "location": {"location": "New York, NY"},
        "companyName": "Granola",
        "companyDomain": "granola.ai",
    },
    {
        "id": "ashby-3002",
        "title": "Deployment Strategist",
        "jobUrl": "https://jobs.ashbyhq.com/Mercor/ashby-3002",
        "publishedDate": (datetime.now(timezone.utc) - timedelta(hours=20)).isoformat(),
        "descriptionPlain": "Work with customers to deploy AI workflows and close the loop with product and engineering. Early-stage team.",
        "location": {"location": "Remote, US"},
        "companyName": "Mercor",
        "companyDomain": "mercor.com/ai",
    },
    {
        "id": "ashby-3003",
        "title": "Implementation Strategy Lead",
        "jobUrl": "https://jobs.ashbyhq.com/Vercel/ashby-3003",
        "publishedDate": (datetime.now(timezone.utc) - timedelta(days=2)).isoformat(),
        "descriptionPlain": "Help enterprise users roll out developer tooling and partner with customer teams on complex deployments.",
        "location": {"location": "Remote, US"},
        "companyName": "Vercel",
        "companyDomain": "vercel.com/developer-tools",
    },
]


class AshbyConnector:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.last_error: str | None = None

    def fetch(self, require_live: bool = False) -> tuple[list[dict], bool]:
        if self.settings.ashby_orgs:
            try:
                self.last_error = None
                return self._fetch_live(self.settings.ashby_orgs), True
            except Exception as exc:
                self.last_error = str(exc)
                logger.warning("Falling back to mock Ashby data: %s", exc)
                if require_live or not self.settings.demo_mode:
                    raise
        elif require_live or not self.settings.demo_mode:
            raise RuntimeError("No Ashby org keys configured for live mode.")
        self.last_error = self.last_error or "Live Ashby orgs unavailable; using demo listings."
        return MOCK_ASHBY_JOBS, False

    def _fetch_live(self, orgs: list[str]) -> list[dict]:
        jobs: list[dict] = []
        for org in orgs:
            response = requests.post(
                "https://jobs.ashbyhq.com/api/non-user-graphql",
                json={
                    "operationName": "ApiJobBoardWithTeams",
                    "variables": {"organizationHostedJobsPageName": org},
                    "query": "query ApiJobBoardWithTeams($organizationHostedJobsPageName: String!) { jobBoard: jobBoardWithTeams(organizationHostedJobsPageName: $organizationHostedJobsPageName) { jobs { id title jobUrl publishedDate descriptionPlain location { location } } } }",
                },
                timeout=15,
            )
            response.raise_for_status()
            payload = response.json()
            for job in payload.get("data", {}).get("jobBoard", {}).get("jobs", []):
                job["companyName"] = org.replace("-", " ").title()
                jobs.append(job)
        return jobs
