from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable
from urllib.parse import parse_qs, unquote, urlparse
import re

import requests

from core.config import get_settings
from core.logging import get_logger


logger = get_logger(__name__)
RESULT_LINK_RE = re.compile(
    r'<a[^>]+class="result__a"[^>]+href="(?P<href>[^"]+)"[^>]*>(?P<title>.*?)</a>',
    re.IGNORECASE | re.DOTALL,
)
TAG_RE = re.compile(r"<[^>]+>")


@dataclass
class SearchDiscoveryResult:
    query_text: str
    title: str
    url: str
    source_surface: str = "duckduckgo_html"


class SearchDiscoveryConnector:
    def __init__(self) -> None:
        self.settings = get_settings()
        self.last_error: str | None = None

    def fetch(self, queries: list[str], require_live: bool = False) -> tuple[list[SearchDiscoveryResult], bool]:
        if self.settings.search_discovery_enabled and queries:
            try:
                self.last_error = None
                return self._fetch_live(queries), True
            except Exception as exc:
                self.last_error = str(exc)
                logger.warning("Search discovery failed: %s", exc)
                if require_live or not self.settings.demo_mode:
                    raise
        elif require_live or not self.settings.demo_mode:
            raise RuntimeError("Search discovery is disabled or has no queries configured.")
        self.last_error = self.last_error or "Search discovery disabled; no web search performed."
        return [], False

    def _fetch_live(self, queries: list[str]) -> list[SearchDiscoveryResult]:
        results: list[SearchDiscoveryResult] = []
        seen_urls: set[str] = set()
        for query_text in queries[: self.settings.search_discovery_query_limit]:
            response = requests.get(
                "https://duckduckgo.com/html/",
                params={"q": query_text},
                timeout=(5, 20),
                headers={"User-Agent": "OpportunityScout/1.0"},
            )
            response.raise_for_status()
            html = response.text
            found = 0
            for match in RESULT_LINK_RE.finditer(html):
                href = _extract_result_url(match.group("href"))
                title = _clean_html(match.group("title"))
                if not href or href in seen_urls:
                    continue
                if not _is_supported_job_surface(href):
                    continue
                results.append(SearchDiscoveryResult(query_text=query_text, title=title, url=href))
                seen_urls.add(href)
                found += 1
                if found >= self.settings.search_discovery_result_limit:
                    break
        return results


def _clean_html(value: str) -> str:
    return TAG_RE.sub("", value or "").strip()


def _extract_result_url(href: str) -> str | None:
    if not href:
        return None
    parsed = urlparse(href)
    if parsed.path.startswith("/l/"):
        uddg = parse_qs(parsed.query).get("uddg", [])
        if uddg:
            return unquote(uddg[0])
    if parsed.scheme and parsed.netloc:
        return href
    return None


def _is_supported_job_surface(url: str) -> bool:
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    path = parsed.path.lower()
    if "greenhouse.io" in host and "/jobs/" in path:
        return True
    if "ashbyhq.com" in host and path.count("/") >= 2:
        return True
    return False


def build_search_queries(
    core_titles: Iterable[str],
    adjacent_titles: Iterable[str],
    preferred_domains: Iterable[str],
    watchlist_items: Iterable[str],
) -> list[str]:
    queries: list[str] = []
    seen: set[str] = set()

    def add(query: str) -> None:
        query = query.strip()
        if not query or query in seen:
            return
        seen.add(query)
        queries.append(query)

    for company in list(watchlist_items)[:4]:
        for title in list(core_titles)[:2]:
            add(f'site:job-boards.greenhouse.io "{company}" "{title}"')
            add(f'site:jobs.ashbyhq.com "{company}" "{title}"')
    for domain in list(preferred_domains)[:3]:
        for title in list(core_titles)[:2]:
            add(f'site:job-boards.greenhouse.io "{domain}" "{title}"')
            add(f'site:jobs.ashbyhq.com "{domain}" "{title}"')
    for title in list(adjacent_titles)[:3]:
        add(f'site:job-boards.greenhouse.io "{title}"')
        add(f'site:jobs.ashbyhq.com "{title}"')
    return queries


def extract_discovered_greenhouse_tokens(results: list[SearchDiscoveryResult]) -> dict[str, list[str]]:
    discovered: dict[str, list[str]] = {}
    for result in results:
        parsed = urlparse(result.url)
        host = parsed.netloc.lower()
        path_parts = [part for part in parsed.path.split("/") if part]
        token = None
        if "job-boards.greenhouse.io" in host and len(path_parts) >= 2:
            token = path_parts[0]
        elif "boards.greenhouse.io" in host and len(path_parts) >= 2:
            token = path_parts[0]
        if token:
            discovered.setdefault(token, []).append(result.query_text)
    return discovered


def extract_discovered_ashby_orgs(results: list[SearchDiscoveryResult]) -> dict[str, list[str]]:
    discovered: dict[str, list[str]] = {}
    for result in results:
        parsed = urlparse(result.url)
        host = parsed.netloc.lower()
        path_parts = [part for part in parsed.path.split("/") if part]
        if "jobs.ashbyhq.com" not in host or not path_parts:
            continue
        org = path_parts[0]
        discovered.setdefault(org, []).append(result.query_text)
    return discovered
