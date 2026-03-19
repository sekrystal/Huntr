from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from ui.app import filter_and_sort_table


def test_filter_and_sort_table_filters_by_search_and_status() -> None:
    table = pd.DataFrame(
        [
            {
                "company": "Mercor",
                "title": "Deployment Strategist",
                "lead_type": "combined",
                "freshness": "fresh",
                "fit": "strong fit",
                "confidence": "high",
                "current_status": "applied",
                "surfaced_at_raw": pd.Timestamp("2026-03-18T10:00:00Z"),
                "posted_at_raw": pd.Timestamp("2026-03-17T10:00:00Z"),
            },
            {
                "company": "Linear",
                "title": "Strategic Operations Lead",
                "lead_type": "listing",
                "freshness": "fresh",
                "fit": "strong fit",
                "confidence": "high",
                "current_status": "",
                "surfaced_at_raw": pd.Timestamp("2026-03-18T09:00:00Z"),
                "posted_at_raw": pd.Timestamp("2026-03-16T10:00:00Z"),
            },
        ]
    )

    filtered = filter_and_sort_table(
        table,
        {
            "search": "merc",
            "lead_type": "all",
            "freshness": "all",
            "fit": "all",
            "status": "applied",
            "surfaced_since": None,
            "surfaced_until": None,
            "posted_since": None,
            "posted_until": None,
            "sort_mode": "Company A-Z",
        },
    )

    assert filtered["company"].tolist() == ["Mercor"]


def test_filter_and_sort_table_sorts_by_freshness_and_date() -> None:
    table = pd.DataFrame(
        [
            {
                "company": "RecentCo",
                "title": "Ops Lead",
                "lead_type": "listing",
                "freshness": "recent",
                "fit": "stretch",
                "confidence": "medium",
                "current_status": "",
                "surfaced_at_raw": pd.Timestamp("2026-03-18T09:00:00Z"),
                "posted_at_raw": pd.Timestamp("2026-03-12T10:00:00Z"),
            },
            {
                "company": "FreshCo",
                "title": "Chief of Staff",
                "lead_type": "listing",
                "freshness": "fresh",
                "fit": "strong fit",
                "confidence": "high",
                "current_status": "",
                "surfaced_at_raw": pd.Timestamp("2026-03-18T08:00:00Z"),
                "posted_at_raw": pd.Timestamp("2026-03-17T10:00:00Z"),
            },
        ]
    )

    filtered = filter_and_sort_table(
        table,
        {
            "search": "",
            "lead_type": "all",
            "freshness": "all",
            "fit": "all",
            "status": "all",
            "surfaced_since": None,
            "surfaced_until": None,
            "posted_since": None,
            "posted_until": None,
            "sort_mode": "Freshest first",
        },
    )

    assert filtered["company"].tolist() == ["FreshCo", "RecentCo"]
