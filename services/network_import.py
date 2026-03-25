from __future__ import annotations

import csv
from io import StringIO
import re
from typing import Any


NETWORK_IMPORT_VERSION = "v1"
HEADER_ALIASES = {
    "name": "name",
    "full_name": "name",
    "contact": "name",
    "contact_name": "name",
    "first_name": "first_name",
    "last_name": "last_name",
    "company": "company",
    "current_company": "company",
    "employer": "company",
    "organization": "company",
    "title": "title",
    "role": "title",
    "headline": "title",
    "position": "title",
    "relationship": "relationship",
    "connection": "relationship",
    "connection_strength": "relationship",
    "degree": "relationship",
    "profile_url": "profile_url",
    "linkedin_url": "profile_url",
    "url": "profile_url",
    "notes": "notes",
    "comment": "notes",
    "location": "location",
    "city": "location",
}
COMPANY_SUFFIXES = {"inc", "llc", "ltd", "corp", "corporation", "co", "company"}


def parse_network_csv(filename: str, raw_text: str) -> dict[str, Any]:
    if not raw_text.strip():
        raise ValueError("Network import file is empty.")

    reader = csv.DictReader(StringIO(raw_text), dialect=_detect_csv_dialect(raw_text))
    if not reader.fieldnames:
        raise ValueError("Network import needs a header row.")

    contacts: list[dict[str, Any]] = []
    indexed_companies: set[str] = set()
    for row in reader:
        contact = _row_to_contact(row)
        if not contact:
            continue
        indexed_companies.update(contact["company_keys"])
        contacts.append(contact)

    if not contacts:
        raise ValueError("No usable network contacts were found. Include at least a name or company per row.")

    return {
        "version": NETWORK_IMPORT_VERSION,
        "source_filename": filename,
        "imported_at_note": "Imported locally in the Streamlit workbench.",
        "contacts": contacts,
        "import_summary": {
            "contact_count": len(contacts),
            "indexed_company_count": len(indexed_companies),
        },
        "guidance": "Referral suggestions stay local-first. JORB does not generate outreach or send messages.",
    }


def match_referral_paths(company_name: str, network_payload: dict[str, Any] | None, limit: int = 3) -> list[dict[str, Any]]:
    if not company_name.strip():
        return []

    target_key = normalize_company_name(company_name)
    if not target_key:
        return []

    contacts = list((network_payload or {}).get("contacts") or [])
    matches: list[dict[str, Any]] = []
    for contact in contacts:
        company_keys = {key for key in contact.get("company_keys", []) if key}
        if target_key not in company_keys:
            continue
        relationship = contact.get("relationship") or "network contact"
        title = contact.get("title") or "title unknown"
        company = contact.get("company") or company_name
        match_type = "direct_company"
        path_summary = f"{contact.get('name') or 'Unknown contact'} at {company} ({relationship})"
        matches.append(
            {
                "contact_name": contact.get("name") or "",
                "company": company,
                "title": title,
                "relationship": relationship,
                "profile_url": contact.get("profile_url") or "",
                "notes": contact.get("notes") or "",
                "location": contact.get("location") or "",
                "match_type": match_type,
                "adjacency_label": "Direct company contact",
                "path_summary": path_summary,
            }
        )

    matches.sort(
        key=lambda item: (
            _relationship_rank(item.get("relationship")),
            item.get("contact_name", "").lower(),
        )
    )
    return matches[:limit]


def normalize_company_name(value: str) -> str:
    lowered = re.sub(r"[^a-z0-9\s]", " ", value.lower())
    parts = [part for part in lowered.split() if part and part not in COMPANY_SUFFIXES]
    return " ".join(parts)


def _detect_csv_dialect(raw_text: str) -> csv.Dialect:
    sample = raw_text[:2048]
    try:
        return csv.Sniffer().sniff(sample, delimiters=",\t;")
    except csv.Error:
        return csv.get_dialect("excel")


def _row_to_contact(row: dict[str, Any]) -> dict[str, Any] | None:
    canonical: dict[str, str] = {}
    for key, value in row.items():
        if key is None:
            continue
        normalized_key = HEADER_ALIASES.get(_normalize_header(key))
        if not normalized_key:
            continue
        canonical[normalized_key] = str(value or "").strip()

    first_name = canonical.get("first_name", "")
    last_name = canonical.get("last_name", "")
    name = canonical.get("name") or " ".join(part for part in [first_name, last_name] if part).strip()
    company = canonical.get("company", "")
    if not name and not company:
        return None

    company_key = normalize_company_name(company)
    return {
        "name": name or company or "Unknown contact",
        "company": company,
        "company_keys": [company_key] if company_key else [],
        "title": canonical.get("title", ""),
        "relationship": canonical.get("relationship", ""),
        "profile_url": canonical.get("profile_url", ""),
        "notes": canonical.get("notes", ""),
        "location": canonical.get("location", ""),
    }


def _normalize_header(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")


def _relationship_rank(value: str | None) -> int:
    lowered = (value or "").lower()
    if "former teammate" in lowered or "manager" in lowered or "close" in lowered:
        return 0
    if "teammate" in lowered or "worked with" in lowered:
        return 1
    if "warm" in lowered or "friend" in lowered:
        return 2
    if "2nd" in lowered or "second" in lowered:
        return 3
    return 4
