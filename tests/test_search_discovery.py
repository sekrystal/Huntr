from __future__ import annotations

from connectors.search_web import (
    SearchDiscoveryResult,
    derive_search_results_from_extraction,
    extract_ats_identifiers_from_html,
)


def test_extract_ats_identifiers_from_careers_page_html() -> None:
    html = """
    <html>
      <head>
        <title>Acme Careers</title>
        <meta property="og:site_name" content="Acme AI" />
      </head>
      <body>
        <a href="https://job-boards.greenhouse.io/acme/jobs/123">View jobs</a>
        <a href="https://jobs.ashbyhq.com/acme/456">More jobs</a>
        <p>Remote US preferred</p>
      </body>
    </html>
    """

    extraction = extract_ats_identifiers_from_html(
        source_url="https://acme.ai/careers",
        html=html,
        final_url="https://acme.ai/careers",
    )

    assert extraction.company_name == "Acme AI"
    assert extraction.greenhouse_tokens == ["acme"]
    assert extraction.ashby_identifiers == ["acme"]
    assert "remote us" in extraction.geography_hints


def test_derive_search_results_from_extraction_creates_connector_ready_urls() -> None:
    html = '<a href="https://job-boards.greenhouse.io/acme/jobs/123">GH</a>'
    extraction = extract_ats_identifiers_from_html(
        source_url="https://acme.ai/careers",
        html=html,
        final_url="https://acme.ai/careers",
    )

    derived = derive_search_results_from_extraction("acme careers", extraction)

    assert derived
    assert isinstance(derived[0], SearchDiscoveryResult)
    assert derived[0].url == "https://job-boards.greenhouse.io/acme/jobs"
