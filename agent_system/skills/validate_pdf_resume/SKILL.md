# validate_pdf_resume

Use this skill when a change could affect resume ingestion or profile extraction.

## Goal

Confirm PDF resume parsing works locally and still updates the candidate profile.

## Validation Checklist

1. run `pytest tests/test_profile.py`
2. verify `pypdf` is installed from `requirements.txt`
3. upload a sample PDF or run the parsing path directly
4. confirm extracted text is stored and profile fields populate
5. confirm failures still allow pasted-text fallback

## Pass Criteria

- PDF text extraction returns non-empty text
- `/resume` succeeds
- candidate profile reflects the parsed content

