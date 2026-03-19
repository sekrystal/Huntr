from __future__ import annotations

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.models import Base
from services.profile import extract_text_from_resume_upload, get_candidate_profile, ingest_resume


def test_resume_ingestion_populates_candidate_profile() -> None:
    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(engine)
    session = sessionmaker(bind=engine, expire_on_commit=False)()

    result = ingest_resume(
        session,
        filename="resume.txt",
        raw_text=(
            "Senior operator with 7+ years in AI and developer tools. "
            "Worked as chief of staff and deployment lead in San Francisco and New York."
        ),
    )

    profile = get_candidate_profile(session)
    assert result.resume_document_id is not None
    assert "chief of staff" in profile.preferred_titles_json
    assert profile.seniority_guess in {"senior", "staff"}


def test_text_resume_upload_extraction() -> None:
    text, warnings = extract_text_from_resume_upload("resume.txt", b"chief of staff\noperations\n")
    assert "chief of staff" in text
    assert warnings == []


def test_pdf_resume_upload_extraction() -> None:
    objects = []

    def add_obj(content: bytes) -> None:
        objects.append(content)

    add_obj(b"<< /Type /Catalog /Pages 2 0 R >>")
    add_obj(b"<< /Type /Pages /Kids [3 0 R] /Count 1 >>")
    add_obj(b"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 300 144] /Contents 4 0 R /Resources << /Font << /F1 5 0 R >> >> >>")
    stream = b"BT /F1 24 Tf 72 72 Td (Hello PDF Resume) Tj ET"
    add_obj(b"<< /Length %d >>\nstream\n%s\nendstream" % (len(stream), stream))
    add_obj(b"<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")

    pdf = b"%PDF-1.4\n"
    offsets = [0]
    for index, obj in enumerate(objects, start=1):
        offsets.append(len(pdf))
        pdf += f"{index} 0 obj\n".encode() + obj + b"\nendobj\n"
    xref_pos = len(pdf)
    pdf += f"xref\n0 {len(objects)+1}\n".encode()
    pdf += b"0000000000 65535 f \n"
    for offset in offsets[1:]:
        pdf += f"{offset:010d} 00000 n \n".encode()
    pdf += f"trailer\n<< /Size {len(objects)+1} /Root 1 0 R >>\nstartxref\n{xref_pos}\n%%EOF\n".encode()

    text, warnings = extract_text_from_resume_upload("resume.pdf", pdf)
    assert "Hello PDF Resume" in text
    assert warnings
