from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.db import SessionLocal, init_db
from services.sync import sync_all


def main() -> None:
    init_db()
    with SessionLocal() as session:
        result = sync_all(session, include_rechecks=True)
        session.commit()
        print(result.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
