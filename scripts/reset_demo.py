from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from core.db import reset_sqlite_db
from scripts.seed_demo_data import main as seed_demo_main


def main() -> None:
    reset_sqlite_db()
    seed_demo_main()


if __name__ == "__main__":
    main()
