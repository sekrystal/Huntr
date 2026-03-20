from __future__ import annotations

import argparse

from core.db import SessionLocal
from services.connector_admin import reset_connector_health


def main() -> None:
    parser = argparse.ArgumentParser(description="Reset persisted connector health state.")
    parser.add_argument("--connector", required=True, help="Connector name, for example greenhouse.")
    parser.add_argument("--confirm", action="store_true", help="Actually perform the reset.")
    args = parser.parse_args()

    if not args.confirm:
        raise SystemExit("Refusing to reset connector health without --confirm.")

    with SessionLocal() as session:
        row = reset_connector_health(session, args.connector)
        session.commit()
        print(f"Reset persisted connector health for {row.connector_name}. New status={row.status} circuit={row.circuit_state}")


if __name__ == "__main__":
    main()
