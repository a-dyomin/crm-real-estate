from app.db.session import SessionLocal
from app.services.parser_orchestrator import run_parser_for_all_agencies


def main() -> None:
    with SessionLocal() as db:
        runs = run_parser_for_all_agencies(db=db, trigger="manual_cli")
        print(f"Parser runs: {len(runs)}")
        for run in runs:
            print(f"Run {run.id}: status={run.status} sources={run.source_count} inserted={run.inserted_count}")


if __name__ == "__main__":
    main()
