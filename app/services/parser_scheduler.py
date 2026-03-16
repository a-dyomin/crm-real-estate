import asyncio

from app.core.config import get_settings
from app.db.session import SessionLocal
from app.services.parser_orchestrator import run_parser_for_all_agencies

settings = get_settings()


def _run_once(trigger: str) -> None:
    with SessionLocal() as db:
        try:
            run_parser_for_all_agencies(db=db, trigger=trigger)
            db.commit()
        except Exception:
            db.rollback()
            raise


async def parser_scheduler_loop(stop_event: asyncio.Event) -> None:
    if not settings.parser_scheduler_enabled:
        await stop_event.wait()
        return

    interval_seconds = max(300, int(settings.parser_poll_interval_minutes) * 60)
    while not stop_event.is_set():
        try:
            await asyncio.to_thread(_run_once, "scheduled")
        except Exception:
            # Keep scheduler alive even if one cycle crashes.
            pass
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
        except TimeoutError:
            continue

