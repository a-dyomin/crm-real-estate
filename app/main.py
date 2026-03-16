import asyncio
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.api.routes import auth, calls, dashboard, deals, health, leads, parser, properties, ui, users
from app.core.config import get_settings
from app.db.base import Base
from app.db.bootstrap import apply_runtime_migrations
from app.db.seed import seed_initial_data
from app.db.session import SessionLocal, engine
from app.services.parser_scheduler import parser_scheduler_loop

# Import models so SQLAlchemy metadata can discover all tables.
from app.models import agency, audit_log, call_record, deal, lead, parser_result, parser_run, parser_source, property, user  # noqa: F401

settings = get_settings()
Path(settings.media_dir).mkdir(parents=True, exist_ok=True)


@asynccontextmanager
async def lifespan(_: FastAPI):
    apply_runtime_migrations(engine)
    Base.metadata.create_all(bind=engine)
    with SessionLocal() as db:
        seed_initial_data(db)
        db.commit()
    stop_event = asyncio.Event()
    scheduler_task = asyncio.create_task(parser_scheduler_loop(stop_event))
    try:
        yield
    finally:
        stop_event.set()
        await scheduler_task


app = FastAPI(title=settings.app_name, version="0.1.0", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="app/static"), name="static")
app.mount("/media", StaticFiles(directory=settings.media_dir), name="media")

app.include_router(ui.router)
app.include_router(health.router, prefix=settings.api_prefix)
app.include_router(auth.router, prefix=settings.api_prefix)
app.include_router(users.router, prefix=settings.api_prefix)
app.include_router(dashboard.router, prefix=settings.api_prefix)
app.include_router(calls.router, prefix=settings.api_prefix)
app.include_router(properties.router, prefix=settings.api_prefix)
app.include_router(leads.router, prefix=settings.api_prefix)
app.include_router(deals.router, prefix=settings.api_prefix)
app.include_router(parser.router, prefix=settings.api_prefix)
