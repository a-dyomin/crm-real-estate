from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.api.routes import auth, dashboard, deals, health, leads, parser, properties, ui, users
from app.core.config import get_settings
from app.db.base import Base
from app.db.bootstrap import apply_runtime_migrations
from app.db.seed import seed_initial_data
from app.db.session import SessionLocal, engine

# Import models so SQLAlchemy metadata can discover all tables.
from app.models import agency, audit_log, deal, lead, parser_result, property, user  # noqa: F401

settings = get_settings()


@asynccontextmanager
async def lifespan(_: FastAPI):
    apply_runtime_migrations(engine)
    Base.metadata.create_all(bind=engine)
    with SessionLocal() as db:
        seed_initial_data(db)
        db.commit()
    yield


app = FastAPI(title=settings.app_name, version="0.1.0", lifespan=lifespan)
app.mount("/static", StaticFiles(directory="app/static"), name="static")

app.include_router(ui.router)
app.include_router(health.router, prefix=settings.api_prefix)
app.include_router(auth.router, prefix=settings.api_prefix)
app.include_router(users.router, prefix=settings.api_prefix)
app.include_router(dashboard.router, prefix=settings.api_prefix)
app.include_router(properties.router, prefix=settings.api_prefix)
app.include_router(leads.router, prefix=settings.api_prefix)
app.include_router(deals.router, prefix=settings.api_prefix)
app.include_router(parser.router, prefix=settings.api_prefix)
