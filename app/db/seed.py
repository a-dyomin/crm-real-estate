from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.security import hash_password
from app.models.agency import Agency
from app.models.enums import SourceChannel, UserRole
from app.models.parser_source import ParserSource
from app.models.user import User


DEFAULT_TELEGRAM_FILTERS = {"commercial_only": True, "udmurtia_only": True}


def _ensure_telegram_filters(extra_config: dict | None) -> dict:
    config = dict(extra_config or {"mode": "html"})
    filters = config.get("telegram_filters")
    if not isinstance(filters, dict):
        filters = dict(DEFAULT_TELEGRAM_FILTERS)
    filters.setdefault("commercial_only", True)
    filters.setdefault("udmurtia_only", True)
    config["telegram_filters"] = filters
    return config


def seed_initial_data(db: Session) -> None:
    settings = get_settings()
    agency_stmt: Select[tuple[Agency]] = select(Agency).where(Agency.id == 1)
    agency = db.execute(agency_stmt).scalar_one_or_none()
    if not agency:
        agency = Agency(id=1, name="Regional CRE Agency", region_code="RU-UDM")
        db.add(agency)
        db.flush()

    admin_stmt: Select[tuple[User]] = select(User).where(User.email == settings.default_admin_email)
    admin = db.execute(admin_stmt).scalar_one_or_none()
    if not admin:
        db.add(
            User(
                agency_id=agency.id,
                email=settings.default_admin_email,
                full_name="System Admin",
                role=UserRole.admin,
                phone="+79000000000",
                password_hash=hash_password(settings.default_admin_password),
                is_active=True,
            )
        )
    else:
        if not admin.password_hash:
            admin.password_hash = hash_password(settings.default_admin_password)
        admin.is_active = True

    source_stmt: Select[tuple[ParserSource]] = select(ParserSource).where(ParserSource.agency_id == agency.id)
    sources = db.execute(source_stmt).scalars().all()
    if not sources:
        db.add_all(
            [
                ParserSource(
                    agency_id=agency.id,
                    name="Avito Udmurtia Commercial",
                    source_channel=SourceChannel.avito,
                    source_url="https://www.avito.ru/udmurtiya/kommercheskaya_nedvizhimost",
                    city="Izhevsk",
                    region_code="RU-UDM",
                    is_active=False,
                    poll_minutes=1440,
                    max_items_per_run=20,
                    extra_config={"mode": "html"},
                ),
                ParserSource(
                    agency_id=agency.id,
                    name="Cian Commercial",
                    source_channel=SourceChannel.cian,
                    source_url="https://www.cian.ru/commercial/",
                    city="Izhevsk",
                    region_code="RU-UDM",
                    is_active=False,
                    poll_minutes=1440,
                    max_items_per_run=20,
                    extra_config={"mode": "html"},
                ),
                ParserSource(
                    agency_id=agency.id,
                    name="Domclick Commercial",
                    source_channel=SourceChannel.domclick,
                    source_url="https://domclick.ru/commerce",
                    city="Izhevsk",
                    region_code="RU-UDM",
                    is_active=False,
                    poll_minutes=1440,
                    max_items_per_run=20,
                    extra_config={"mode": "html"},
                ),
                ParserSource(
                    agency_id=agency.id,
                    name="Telegram Realty Feed",
                    source_channel=SourceChannel.telegram,
                    source_url="https://t.me/s/realty",
                    city="Izhevsk",
                    region_code="RU-UDM",
                    is_active=True,
                    poll_minutes=1440,
                    max_items_per_run=20,
                    extra_config={"mode": "html", "telegram_filters": dict(DEFAULT_TELEGRAM_FILTERS)},
                ),
            ]
        )
    else:
        for source in sources:
            if source.poll_minutes < 60:
                source.poll_minutes = 1440
            if source.name == "Telegram Channel Template":
                source.name = "Telegram Realty Feed"
                source.source_url = "https://t.me/s/realty"
                source.is_active = True
            if source.name == "Cian Commercial" and source.source_url == "https://www.cian.ru/rent/commercial/":
                source.source_url = "https://www.cian.ru/commercial/"
            if source.source_channel == SourceChannel.telegram:
                source.extra_config = _ensure_telegram_filters(source.extra_config)
            if source.name in {"Avito Udmurtia Commercial", "Cian Commercial", "Domclick Commercial"}:
                source.extra_config = source.extra_config or {"mode": "html"}
                if source.last_success_at is None and source.last_error:
                    source.is_active = False
