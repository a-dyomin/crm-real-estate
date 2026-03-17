import copy

from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.security import hash_password
from app.models.agency import Agency
from app.models.enums import SourceChannel, UserRole
from app.models.parser_source import ParserSource
from app.models.user import User


DEFAULT_TELEGRAM_FILTERS = {
    "commercial_only": True,
    "udmurtia_only": False,
    "require_transaction_keyword": True,
    "require_real_estate_keyword": True,
    "exclude_keywords": [
        "квартир",
        "жилой",
        "жилье",
        "новострой",
        "ипотек",
        "крипт",
        "майнинг",
    ],
}
DEFAULT_TELEGRAM_SEARCH = {
    "queries": [
        "#коммерческаянедвижимость",
        "#недвижимостьижевск",
        "коммерческая недвижимость удмуртия",
        "аренда офис ижевск",
        "склад ижевск",
    ],
    "discover_channels": True,
    "channels_limit": 10000,
    "posts_limit_per_query": 10000,
    "days_back": 30,
    "whitelist_enabled": False,
    "allowed_channels": [],
}


def _ensure_telegram_filters(extra_config: dict | None) -> dict:
    def _to_int(value: object, fallback: int) -> int:
        try:
            return int(value)  # type: ignore[arg-type]
        except Exception:
            return fallback

    config = copy.deepcopy(extra_config) if isinstance(extra_config, dict) else {"mode": "telegram_api_search"}
    raw_filters = config.get("telegram_filters")
    filters = copy.deepcopy(raw_filters) if isinstance(raw_filters, dict) else None
    if not isinstance(filters, dict):
        filters = dict(DEFAULT_TELEGRAM_FILTERS)
    filters.setdefault("commercial_only", True)
    filters.setdefault("udmurtia_only", False)
    filters.setdefault("require_transaction_keyword", True)
    filters.setdefault("require_real_estate_keyword", True)
    filters.setdefault("exclude_keywords", list(DEFAULT_TELEGRAM_FILTERS["exclude_keywords"]))
    config["telegram_filters"] = filters
    raw_search = config.get("telegram_search")
    search = copy.deepcopy(raw_search) if isinstance(raw_search, dict) else None
    if not isinstance(search, dict):
        search = dict(DEFAULT_TELEGRAM_SEARCH)
    search.setdefault("queries", list(DEFAULT_TELEGRAM_SEARCH["queries"]))
    search.setdefault("discover_channels", True)
    search.setdefault("channels_limit", 10000)
    search.setdefault("posts_limit_per_query", 10000)
    # High-volume mode for Telegram discovery/search: keep hard floor at 10k.
    search["channels_limit"] = max(_to_int(search.get("channels_limit"), 10000), 10000)
    search["posts_limit_per_query"] = max(_to_int(search.get("posts_limit_per_query"), 10000), 10000)
    search.setdefault("days_back", 30)
    search.setdefault("whitelist_enabled", False)
    search.setdefault("allowed_channels", [])
    config["telegram_search"] = search
    config.setdefault("mode", "telegram_api_search")
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
                    max_items_per_run=10000,
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
                    max_items_per_run=10000,
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
                    max_items_per_run=10000,
                    extra_config={"mode": "html"},
                ),
                ParserSource(
                    agency_id=agency.id,
                    name="Telegram Realty Feed",
                    source_channel=SourceChannel.telegram,
                    source_url="https://t.me",
                    city="Izhevsk",
                    region_code="RU-UDM",
                    is_active=True,
                    poll_minutes=1440,
                    max_items_per_run=10000,
                    extra_config={
                        "mode": "telegram_api_search",
                        "telegram_filters": dict(DEFAULT_TELEGRAM_FILTERS),
                        "telegram_search": dict(DEFAULT_TELEGRAM_SEARCH),
                    },
                ),
            ]
        )
    else:
        for source in sources:
            if source.poll_minutes < 60:
                source.poll_minutes = 1440
            if source.name == "Telegram Channel Template":
                source.name = "Telegram Realty Feed"
                source.source_url = "https://t.me"
                source.is_active = True
            if source.name == "Cian Commercial" and source.source_url == "https://www.cian.ru/rent/commercial/":
                source.source_url = "https://www.cian.ru/commercial/"
            if source.source_channel == SourceChannel.telegram:
                source.max_items_per_run = max(int(source.max_items_per_run or 10000), 10000)
                if source.source_url == "https://t.me/s/realty":
                    source.source_url = "https://t.me"
                source.extra_config = _ensure_telegram_filters(source.extra_config)
            if source.name in {"Avito Udmurtia Commercial", "Cian Commercial", "Domclick Commercial"}:
                source.extra_config = source.extra_config or {"mode": "html"}
                if source.last_success_at is None and source.last_error:
                    source.is_active = False
