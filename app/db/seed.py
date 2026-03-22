import copy

from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.core.security import hash_password
from app.models.agency import Agency
from app.models.enums import (
    ActivationMode,
    DiscoverySeedType,
    SourceChannel,
    SourceHealthStatus,
    SourceState,
    UserRole,
)
from app.models.parser_source import ParserSource
from app.models.parser_template import ParserTemplate
from app.models.scheduled_job import ScheduledJob
from app.models.source_seed import SourceSeed
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

DEFAULT_PARSER_TEMPLATES = [
    {
        "key": "listing_grid_basic",
        "name": "Listing grid + detail",
        "template": {
            "version": "1.0",
            "type": "html_listing_grid",
            "list": {
                "item_selector": ".card, .item, .listing, .offer, .object",
                "link_selector": "a",
                "link_attr": "href",
                "pagination": {"type": "next_link", "next_selector": "a.next", "max_pages": 10},
            },
            "detail": {
                "fields": {
                    "title": {"selector": "h1"},
                    "description": {"selector": ".description, .text, .content"},
                    "price": {"selector": ".price, .cost"},
                    "area_sqm": {"selector": ".area, .sqm"},
                    "address_raw": {"selector": ".address, .location"},
                    "contact_phone": {"selector": "a[href^='tel:']", "attr": "href"},
                }
            },
        },
        "notes": "Universal listing grid with detail page.",
    },
    {
        "key": "agency_site_basic",
        "name": "Agency site listing",
        "template": {
            "version": "1.0",
            "type": "html_listing_grid",
            "list": {
                "item_selector": ".listing, .offer, .object, .card, .property",
                "link_selector": "a[href]",
                "link_attr": "href",
                "pagination": {"type": "next_link", "next_selector": "a.next, a.pagination-next", "max_pages": 10},
            },
            "detail": {
                "fields": {
                    "title": {"selector": "h1"},
                    "description": {"selector": ".description, .object-description, .content"},
                    "price": {"selector": ".price, .cost, .object-price"},
                    "area_sqm": {"selector": ".area, .square, .object-area"},
                    "address_raw": {"selector": ".address, .location, .object-address"},
                    "contact_phone": {"selector": "a[href^='tel:']", "attr": "href"},
                }
            },
        },
        "notes": "Agency catalogs with listing cards.",
    },
    {
        "key": "business_center_basic",
        "name": "Business center catalog",
        "template": {
            "version": "1.0",
            "type": "html_listing_grid",
            "list": {
                "item_selector": ".office, .space, .unit, .offer, .card",
                "link_selector": "a[href]",
                "link_attr": "href",
                "pagination": {"type": "next_link", "next_selector": "a.next, a.pagination-next", "max_pages": 10},
            },
            "detail": {
                "fields": {
                    "title": {"selector": "h1, .object-title"},
                    "description": {"selector": ".description, .text, .content"},
                    "price": {"selector": ".price, .rent, .cost"},
                    "area_sqm": {"selector": ".area, .square, .sqm"},
                    "address_raw": {"selector": ".address, .location"},
                    "contact_phone": {"selector": "a[href^='tel:']", "attr": "href"},
                }
            },
        },
        "notes": "Business center catalogs and commercial complexes.",
    },
    {
        "key": "jsonld_listing",
        "name": "JSON-LD Offer",
        "template": {
            "version": "1.0",
            "type": "jsonld",
            "detail": {
                "fields": {
                    "jsonld": {"selector": "script[type='application/ld+json']"},
                    "title": {"path": "name"},
                    "description": {"path": "description"},
                    "price": {"path": "offers.price"},
                    "price_currency": {"path": "offers.priceCurrency"},
                    "address_raw": {"path": "address.streetAddress"},
                }
            },
        },
        "notes": "JSON-LD extraction for schema.org offers.",
    },
    {
        "key": "telegram_channel",
        "name": "Telegram channel feed",
        "template": {
            "version": "1.0",
            "type": "telegram",
            "fields": {
                "title": {"path": "message"},
                "publication_date": {"path": "date"},
                "contact_phone": {"path": "phones"},
                "telegram_post_url": {"path": "url"},
            },
        },
        "notes": "Telegram channel collector schema.",
    },
    {
        "key": "pdf_brochure",
        "name": "PDF brochure parser",
        "template": {
            "version": "1.0",
            "type": "pdf",
            "fields": {
                "title": {"strategy": "first_heading"},
                "description": {"strategy": "full_text"},
                "contact_phone": {"strategy": "regex_phone"},
            },
        },
        "notes": "PDF brochure extraction with OCR fallback.",
    },
]

DEFAULT_DISCOVERY_SEEDS = [
    {
        "seed_type": DiscoverySeedType.url,
        "value": "https://www.avito.ru/udmurtiya/kommercheskaya_nedvizhimost",
        "priority": 100,
        "region": "RU-UDM",
        "enabled": True,
    },
    {
        "seed_type": DiscoverySeedType.url,
        "value": "https://izhevsk.cian.ru/commercial/",
        "priority": 95,
        "region": "RU-UDM",
        "enabled": True,
    },
    {
        "seed_type": DiscoverySeedType.url,
        "value": "https://domclick.ru/commerce",
        "priority": 90,
        "region": "RU-UDM",
        "enabled": True,
    },
    {
        "seed_type": DiscoverySeedType.url,
        "value": "https://realty.yandex.ru/izhevsk/kupit/kommercheskaya-nedvizhimost/",
        "priority": 90,
        "region": "RU-UDM",
        "enabled": True,
    },
    {
        "seed_type": DiscoverySeedType.url,
        "value": "https://realty.yandex.ru/izhevsk/snyat/kommercheskaya-nedvizhimost/",
        "priority": 90,
        "region": "RU-UDM",
        "enabled": True,
    },
    {
        "seed_type": DiscoverySeedType.domain,
        "value": "avito.ru",
        "priority": 70,
        "region": "RU-UDM",
        "enabled": True,
    },
    {
        "seed_type": DiscoverySeedType.domain,
        "value": "cian.ru",
        "priority": 70,
        "region": "RU-UDM",
        "enabled": True,
    },
    {
        "seed_type": DiscoverySeedType.domain,
        "value": "domclick.ru",
        "priority": 70,
        "region": "RU-UDM",
        "enabled": True,
    },
    {
        "seed_type": DiscoverySeedType.domain,
        "value": "realty.yandex.ru",
        "priority": 70,
        "region": "RU-UDM",
        "enabled": True,
    },
    {
        "seed_type": DiscoverySeedType.domain,
        "value": "bankrot.fedresurs.ru",
        "priority": 55,
        "region": "RU-UDM",
        "enabled": True,
    },
    {
        "seed_type": DiscoverySeedType.domain,
        "value": "bankruptcy.kommersant.ru",
        "priority": 55,
        "region": "RU-UDM",
        "enabled": True,
    },
    {
        "seed_type": DiscoverySeedType.domain,
        "value": "torgi.gov.ru",
        "priority": 55,
        "region": "RU-UDM",
        "enabled": True,
    },
    {
        "seed_type": DiscoverySeedType.keyword,
        "value": "коммерческая недвижимость Удмуртия",
        "priority": 60,
        "region": "RU-UDM",
        "enabled": True,
    },
    {
        "seed_type": DiscoverySeedType.keyword,
        "value": "аренда склад Ижевск",
        "priority": 60,
        "region": "RU-UDM",
        "enabled": True,
    },
    {
        "seed_type": DiscoverySeedType.keyword,
        "value": "помещение под офис Ижевск",
        "priority": 55,
        "region": "RU-UDM",
        "enabled": True,
    },
    {
        "seed_type": DiscoverySeedType.keyword,
        "value": "продажа коммерческой недвижимости Ижевск",
        "priority": 55,
        "region": "RU-UDM",
        "enabled": True,
    },
]

DEFAULT_SCHEDULED_JOBS = [
    {
        "job_key": "daily_discovery_job",
        "name": "Daily discovery",
        "schedule_type": "daily",
        "schedule_hour": 5,
        "schedule_minute": 0,
        "timezone": "Europe/Moscow",
        "enabled": True,
        "notes": "Primary daily discovery run (05:00 MSK).",
    },
    {
        "job_key": "daily_active_source_parse_job",
        "name": "Daily parsing for active sources",
        "schedule_type": "daily",
        "schedule_hour": 5,
        "schedule_minute": 10,
        "timezone": "Europe/Moscow",
        "enabled": True,
        "notes": "Runs after discovery to parse all active sources.",
    },
    {
        "job_key": "periodic_source_health_check",
        "name": "Periodic source health checks",
        "schedule_type": "interval",
        "interval_minutes": 45,
        "timezone": "Europe/Moscow",
        "enabled": True,
        "notes": "Lightweight health checks every 45 minutes.",
    },
    {
        "job_key": "retry_failed_jobs",
        "name": "Retry failed jobs",
        "schedule_type": "interval",
        "interval_minutes": 15,
        "timezone": "Europe/Moscow",
        "enabled": True,
        "notes": "Retries failed scheduled jobs with backoff.",
    },
    {
        "job_key": "high_priority_source_refresh",
        "name": "High priority source refresh",
        "schedule_type": "interval",
        "interval_minutes": 180,
        "timezone": "Europe/Moscow",
        "enabled": True,
        "notes": "Extra refresh for top priority sources.",
    },
]


def _ensure_avito_official_config(extra_config: dict | None) -> dict:
    config = copy.deepcopy(extra_config) if isinstance(extra_config, dict) else {}
    raw_avito = config.get("avito_api")
    avito_api = copy.deepcopy(raw_avito) if isinstance(raw_avito, dict) else {}
    avito_api.setdefault("status", ["active"])
    avito_api.setdefault("per_page", 100)
    avito_api.setdefault("max_pages", 200)
    avito_api.setdefault("with_item_details", True)
    avito_api.setdefault("details_limit", 300)
    config["avito_api"] = avito_api
    config["mode"] = "avito_official_api"
    return config


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


def _seed_discovery_registry(db: Session) -> None:
    existing_templates = {
        template.key: template for template in db.execute(select(ParserTemplate)).scalars().all()
    }
    for template in DEFAULT_PARSER_TEMPLATES:
        if template["key"] in existing_templates:
            continue
        db.add(
            ParserTemplate(
                key=template["key"],
                name=template["name"],
                template=template.get("template"),
                notes=template.get("notes"),
            )
        )

    existing_seeds = {
        (seed.seed_type, seed.value): seed for seed in db.execute(select(SourceSeed)).scalars().all()
    }
    for seed in DEFAULT_DISCOVERY_SEEDS:
        key = (seed["seed_type"], seed["value"])
        existing = existing_seeds.get(key)
        if not existing:
            db.add(SourceSeed(**seed))
            continue
        existing.priority = seed["priority"]
        existing.region = seed["region"]
        existing.enabled = seed["enabled"]


def _seed_scheduler_jobs(db: Session) -> None:
    existing_jobs = {job.job_key: job for job in db.execute(select(ScheduledJob)).scalars().all()}
    for job in DEFAULT_SCHEDULED_JOBS:
        existing = existing_jobs.get(job["job_key"])
        if not existing:
            db.add(ScheduledJob(**job))
            continue
        existing.name = job["name"]
        existing.schedule_type = job["schedule_type"]
        existing.schedule_hour = job.get("schedule_hour")
        existing.schedule_minute = job.get("schedule_minute")
        existing.interval_minutes = job.get("interval_minutes")
        existing.timezone = job.get("timezone", existing.timezone)
        existing.enabled = job.get("enabled", existing.enabled)
        existing.notes = job.get("notes", existing.notes)


def seed_initial_data(db: Session) -> None:
    settings = get_settings()
    avito_ready = bool(settings.avito_client_id and settings.avito_client_secret and settings.avito_user_id)
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
                    is_active=avito_ready,
                    poll_minutes=1440,
                    max_items_per_run=10000,
                    extra_config=_ensure_avito_official_config(None),
                ),
                ParserSource(
                    agency_id=agency.id,
                    name="Cian Commercial",
                    source_channel=SourceChannel.cian,
                    source_url="https://izhevsk.cian.ru/commercial/",
                    city="Izhevsk",
                    region_code="RU-UDM",
                    is_active=True,
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
                    is_active=True,
                    poll_minutes=1440,
                    max_items_per_run=10000,
                    extra_config={"mode": "html"},
                ),
                ParserSource(
                    agency_id=agency.id,
                    name="Yandex Realty Commercial",
                    source_channel=SourceChannel.yandex,
                    source_url="https://realty.yandex.ru/izhevsk/kupit/kommercheskaya-nedvizhimost/",
                    city="Izhevsk",
                    region_code="RU-UDM",
                    is_active=True,
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
                ParserSource(
                    agency_id=agency.id,
                    name="Bankruptcy Feed (Fedresurs)",
                    source_channel=SourceChannel.bankrupt,
                    source_url="https://bankrot.fedresurs.ru/",
                    city="Izhevsk",
                    region_code="RU-UDM",
                    is_active=False,
                    poll_minutes=1440,
                    max_items_per_run=10000,
                    extra_config={
                        "mode": "html",
                        "link_keywords": ["банкрот", "bankrot", "торги", "torgi", "auction"],
                    },
                ),
                ParserSource(
                    agency_id=agency.id,
                    name="Bankruptcy Auctions (Kommersant)",
                    source_channel=SourceChannel.bankrupt,
                    source_url="https://bankruptcy.kommersant.ru/",
                    city="Izhevsk",
                    region_code="RU-UDM",
                    is_active=False,
                    poll_minutes=1440,
                    max_items_per_run=10000,
                    extra_config={
                        "mode": "html",
                        "link_keywords": ["банкрот", "торги", "auction", "sale"],
                    },
                ),
            ]
        )
    else:
        existing_channels = {source.source_channel for source in sources}
        if SourceChannel.yandex not in existing_channels:
            db.add(
                ParserSource(
                    agency_id=agency.id,
                    name="Yandex Realty Commercial",
                    source_channel=SourceChannel.yandex,
                    source_url="https://realty.yandex.ru/izhevsk/kupit/kommercheskaya-nedvizhimost/",
                    city="Izhevsk",
                    region_code="RU-UDM",
                    is_active=True,
                    poll_minutes=1440,
                    max_items_per_run=10000,
                    extra_config={"mode": "html"},
                )
            )
        if SourceChannel.bankrupt not in existing_channels:
            db.add(
                ParserSource(
                    agency_id=agency.id,
                    name="Bankruptcy Feed (Fedresurs)",
                    source_channel=SourceChannel.bankrupt,
                    source_url="https://bankrot.fedresurs.ru/",
                    city="Izhevsk",
                    region_code="RU-UDM",
                    is_active=False,
                    poll_minutes=1440,
                    max_items_per_run=10000,
                    extra_config={
                        "mode": "html",
                        "link_keywords": ["банкрот", "bankrot", "торги", "torgi", "auction"],
                    },
                )
            )
        for source in sources:
            if source.poll_minutes < 60:
                source.poll_minutes = 1440
            if source.name == "Telegram Channel Template":
                source.name = "Telegram Realty Feed"
                source.source_url = "https://t.me"
                source.is_active = True
            if source.name == "Cian Commercial" and source.source_url in (
                "https://www.cian.ru/rent/commercial/",
                "https://www.cian.ru/commercial/",
            ):
                source.source_url = "https://izhevsk.cian.ru/commercial/"
            if source.source_channel == SourceChannel.avito:
                source.is_active = avito_ready
                source.max_items_per_run = max(int(source.max_items_per_run or 10000), 10000)
                source.extra_config = _ensure_avito_official_config(source.extra_config)
            if source.source_channel == SourceChannel.telegram:
                source.max_items_per_run = max(int(source.max_items_per_run or 10000), 10000)
                if source.source_url == "https://t.me/s/realty":
                    source.source_url = "https://t.me"
            if source.source_channel == SourceChannel.cian:
                source.is_active = True
            if source.source_channel == SourceChannel.domclick:
                source.is_active = True
            if source.source_channel == SourceChannel.yandex:
                source.is_active = True
                source.extra_config = source.extra_config or {"mode": "html"}
            if source.source_channel == SourceChannel.bankrupt:
                source.extra_config = source.extra_config or {
                    "mode": "html",
                    "link_keywords": ["банкрот", "bankrot", "торги", "torgi", "auction"],
                }
            if source.name in {"Cian Commercial", "Domclick Commercial"}:
                source.extra_config = source.extra_config or {"mode": "html"}
                if source.last_success_at is None and source.last_error:
                    source.is_active = False
            source.parse_frequency_minutes = max(int(source.parse_frequency_minutes or source.poll_minutes or 1440), 60)
            if source.parse_priority is None:
                source.parse_priority = 70 if source.source_channel in {SourceChannel.avito, SourceChannel.cian} else 50
            if source.health_status is None:
                source.health_status = SourceHealthStatus.new
            if source.source_state is None:
                source.source_state = SourceState.active if source.is_active else SourceState.paused
            if source.activation_mode is None:
                source.activation_mode = ActivationMode.manual

    _seed_discovery_registry(db)
    _seed_scheduler_jobs(db)
