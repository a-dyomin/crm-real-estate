from __future__ import annotations

from datetime import datetime, timedelta
import re
from typing import Any
from urllib.parse import parse_qs, quote_plus, urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from app.models.discovered_source import DiscoveredSource
from app.models.enums import (
    ActivationMode,
    DiscoveryRunStatus,
    DiscoverySeedType,
    DiscoveryStatus,
    DiscoveredSourceType,
    OnboardingPriority,
    SourceHealthStatus,
    SourceLinkType,
    SourceState,
    SourceFrontierStatus,
    SourceChannel,
)
from app.models.parser_result import ParserResult
from app.models.parser_source import ParserSource
from app.models.parser_template import ParserTemplate
from app.models.source_activation_event import SourceActivationEvent
from app.models.source_discovery_run import SourceDiscoveryRun
from app.models.source_frontier import SourceFrontier
from app.models.source_health_check import SourceHealthCheck
from app.models.source_link import SourceLink
from app.models.source_seed import SourceSeed
from app.models.source_state_history import SourceStateHistory
from app.models.source_template_match import SourceTemplateMatch

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)

SEARCH_QUERY_URL = "https://duckduckgo.com/html/?q="
SEARCH_ENGINE_DOMAINS = {
    "duckduckgo.com",
    "yandex.ru",
    "yandex.kz",
    "google.com",
    "www.google.com",
    "bing.com",
}

COMMERCIAL_KEYWORDS = (
    "коммерческ",
    "недвижим",
    "офис",
    "склад",
    "торгов",
    "помещен",
    "аренд",
    "продаж",
    "бизнес-центр",
    "бц",
    "warehouse",
    "office",
    "retail",
    "industrial",
)

SEARCH_KEYWORD_PACK = (
    "аренда офиса",
    "аренда склада",
    "продажа коммерческой недвижимости",
    "помещение свободного назначения",
    "торговое помещение",
    "бизнес-центр",
    "складской комплекс",
    "собственник",
    "без комиссии",
    "продажа здания",
    "индустриальная недвижимость",
    "производственное помещение",
)

AGENCY_KEYWORDS = ("агентство", "риэлтор", "broker", "agency")
DEVELOPER_KEYWORDS = ("застройщик", "developer", "девелопер")
BUSINESS_CENTER_KEYWORDS = ("бизнес-центр", "бизнес центр", "бц")
MALL_KEYWORDS = ("торговый центр", "тц", "mall")
AUCTION_KEYWORDS = ("банкрот", "торги", "auction")
GOV_KEYWORDS = (".gov.ru", "гос", "реестр")

LISTING_URL_HINTS = (
    "/commercial",
    "/kommerchesk",
    "/office",
    "/warehouse",
    "/sklad",
    "/arenda",
    "/rent",
    "/sale",
    "/prodazha",
    "/catalog",
    "/listing",
)

SITEMAP_LOC_RE = re.compile(r"<loc>([^<]+)</loc>", re.IGNORECASE)

PHONE_RE = re.compile(r"(?:\+7|8)[\s\-()]*\d{3}[\s\-()]*\d{3}[\s\-]*\d{2}[\s\-]*\d{2}")
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
DATE_RE = re.compile(r"\b(\d{1,2}[./]\d{1,2}[./]\d{2,4})\b")


def _normalize_domain(url: str) -> str:
    parsed = urlparse(url if "://" in url else f"https://{url}")
    hostname = parsed.netloc.lower()
    if hostname.startswith("www."):
        hostname = hostname[4:]
    return hostname


def _safe_url(value: str) -> str:
    if value.startswith("http://") or value.startswith("https://"):
        return value
    return f"https://{value}"


def _unwrap_search_url(url: str) -> str:
    parsed = urlparse(url)
    if parsed.netloc.endswith("duckduckgo.com") and parsed.path.startswith("/l/"):
        params = parse_qs(parsed.query)
        target = params.get("uddg")
        if target:
            return target[0]
    return url


def _enqueue_frontier(
    db: Session,
    *,
    url: str,
    domain: str,
    priority: int = 0,
    source_id: int | None = None,
    metadata: dict[str, Any] | None = None,
) -> None:
    stmt: Select[tuple[SourceFrontier]] = select(SourceFrontier).where(SourceFrontier.url == url)
    existing = db.execute(stmt).scalar_one_or_none()
    if existing:
        existing.priority = max(existing.priority, priority)
        if metadata:
            existing.metadata_json = {**(existing.metadata_json or {}), **metadata}
        return
    db.add(
        SourceFrontier(
            url=url,
            domain=domain,
            priority=priority,
            discovered_from_source_id=source_id,
            metadata_json=metadata,
        )
    )


def _auto_seed_candidates(db: Session) -> list[dict[str, Any]]:
    seeds: list[dict[str, Any]] = []
    sources = db.execute(select(ParserSource).where(ParserSource.is_active.is_(True))).scalars().all()
    for source in sources:
        if source.source_url:
            seeds.append(
                {
                    "seed_type": DiscoverySeedType.url,
                    "value": source.source_url,
                    "priority": 80,
                    "region": source.region_code,
                    "seed_origin": "active_source",
                }
            )
        domain = _normalize_domain(source.source_url)
        if domain:
            seeds.append(
                {
                    "seed_type": DiscoverySeedType.domain,
                    "value": domain,
                    "priority": 65,
                    "region": source.region_code,
                    "seed_origin": "active_source_domain",
                }
            )

    keyword_region = "RU-UDM"
    for keyword in SEARCH_KEYWORD_PACK:
        seeds.append(
            {
                "seed_type": DiscoverySeedType.keyword,
                "value": keyword,
                "priority": 60,
                "region": keyword_region,
                "seed_origin": "keyword_pack",
            }
        )

    recent_cutoff = datetime.utcnow() - timedelta(days=30)
    result_domains = (
        db.execute(
            select(ParserResult.raw_url)
            .where(ParserResult.created_at >= recent_cutoff, ParserResult.raw_url.is_not(None))
            .limit(400)
        )
        .scalars()
        .all()
    )
    for raw_url in result_domains:
        if not raw_url:
            continue
        domain = _normalize_domain(raw_url)
        if domain:
            seeds.append(
                {
                    "seed_type": DiscoverySeedType.domain,
                    "value": domain,
                    "priority": 55,
                    "region": keyword_region,
                    "seed_origin": "parser_results",
                }
            )

    frontier_rows = (
        db.execute(
            select(SourceFrontier)
            .where(SourceFrontier.status == SourceFrontierStatus.new)
            .order_by(SourceFrontier.priority.desc(), SourceFrontier.id.desc())
            .limit(50)
        )
        .scalars()
        .all()
    )
    for row in frontier_rows:
        seeds.append(
            {
                "seed_type": DiscoverySeedType.url,
                "value": row.url,
                "priority": row.priority,
                "region": keyword_region,
                "seed_origin": "frontier",
                "frontier_id": row.id,
            }
        )
        row.status = SourceFrontierStatus.processing
        row.last_checked_at = datetime.utcnow()

    return seeds


def _fetch(url: str) -> tuple[str | None, int | None, float]:
    start = datetime.utcnow()
    try:
        response = requests.get(
            url,
            headers={"User-Agent": USER_AGENT, "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8"},
            timeout=12,
        )
        duration = (datetime.utcnow() - start).total_seconds() * 1000
        if response.ok:
            return response.text or "", response.status_code, duration
        return None, response.status_code, duration
    except Exception:
        duration = (datetime.utcnow() - start).total_seconds() * 1000
        return None, None, duration


def _extract_links(soup: BeautifulSoup, base_url: str) -> list[tuple[str, str]]:
    links: list[tuple[str, str]] = []
    for anchor in soup.select("a[href]")[:400]:
        href = (anchor.get("href") or "").strip()
        if not href or href.startswith("#") or href.startswith("mailto:") or href.startswith("tel:"):
            continue
        absolute = urljoin(base_url, href)
        text = " ".join(anchor.get_text(" ", strip=True).split())
        links.append((absolute, text))
    return links


def _discover_sitemap_links(domain: str, *, limit: int = 80) -> list[str]:
    sitemap_url = f"https://{domain}/sitemap.xml"
    html, status_code, _ = _fetch(sitemap_url)
    if not html or not status_code or status_code >= 400:
        return []
    urls = []
    for match in SITEMAP_LOC_RE.findall(html):
        url = match.strip()
        if not url:
            continue
        if _listing_link_score(url) == 0:
            continue
        urls.append(url)
        if len(urls) >= limit:
            break
    return urls


def _listing_link_score(url: str) -> int:
    lowered = url.lower()
    return sum(1 for token in LISTING_URL_HINTS if token in lowered)


def _score_page(text: str, listing_links: int, contact_hits: int, date_hits: int) -> dict[str, float]:
    lowered = text.lower()
    keyword_hits = sum(1 for token in COMMERCIAL_KEYWORDS if token in lowered)
    relevance = min(100.0, keyword_hits * 10.0 + listing_links * 2.0)
    listing_density = min(100.0, listing_links * 4.0)
    contact_richness = min(100.0, contact_hits * 40.0)
    update_frequency = min(100.0, date_hits * 20.0)
    return {
        "relevance_score": relevance,
        "listing_density_score": listing_density,
        "contact_richness_score": contact_richness,
        "update_frequency_score": update_frequency,
    }


def _classify_source(domain: str, text: str) -> DiscoveredSourceType:
    lowered = text.lower()
    if domain.endswith(".t.me") or "t.me" in domain:
        return DiscoveredSourceType.telegram
    if any(token in lowered for token in AUCTION_KEYWORDS) or "bankrot" in domain:
        return DiscoveredSourceType.auction
    if any(token in lowered for token in GOV_KEYWORDS) or domain.endswith(".gov.ru"):
        return DiscoveredSourceType.government
    if any(token in lowered for token in BUSINESS_CENTER_KEYWORDS):
        return DiscoveredSourceType.business_center
    if any(token in lowered for token in MALL_KEYWORDS):
        return DiscoveredSourceType.mall
    if any(token in lowered for token in DEVELOPER_KEYWORDS):
        return DiscoveredSourceType.developer
    if any(token in lowered for token in AGENCY_KEYWORDS):
        return DiscoveredSourceType.agency
    if any(token in lowered for token in COMMERCIAL_KEYWORDS):
        return DiscoveredSourceType.classifieds
    return DiscoveredSourceType.unknown


def _priority_from_score(score: float) -> OnboardingPriority:
    if score >= 80:
        return OnboardingPriority.urgent
    if score >= 65:
        return OnboardingPriority.high
    if score >= 50:
        return OnboardingPriority.medium
    return OnboardingPriority.low


def _match_template(html: str, soup: BeautifulSoup, templates: list[ParserTemplate]) -> tuple[ParserTemplate | None, float, dict[str, Any] | None]:
    lowered = html.lower()
    jsonld_score = 0.0
    if "application/ld+json" in lowered and ("offer" in lowered or "realestate" in lowered):
        jsonld_score = 0.85

    card_candidates = soup.select(".card, .item, .listing, .offer, .object, .product")
    listing_score = 0.0
    if len(card_candidates) >= 6:
        listing_score = 0.7
    elif len(card_candidates) >= 3:
        listing_score = 0.5

    best_score = max(jsonld_score, listing_score)
    if best_score == 0:
        return None, 0.0, None

    template_key = "jsonld_listing" if jsonld_score >= listing_score else "listing_grid_basic"
    template = next((item for item in templates if item.key == template_key), None)
    if not template:
        return None, 0.0, None

    generated = {
        "version": "1.0",
        "template_key": template.key,
        "start_urls": [],
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
    }
    if template.key == "jsonld_listing":
        generated["detail"]["fields"]["jsonld"] = {"selector": "script[type='application/ld+json']"}
    return template, best_score, generated


def _upsert_discovered_source(
    db: Session,
    *,
    domain: str,
    root_url: str,
    parent_id: int | None,
    scores: dict[str, float],
    source_type: DiscoveredSourceType,
    metadata: dict[str, Any] | None,
    status_override: DiscoveryStatus | None = None,
) -> DiscoveredSource:
    stmt: Select[tuple[DiscoveredSource]] = select(DiscoveredSource).where(DiscoveredSource.domain == domain)
    existing = db.execute(stmt).scalar_one_or_none()
    now = datetime.utcnow()
    if existing:
        existing.last_seen_at = now
        existing.relevance_score = max(existing.relevance_score, scores["relevance_score"])
        existing.listing_density_score = max(existing.listing_density_score, scores["listing_density_score"])
        existing.contact_richness_score = max(existing.contact_richness_score, scores["contact_richness_score"])
        existing.update_frequency_score = max(existing.update_frequency_score, scores["update_frequency_score"])
        existing.source_type = source_type
        existing.discovery_status = status_override or DiscoveryStatus.classified
        existing.onboarding_priority = _priority_from_score(existing.relevance_score)
        if metadata:
            existing.metadata_json = {**(existing.metadata_json or {}), **metadata}
        return existing
    source = DiscoveredSource(
        domain=domain,
        root_url=root_url,
        source_type=source_type,
        discovery_status=status_override or DiscoveryStatus.classified,
        relevance_score=scores["relevance_score"],
        listing_density_score=scores["listing_density_score"],
        contact_richness_score=scores["contact_richness_score"],
        update_frequency_score=scores["update_frequency_score"],
        onboarding_priority=_priority_from_score(scores["relevance_score"]),
        discovery_parent_source_id=parent_id,
        first_seen_at=now,
        last_seen_at=now,
        metadata_json=metadata,
    )
    db.add(source)
    db.flush()
    return source


def _map_source_channel(domain: str, source_type: DiscoveredSourceType) -> SourceChannel:
    if "avito" in domain:
        return SourceChannel.avito
    if "cian" in domain:
        return SourceChannel.cian
    if "domclick" in domain:
        return SourceChannel.domclick
    if "realty.yandex" in domain or "yandex" in domain:
        return SourceChannel.yandex
    if "t.me" in domain or domain.endswith(".t.me"):
        return SourceChannel.telegram
    if source_type == DiscoveredSourceType.auction:
        return SourceChannel.bankrupt
    return SourceChannel.web


def _activation_decision(
    source: DiscoveredSource, template_confidence: float | None
) -> tuple[str, str]:
    relevance = source.relevance_score
    listing_density = source.listing_density_score
    contact_score = source.contact_richness_score
    template_score = template_confidence or 0.0

    if relevance < 40 or listing_density < 10:
        return "reject", "Low relevance or listing density."
    if relevance >= 70 and listing_density >= 25 and (template_score >= 0.7 or source.parser_template_id):
        return "activate", "High confidence auto-activation."
    if relevance >= 55 and listing_density >= 20 and contact_score >= 20 and (template_score >= 0.6):
        return "activate", "Medium confidence with contact signals."
    return "review", "Requires manual review."


def auto_activate_sources(db: Session, *, limit: int = 50) -> int:
    default_agency_id = (
        db.execute(select(ParserSource.agency_id).order_by(ParserSource.agency_id.asc()).limit(1)).scalar()
        or 1
    )
    candidates = (
        db.execute(
            select(DiscoveredSource)
            .where(
                DiscoveredSource.discovery_status.in_(
                    [DiscoveryStatus.matched, DiscoveryStatus.classified, DiscoveryStatus.ready_for_activation]
                )
            )
            .order_by(DiscoveredSource.relevance_score.desc(), DiscoveredSource.id.desc())
            .limit(limit)
        )
        .scalars()
        .all()
    )
    if not candidates:
        return 0

    template_matches = (
        db.execute(select(SourceTemplateMatch).order_by(SourceTemplateMatch.matched_at.desc())).scalars().all()
    )
    latest_match: dict[int, SourceTemplateMatch] = {}
    for match in template_matches:
        if match.discovered_source_id not in latest_match:
            latest_match[match.discovered_source_id] = match

    activated = 0
    for source in candidates:
        match = latest_match.get(source.id)
        decision, reason = _activation_decision(source, match.confidence if match else None)
        if decision == "reject":
            source.discovery_status = DiscoveryStatus.rejected
            continue
        if decision == "review":
            source.discovery_status = DiscoveryStatus.ready_for_activation
            continue

        channel_value = _map_source_channel(source.domain, source.source_type)
        existing = (
            db.execute(select(ParserSource).where(ParserSource.source_url == source.root_url))
            .scalars()
            .first()
        )
        if existing:
            existing.is_active = True
            if existing.source_state != SourceState.active:
                db.add(
                    SourceStateHistory(
                        parser_source_id=existing.id,
                        from_state=existing.source_state,
                        to_state=SourceState.active,
                        reason="Auto-reactivated by discovery.",
                    )
                )
            existing.source_state = SourceState.active
            existing.activation_mode = ActivationMode.automatic
            existing.auto_discovered = True
            existing.auto_activation_reason = reason
            existing.last_discovery_at = datetime.utcnow()
            source.discovery_status = DiscoveryStatus.active
            activated += 1
            continue

        extra_config: dict[str, Any] = {"mode": "html"}
        if match and match.generated_parser_config:
            extra_config["auto_template_key"] = match.parser_template_id
            extra_config["auto_config"] = match.generated_parser_config
        new_source = ParserSource(
            agency_id=default_agency_id,
            name=f"Auto: {source.domain}",
            source_channel=channel_value,
            source_url=source.root_url,
            city=None,
            region_code=source.metadata_json.get("region", "RU-UDM") if source.metadata_json else "RU-UDM",
            is_active=True,
            source_state=SourceState.active,
            activation_mode=ActivationMode.automatic,
            auto_discovered=True,
            parse_frequency_minutes=1440,
            parse_priority=int(source.relevance_score),
            extra_config=extra_config,
            auto_activation_reason=reason,
            last_discovery_at=datetime.utcnow(),
        )
        db.add(new_source)
        db.flush()
        db.add(
            SourceStateHistory(
                parser_source_id=new_source.id,
                from_state=None,
                to_state=SourceState.active,
                reason="Auto-activated from discovery.",
            )
        )
        db.add(
            SourceActivationEvent(
                discovered_source_id=source.id,
                parser_source_id=new_source.id,
                activation_mode=ActivationMode.automatic,
                reason=reason,
                metadata_json={"template_confidence": match.confidence if match else None},
            )
        )
        source.discovery_status = DiscoveryStatus.active
        activated += 1
    return activated


def run_source_discovery(db: Session, *, max_seeds: int = 50, auto_mode: bool = True) -> SourceDiscoveryRun:
    manual_seeds = (
        db.execute(select(SourceSeed).where(SourceSeed.enabled.is_(True)).order_by(SourceSeed.priority.desc()))
        .scalars()
        .all()
    )
    seed_items: list[dict[str, Any]] = []
    for seed in manual_seeds:
        seed_items.append(
            {
                "seed_type": seed.seed_type,
                "value": seed.value,
                "priority": seed.priority,
                "region": seed.region,
                "seed_origin": "manual",
            }
        )

    if auto_mode:
        seed_items.extend(_auto_seed_candidates(db))

    seed_items = sorted(seed_items, key=lambda item: int(item.get("priority") or 0), reverse=True)[:max_seeds]
    run = SourceDiscoveryRun(
        status=DiscoveryRunStatus.running,
        seed_count=len(seed_items),
    )
    db.add(run)
    db.flush()
    run_id = run.id
    db.commit()

    templates = db.execute(select(ParserTemplate)).scalars().all()
    touched_sources: set[int] = set()
    matched_count = 0
    errors = 0

    for seed in seed_items:
        seed_type = seed["seed_type"]
        seed_value = seed["value"]
        frontier_id = seed.get("frontier_id")
        if seed_type == DiscoverySeedType.telegram_channel:
            channel = str(seed_value).strip().lstrip("@")
            if not channel:
                continue
            domain = "t.me"
            root_url = f"https://t.me/{channel}"
            scores = {
                "relevance_score": 75.0,
                "listing_density_score": 65.0,
                "contact_richness_score": 40.0,
                "update_frequency_score": 60.0,
            }
            source = _upsert_discovered_source(
                db,
                domain=domain,
                root_url=root_url,
                parent_id=None,
                scores=scores,
                source_type=DiscoveredSourceType.telegram,
                metadata={"seed_type": seed_type.value, "seed_value": seed_value},
            )
            touched_sources.add(source.id)
            continue

        is_search_seed = False
        start_url = ""
        if seed_type == DiscoverySeedType.keyword:
            query = str(seed_value).strip()
            if not query:
                continue
            is_search_seed = True
            start_url = f"{SEARCH_QUERY_URL}{quote_plus(query)}"
        else:
            start_url = _safe_url(str(seed_value))
        html, status_code, duration = _fetch(start_url)
        health_status = SourceHealthStatus.healthy if html else SourceHealthStatus.failed
        if not html:
            errors += 1
            domain = _normalize_domain(start_url)
            if not is_search_seed:
                failed_source = _upsert_discovered_source(
                    db,
                    domain=domain,
                    root_url=start_url,
                    parent_id=None,
                    scores={
                        "relevance_score": 0,
                        "listing_density_score": 0,
                        "contact_richness_score": 0,
                        "update_frequency_score": 0,
                    },
                    source_type=DiscoveredSourceType.unknown,
                    metadata={"seed_type": seed_type.value, "seed_value": seed_value, "sample_url": start_url},
                    status_override=DiscoveryStatus.error,
                )
                touched_sources.add(failed_source.id)
                db.add(
                    SourceHealthCheck(
                        discovered_source_id=failed_source.id,
                        status=health_status,
                        http_status=status_code,
                        response_time_ms=int(duration),
                        error_message="seed_fetch_failed",
                        metadata_json={"url": start_url},
                    )
                )
            if frontier_id:
                frontier_row = db.get(SourceFrontier, frontier_id)
                if frontier_row:
                    frontier_row.status = SourceFrontierStatus.error
                    frontier_row.notes = "seed_fetch_failed"
            continue

        soup = BeautifulSoup(html, "html.parser")
        text = " ".join(soup.get_text(" ", strip=True).split())[:15000]
        links = _extract_links(soup, start_url)
        listing_links = sum(1 for link, _ in links if _listing_link_score(link) > 0)
        contact_hits = len(PHONE_RE.findall(text)) + len(EMAIL_RE.findall(text))
        date_hits = len(DATE_RE.findall(text))

        scores = _score_page(text, listing_links, contact_hits, date_hits)
        domain = _normalize_domain(start_url)
        source = None
        if not is_search_seed:
            source_type = _classify_source(domain, text)
            source = _upsert_discovered_source(
                db,
                domain=domain,
                root_url=start_url,
                parent_id=None,
                scores=scores,
                source_type=source_type,
                metadata={
                    "seed_type": seed_type.value,
                    "seed_value": seed_value,
                    "sample_url": start_url,
                    "seed_origin": seed.get("seed_origin"),
                },
            )
            touched_sources.add(source.id)
            if frontier_id:
                frontier_row = db.get(SourceFrontier, frontier_id)
                if frontier_row:
                    frontier_row.status = SourceFrontierStatus.processed
                    frontier_row.last_checked_at = datetime.utcnow()

        if source:
            template, confidence, generated = _match_template(html, soup, templates)
            if template and confidence >= 0.55:
                source.parser_template_id = template.id
                source.discovery_status = DiscoveryStatus.matched
                db.add(
                    SourceTemplateMatch(
                        discovered_source_id=source.id,
                        parser_template_id=template.id,
                        confidence=confidence,
                        generated_parser_config=generated,
                    )
                )
                matched_count += 1

            db.add(
                SourceHealthCheck(
                    discovered_source_id=source.id,
                    status=health_status,
                    http_status=status_code,
                    response_time_ms=int(duration),
                    metadata_json={"url": start_url},
                )
            )

            for sitemap_url in _discover_sitemap_links(domain):
                _enqueue_frontier(
                    db,
                    url=sitemap_url,
                    domain=_normalize_domain(sitemap_url),
                    priority=60,
                    source_id=source.id,
                    metadata={"via": "sitemap"},
                )

        for link_url, anchor in links[:120]:
            link_url = _unwrap_search_url(link_url)
            target_domain = _normalize_domain(link_url)
            if not target_domain:
                continue
            if target_domain in SEARCH_ENGINE_DOMAINS:
                continue
            link_type = SourceLinkType.listing if _listing_link_score(link_url) > 0 else SourceLinkType.outbound
            db.add(
                SourceLink(
                    from_source_id=source.id if source else None,
                    from_url=start_url,
                    to_domain=target_domain,
                    to_url=link_url,
                    anchor_text=anchor[:255] if anchor else None,
                    link_type=link_type,
                )
            )
            if not is_search_seed and target_domain == domain:
                continue
            candidate_scores = {
                "relevance_score": min(100.0, scores["relevance_score"] * 0.6),
                "listing_density_score": min(100.0, scores["listing_density_score"] * 0.6),
                "contact_richness_score": min(100.0, scores["contact_richness_score"] * 0.6),
                "update_frequency_score": min(100.0, scores["update_frequency_score"] * 0.6),
            }
            _enqueue_frontier(
                db,
                url=link_url,
                domain=target_domain,
                priority=int(candidate_scores["relevance_score"]),
                source_id=source.id if source else None,
                metadata={"parent_domain": domain if source else None, "sample_url": link_url},
            )

    run_row = db.get(SourceDiscoveryRun, run_id)
    if run_row:
        run_row.status = (
            DiscoveryRunStatus.completed_with_errors if errors > 0 else DiscoveryRunStatus.completed
        )
        run_row.finished_at = datetime.utcnow()
        run_row.candidate_count = len(touched_sources)
        run_row.matched_count = matched_count
        run_row.error_count = errors
        db.commit()
        db.refresh(run_row)
        return run_row
    return run
