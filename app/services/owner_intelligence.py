from __future__ import annotations

import re
from collections import Counter, defaultdict
from datetime import datetime
from statistics import median
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.models.contact_identity import ContactIdentity
from app.models.contact_identity_link import ContactIdentityLink
from app.models.enums import ParserResultStatus
from app.models.parser_result import ParserResult

OWNER_KEYWORDS = (
    "собственник",
    "от собственника",
    "без комиссии",
    "прямой договор",
    "в собственности",
)
AGENT_KEYWORDS = (
    "комиссия",
    "агентство",
    "подберем",
    "есть другие варианты",
    "риэлтор",
    "брокер",
    "агент",
)
URGENCY_KEYWORDS = (
    "срочно",
    "горяч",
    "скидка",
    "снижение",
    "торг",
    "быстрый",
    "urgent",
)
NEGOTIATION_KEYWORDS = (
    "торг",
    "обсудим",
    "гибкие условия",
    "возможен торг",
)

ORG_RE = re.compile(
    r"\b(ООО|ИП|АО|ПАО|ЗАО|ОАО|ГК|ТОО)\s+\"?([A-Za-zА-Яа-я0-9 .\-]{2,})\"?",
    re.IGNORECASE,
)


def _normalize_phone(value: str | None) -> str:
    if not value:
        return ""
    digits = re.sub(r"\D+", "", value)
    if digits.startswith("8") and len(digits) == 11:
        digits = "7" + digits[1:]
    if digits.startswith("7") and len(digits) == 11:
        return f"+{digits}"
    return ""


def _normalize_email(value: str | None) -> str:
    return (value or "").strip().lower()


def _normalize_text(value: str | None) -> str:
    return " ".join((value or "").split()).strip().lower()


def _extract_orgs(text: str) -> list[str]:
    names: list[str] = []
    for match in ORG_RE.finditer(text or ""):
        prefix = match.group(1).upper()
        name = " ".join(match.group(2).split())
        if name:
            names.append(f"{prefix} {name}")
    return names


def _object_key(result: ParserResult) -> str:
    if result.fingerprint:
        return result.fingerprint
    parts = [
        result.normalized_address or "",
        str(result.area_sqm or ""),
        result.listing_type or "",
        result.raw_url or "",
    ]
    return "|".join(parts)


def _cluster_ratio(values: list[str]) -> tuple[str | None, float]:
    if not values:
        return None, 0.0
    counts = Counter(values)
    value, count = counts.most_common(1)[0]
    return value, count / max(1, len(values))


def _contact_class(record: ParserResult) -> str | None:
    selected = record.selected_contact
    if isinstance(selected, dict):
        return str(selected.get("class") or "").strip() or None
    return None


def _price_per_sqm(result: ParserResult) -> float | None:
    if result.price_rub is None or result.area_sqm is None:
        return None
    try:
        area = float(result.area_sqm)
        price = float(result.price_rub)
    except (TypeError, ValueError):
        return None
    if area <= 0:
        return None
    return price / area


def _score_contact(
    *,
    total_listings: int,
    active_listings: int,
    unique_addresses: int,
    unique_objects: int,
    source_diversity: int,
    repost_rate: float,
    object_reuse_rate: float,
    cross_source_dup_rate: float,
    geo_cluster_ratio: float,
    template_ratio: float,
    posting_frequency: float,
    owner_hits: int,
    agent_hits: int,
    urgency_hits: int,
    negotiation_hits: int,
    owner_class_ratio: float,
    agent_class_ratio: float,
    platform_class_ratio: float,
    org_count: int,
) -> tuple[float, float, float, dict[str, Any]]:
    owner_score = 50.0
    agent_score = 50.0
    platform_score = 0.0

    owner_signals: list[str] = []
    agent_signals: list[str] = []

    if total_listings <= 1:
        owner_score += 25
        owner_signals.append("только 1 объявление")
    elif total_listings <= 3:
        owner_score += 15
        owner_signals.append("мало объявлений")
    elif total_listings >= 10:
        owner_score -= 20
        agent_score += 25
        agent_signals.append("много объявлений")
    elif total_listings >= 6:
        owner_score -= 10
        agent_score += 12
        agent_signals.append("активная публикация")

    if unique_addresses <= 1:
        owner_score += 15
        owner_signals.append("объявления в одной локации")
    elif unique_addresses <= 2:
        owner_score += 8
    else:
        agent_score += 10
        agent_signals.append("много разных адресов")

    if unique_objects <= 1:
        owner_score += 10
        owner_signals.append("один объект")
    elif unique_objects <= 2:
        owner_score += 5
    elif unique_objects >= 8:
        agent_score += 10
        agent_signals.append("много разных объектов")

    if geo_cluster_ratio >= 0.7:
        owner_score += 10
        owner_signals.append("география ограничена")
    elif geo_cluster_ratio <= 0.4:
        agent_score += 8
        agent_signals.append("широкая география")

    if source_diversity <= 1:
        owner_score += 8
        owner_signals.append("один источник")
    elif source_diversity >= 3:
        agent_score += 12
        agent_signals.append("много источников")

    if repost_rate >= 0.5:
        agent_score += 10
        owner_score -= 8
        agent_signals.append("частые репосты")
    elif repost_rate <= 0.2:
        owner_score += 5

    if cross_source_dup_rate >= 0.3:
        agent_score += 8
        agent_signals.append("кросс-публикации")
    elif cross_source_dup_rate <= 0.1:
        owner_score += 3

    if template_ratio >= 0.6:
        agent_score += 10
        owner_score -= 8
        agent_signals.append("шаблонные описания")

    if posting_frequency >= 12:
        agent_score += 10
        agent_signals.append("высокая частота публикаций")
    elif posting_frequency <= 2 and total_listings >= 1:
        owner_score += 5
        owner_signals.append("редкие публикации")

    if object_reuse_rate >= 2 and repost_rate < 0.4 and unique_objects <= 2:
        owner_score += 5
        owner_signals.append("несколько публикаций по одному объекту")
    elif object_reuse_rate <= 1.2 and total_listings >= 6:
        agent_score += 5
        agent_signals.append("много разных объектов")

    if owner_hits > 0:
        owner_score += min(20, owner_hits * 4)
        owner_signals.append("фразы собственника в тексте")
    if agent_hits > 0:
        owner_score -= min(15, agent_hits * 3)
        agent_score += min(20, agent_hits * 4)
        agent_signals.append("агентские фразы в тексте")

    if urgency_hits > 0:
        owner_signals.append("есть срочные объявления")
    if negotiation_hits > 0:
        owner_score += 3
        owner_signals.append("есть торг/условия")

    if org_count == 1 and total_listings <= 3:
        owner_score += 5
        owner_signals.append("одна организация")
    elif org_count >= 3 and total_listings >= 5:
        agent_score += 5
        agent_signals.append("несколько организаций")

    if owner_class_ratio >= 0.5:
        owner_score += 8
        owner_signals.append("контакт помечен как собственник")
    if agent_class_ratio >= 0.5:
        agent_score += 8
        agent_signals.append("контакт помечен как агент")

    if platform_class_ratio >= 0.6:
        platform_score = 80.0
        owner_score -= 20
        agent_score -= 10

    if active_listings <= 2:
        owner_score += 5

    owner_score = max(0.0, min(100.0, owner_score))
    agent_score = max(0.0, min(100.0, agent_score))
    platform_score = max(0.0, min(100.0, platform_score))

    explanation = {
        "owner_signals": owner_signals,
        "agent_signals": agent_signals,
    }
    return owner_score, agent_score, platform_score, explanation


def _priority_class(
    *,
    owner_score: float,
    last_seen_at: datetime | None,
    unique_objects: int,
    repost_rate: float,
    urgency_ratio: float,
    under_market_ratio: float,
) -> tuple[str, float, list[str]]:
    freshness = 0.0
    reasons: list[str] = []
    if last_seen_at:
        delta_hours = (datetime.utcnow() - last_seen_at).total_seconds() / 3600
        if delta_hours <= 24:
            freshness = 20
            reasons.append("свежие объявления")
        elif delta_hours <= 72:
            freshness = 10
            reasons.append("обновления за 3 дня")

    uniqueness = 10 if unique_objects <= 1 else 5 if unique_objects <= 2 else 0
    if uniqueness >= 10:
        reasons.append("уникальный объект")

    urgency = 10 if urgency_ratio >= 0.3 else 5 if urgency_ratio >= 0.15 else 0
    if urgency:
        reasons.append("есть срочные признаки")

    price_bonus = 5 if under_market_ratio >= 0.2 else 0
    if price_bonus:
        reasons.append("ниже медианы по контактам")

    competition = -5 if repost_rate >= 0.5 else 0
    if repost_rate >= 0.5:
        reasons.append("много репостов")

    score = owner_score * 0.6 + freshness + uniqueness + urgency + price_bonus + competition
    if score >= 80:
        return "Горячий собственник", score, reasons
    if score >= 65:
        return "Высокий приоритет", score, reasons
    if score >= 45:
        return "Средний", score, reasons
    return "Низкий", score, reasons


def refresh_owner_intelligence(db: Session, agency_id: int) -> int:
    results: list[ParserResult] = (
        db.execute(
            select(ParserResult).where(
                ParserResult.agency_id == agency_id,
                (ParserResult.contact_phone.is_not(None) | ParserResult.contact_email.is_not(None)),
            )
        )
        .scalars()
        .all()
    )
    groups: dict[str, list[ParserResult]] = defaultdict(list)
    identity_meta: dict[str, dict[str, str]] = {}
    for result in results:
        phone_key = _normalize_phone(result.contact_phone)
        email_key = _normalize_email(result.contact_email)
        if phone_key:
            key = f"phone:{phone_key}"
            groups[key].append(result)
            identity_meta[key] = {"type": "phone", "value": phone_key, "display": result.contact_phone or phone_key}
        elif email_key:
            key = f"email:{email_key}"
            groups[key].append(result)
            identity_meta[key] = {"type": "email", "value": email_key, "display": result.contact_email or email_key}

    if not groups:
        return 0

    existing_ids = db.execute(select(ContactIdentity.id).where(ContactIdentity.agency_id == agency_id)).scalars().all()
    if existing_ids:
        db.execute(delete(ContactIdentityLink).where(ContactIdentityLink.contact_identity_id.in_(existing_ids)))
        db.execute(delete(ContactIdentity).where(ContactIdentity.id.in_(existing_ids)))
        db.commit()

    created = 0
    for key, records in groups.items():
        meta = identity_meta.get(key, {})
        total_listings = len(records)
        active_listings = sum(1 for record in records if record.status != ParserResultStatus.rejected)
        object_keys = [_object_key(record) for record in records]
        unique_objects = len(set(object_keys))
        addresses = [record.normalized_address or "" for record in records]
        unique_addresses = len(set(addresses))
        source_diversity = len({record.source_channel for record in records})
        duplicate_count = sum(
            1 for record in records if record.status in {ParserResultStatus.duplicate, ParserResultStatus.possible_duplicate}
        )
        repost_rate = duplicate_count / total_listings if total_listings else 0.0
        region_values = [res.region_code for res in records if res.region_code]
        city_values = [res.city for res in records if res.city]
        region_cluster, region_ratio = _cluster_ratio(region_values)
        city_cluster, city_ratio = _cluster_ratio(city_values)
        geo_cluster_ratio = max(region_ratio, city_ratio)
        region_clustered = region_cluster or city_cluster

        descriptions = [_normalize_text((record.description or "")[:240]) for record in records if record.description]
        template_ratio = 0.0
        if descriptions:
            unique_descriptions = len(set(descriptions))
            template_ratio = 1.0 - (unique_descriptions / max(1, len(descriptions)))

        owner_hits = 0
        agent_hits = 0
        urgency_hits = 0
        negotiation_hits = 0
        org_counter: Counter[str] = Counter()
        class_counter: Counter[str] = Counter()
        first_seen = min((record.created_at for record in records), default=None)
        last_seen = max((record.updated_at for record in records), default=None)
        name_counter: Counter[str] = Counter()
        price_per_sqm_values: list[float] = []
        object_sources: dict[str, set[str]] = defaultdict(set)
        object_counts: Counter[str] = Counter()
        address_counts: Counter[str] = Counter()

        for record in records:
            text = _normalize_text(f"{record.title} {record.description or ''}")
            if any(keyword in text for keyword in OWNER_KEYWORDS):
                owner_hits += 1
            if any(keyword in text for keyword in AGENT_KEYWORDS):
                agent_hits += 1
            if any(keyword in text for keyword in URGENCY_KEYWORDS):
                urgency_hits += 1
            if any(keyword in text for keyword in NEGOTIATION_KEYWORDS):
                negotiation_hits += 1
            for org_name in _extract_orgs(text):
                org_counter[org_name] += 1
            contact_class = _contact_class(record)
            if contact_class:
                class_counter[contact_class] += 1
            if record.contact_name:
                name_counter[record.contact_name.strip()] += 1
            price_per = _price_per_sqm(record)
            if price_per is not None:
                price_per_sqm_values.append(price_per)
            obj_key = _object_key(record)
            object_sources[obj_key].add(record.source_channel.value)
            object_counts[obj_key] += 1
            address_counts[record.normalized_address or ""] += 1

        object_reuse_rate = total_listings / unique_objects if unique_objects else 0.0
        objects_with_multi_sources = sum(1 for sources in object_sources.values() if len(sources) > 1)
        cross_source_dup_rate = objects_with_multi_sources / unique_objects if unique_objects else 0.0
        span_days = max(1, ((last_seen or datetime.utcnow()) - (first_seen or datetime.utcnow())).days + 1)
        posting_frequency = total_listings / span_days * 30
        owner_class_ratio = class_counter.get("owner_candidate", 0) / max(1, total_listings)
        agent_class_ratio = class_counter.get("agent_candidate", 0) / max(1, total_listings)
        platform_class_ratio = (
            class_counter.get("platform_contact", 0) + class_counter.get("support_contact", 0)
        ) / max(1, total_listings)

        median_price = median(price_per_sqm_values) if price_per_sqm_values else None
        under_market_count = 0
        if median_price:
            threshold = median_price * 0.8
            under_market_count = sum(1 for value in price_per_sqm_values if value <= threshold)
        under_market_ratio = under_market_count / max(1, len(price_per_sqm_values))

        owner_score, agent_score, platform_score, explanation = _score_contact(
            total_listings=total_listings,
            active_listings=active_listings,
            unique_addresses=unique_addresses,
            unique_objects=unique_objects,
            source_diversity=source_diversity,
            repost_rate=repost_rate,
            object_reuse_rate=object_reuse_rate,
            cross_source_dup_rate=cross_source_dup_rate,
            geo_cluster_ratio=geo_cluster_ratio,
            template_ratio=template_ratio,
            posting_frequency=posting_frequency,
            owner_hits=owner_hits,
            agent_hits=agent_hits,
            urgency_hits=urgency_hits,
            negotiation_hits=negotiation_hits,
            owner_class_ratio=owner_class_ratio,
            agent_class_ratio=agent_class_ratio,
            platform_class_ratio=platform_class_ratio,
            org_count=len(org_counter),
        )

        if platform_score >= 70:
            final_class = "platform_contact"
        elif owner_score >= agent_score and owner_score >= 60:
            final_class = "owner_candidate"
        elif agent_score >= 60:
            final_class = "agent_candidate"
        else:
            final_class = "unknown"

        top_score = max(owner_score, agent_score, platform_score)
        second_score = sorted([owner_score, agent_score, platform_score])[-2]
        confidence = max(0.1, min(1.0, (top_score - second_score) / 100 + 0.2))

        summary = "Средняя вероятность собственника"
        if final_class == "owner_candidate":
            summary = "Высокая вероятность прямого собственника"
        elif final_class == "agent_candidate":
            summary = "Высокая вероятность агента"
        elif final_class == "platform_contact":
            summary = "Похоже на платформенный контакт"
        explanation["summary"] = summary

        owner_priority, priority_score, priority_reasons = _priority_class(
            owner_score=owner_score,
            last_seen_at=last_seen,
            unique_objects=unique_objects,
            repost_rate=repost_rate,
            urgency_ratio=urgency_hits / max(1, total_listings),
            under_market_ratio=under_market_ratio,
        )

        organizations = [name for name, _count in org_counter.most_common(3)] if org_counter else None
        display_name = name_counter.most_common(1)[0][0] if name_counter else None
        geo_cluster_value = region_clustered or None
        if not geo_cluster_value and city_cluster:
            geo_cluster_value = city_cluster

        explanation.update(
            {
                "behavior": {
                    "posting_frequency_per_month": round(posting_frequency, 2),
                    "repost_rate": round(repost_rate, 3),
                    "object_reuse_rate": round(object_reuse_rate, 3),
                    "cross_source_dup_rate": round(cross_source_dup_rate, 3),
                    "template_ratio": round(template_ratio, 3),
                    "span_days": span_days,
                    "geo_cluster_ratio": round(geo_cluster_ratio, 3),
                },
                "text_signals": {
                    "owner_hits": owner_hits,
                    "agent_hits": agent_hits,
                    "urgency_hits": urgency_hits,
                    "negotiation_hits": negotiation_hits,
                },
                "organization_signals": {
                    "organizations": organizations or [],
                },
                "graph": {
                    "nodes": {
                        "contacts": 1,
                        "listings": total_listings,
                        "objects": unique_objects,
                        "addresses": unique_addresses,
                        "organizations": len(org_counter),
                    },
                    "edges": {
                        "contact_listing": total_listings,
                        "listing_object": total_listings,
                        "object_address": unique_objects,
                        "contact_org": len(org_counter),
                    },
                    "max_object_cluster": max(object_counts.values()) if object_counts else 0,
                    "max_address_cluster": max(address_counts.values()) if address_counts else 0,
                    "hub_score": round(total_listings * source_diversity * (1 - geo_cluster_ratio), 2),
                },
                "priority_reasons": priority_reasons,
                "priority_score": round(priority_score, 2),
            }
        )

        identity = ContactIdentity(
            agency_id=agency_id,
            key_type=meta.get("type", "unknown"),
            key_value=meta.get("value", key),
            display_value=meta.get("display"),
            display_name=display_name,
            owner_probability=round(owner_score, 2),
            agent_probability=round(agent_score, 2),
            platform_probability=round(platform_score, 2),
            final_class=final_class,
            confidence=round(confidence, 3),
            owner_priority=owner_priority,
            owner_priority_score=round(priority_score, 2),
            explanation=explanation,
            organizations=organizations,
            total_listings=total_listings,
            active_listings=active_listings,
            unique_objects=unique_objects,
            unique_addresses=unique_addresses,
            source_diversity=source_diversity,
            repost_rate=round(repost_rate, 3),
            region_cluster=geo_cluster_value,
            first_seen_at=first_seen,
            last_seen_at=last_seen,
        )
        db.add(identity)
        db.flush()
        created += 1

        for record in records:
            db.add(ContactIdentityLink(contact_identity_id=identity.id, parser_result_id=record.id))
            payload = dict(record.payload or {})
            payload.update(
                {
                    "owner_intel_score": identity.owner_probability,
                    "owner_intel_class": identity.final_class,
                    "owner_intel_priority": identity.owner_priority,
                    "owner_intel_priority_score": identity.owner_priority_score,
                    "owner_intel_confidence": identity.confidence,
                    "owner_intel_summary": identity.explanation.get("summary") if identity.explanation else None,
                    "owner_intel_explanation": identity.explanation,
                    "owner_intel_contact_id": identity.id,
                }
            )
            record.payload = payload

    db.commit()
    return created
