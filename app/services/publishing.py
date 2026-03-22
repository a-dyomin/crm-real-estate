from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from sqlalchemy import Select, and_, or_, select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.models.call_record import CallRecord
from app.models.contact_identity import ContactIdentity
from app.models.enums import CallDirection, CallStatus, ParserResultStatus, PropertyDealType, PropertyType
from app.models.lead import Lead
from app.models.parser_result import ParserResult
from app.models.property import Property
from app.services.conversion import parser_result_to_lead
from app.services.market_benchmarks import normalize_deal_type, normalize_property_type


PIPELINE_STAGE_ORDER = {
    "raw": 0,
    "parsed": 1,
    "normalized": 2,
    "object_resolved": 3,
    "contacts_resolved": 4,
    "scored": 5,
    "published": 6,
    "rejected": 7,
}

INVALID_CONTACT_CLASSES = {"platform_contact", "support_contact"}


@dataclass
class PublishCounters:
    published: int = 0
    rejected: int = 0
    objects_resolved: int = 0


@dataclass
class LeadPublishOutcome:
    total: int = 0
    by_source: dict[int, int] = field(default_factory=dict)
    leads: list[Lead] = field(default_factory=list)


@dataclass
class CallCenterOutcome:
    total: int = 0
    by_source: dict[int, int] = field(default_factory=dict)


def _advance_pipeline_status(result: ParserResult, stage: str) -> None:
    current = result.pipeline_status
    if stage not in PIPELINE_STAGE_ORDER:
        return
    if current is None:
        result.pipeline_status = stage
        return
    current_rank = PIPELINE_STAGE_ORDER.get(current, 0)
    next_rank = PIPELINE_STAGE_ORDER.get(stage, 0)
    if next_rank > current_rank:
        result.pipeline_status = stage


def _selected_contact_class(result: ParserResult) -> str:
    selected = result.selected_contact
    if isinstance(selected, dict):
        return str(selected.get("class") or "").strip()
    return ""


def _contact_valid(result: ParserResult) -> bool:
    contact_class = _selected_contact_class(result)
    if contact_class in INVALID_CONTACT_CLASSES:
        return False
    return bool(result.contact_phone or result.contact_email)


def _quality_gate(result: ParserResult) -> tuple[bool, list[str], dict[str, Any]]:
    reasons: list[str] = []
    if not result.title or len(result.title.strip()) < 3:
        reasons.append("missing_title")
    if not (result.normalized_address or result.city):
        reasons.append("missing_location")
    contact_class = _selected_contact_class(result)
    if contact_class in INVALID_CONTACT_CLASSES:
        reasons.append("invalid_contact")

    payload = result.payload or {}
    prop_type = normalize_property_type(payload, result.title, result.description or "")
    deal_type = normalize_deal_type(result.listing_type, result.title, result.description or "")
    meta = {"property_type": prop_type, "deal_type": deal_type}
    return len(reasons) == 0, reasons, meta


def _ensure_property(db: Session, result: ParserResult, *, prop_type: str, deal_type: str) -> bool:
    if result.property_id:
        return False
    stmt: Select[tuple[Property]] = select(Property).where(
        Property.agency_id == result.agency_id,
        Property.source_channel == result.source_channel,
        Property.source_external_id == result.source_external_id,
    )
    existing = db.execute(stmt).scalar_one_or_none()
    if existing:
        result.property_id = existing.id
        return False

    prop_type_map = {
        "office": PropertyType.office,
        "retail": PropertyType.retail,
        "warehouse": PropertyType.warehouse,
        "industrial": PropertyType.industrial,
        "land": PropertyType.land,
        "mixed_use": PropertyType.mixed_use,
        "other": PropertyType.other,
    }
    prop_type_value = prop_type_map.get(prop_type, PropertyType.other)
    deal_type_value = PropertyDealType.sale if deal_type == "sale" else PropertyDealType.rent
    prop = Property(
        agency_id=result.agency_id,
        title=result.title,
        description=result.description,
        address=result.normalized_address or "Без адреса",
        city=result.city or "Не указан",
        region_code=result.region_code or "RU-UDM",
        latitude=result.latitude,
        longitude=result.longitude,
        area_sqm=result.area_sqm,
        price_rub=result.price_rub,
        deal_type=deal_type_value,
        property_type=prop_type_value,
        source_channel=result.source_channel,
        source_external_id=result.source_external_id,
    )
    db.add(prop)
    db.flush()
    result.property_id = prop.id
    return True


def publish_parser_results(db: Session, agency_id: int, *, since: datetime | None = None) -> PublishCounters:
    stmt: Select[tuple[ParserResult]] = select(ParserResult).where(ParserResult.agency_id == agency_id)
    if since:
        stmt = stmt.where(ParserResult.updated_at >= since)
    results = db.execute(stmt).scalars().all()
    counters = PublishCounters()
    now = datetime.utcnow()

    for result in results:
        _advance_pipeline_status(result, "normalized")
        ok, reasons, meta = _quality_gate(result)

        payload = dict(result.payload or {})
        payload["deal_type_normalized"] = meta.get("deal_type")
        payload["property_type_normalized"] = meta.get("property_type")
        payload["quality_gate"] = {"ok": ok, "reasons": reasons}
        result.payload = payload

        if ok:
            created = _ensure_property(db, result, prop_type=meta.get("property_type") or "other", deal_type=meta.get("deal_type") or "rent")
            if created:
                counters.objects_resolved += 1
            _advance_pipeline_status(result, "object_resolved")
            _advance_pipeline_status(result, "contacts_resolved")
            if result.lead_score is not None:
                _advance_pipeline_status(result, "scored")
            if result.pipeline_status != "published":
                counters.published += 1
            result.pipeline_status = "published"
            if result.published_at is None:
                result.published_at = now
        else:
            if result.pipeline_status != "rejected":
                counters.rejected += 1
            result.pipeline_status = "rejected"
            if result.status not in (
                ParserResultStatus.converted_to_lead,
                ParserResultStatus.converted_to_deal,
            ):
                result.status = ParserResultStatus.rejected

    db.commit()
    return counters


def publish_owners(db: Session, agency_id: int) -> int:
    stmt: Select[tuple[ContactIdentity]] = select(ContactIdentity).where(ContactIdentity.agency_id == agency_id)
    identities = db.execute(stmt).scalars().all()
    published = 0
    now = datetime.utcnow()
    for identity in identities:
        if identity.final_class == "owner_candidate":
            identity.lifecycle_status = "published_to_owners"
            identity.published_to_owners_at = identity.published_to_owners_at or now
            published += 1
        else:
            identity.lifecycle_status = identity.lifecycle_status or "owner_scored"
    db.commit()
    return published


def auto_create_leads(db: Session, agency_id: int, *, since: datetime | None = None) -> LeadPublishOutcome:
    settings = get_settings()
    stmt: Select[tuple[ParserResult]] = select(ParserResult).where(
        ParserResult.agency_id == agency_id,
        ParserResult.pipeline_status == "published",
        ParserResult.status.notin_(
            [ParserResultStatus.duplicate, ParserResultStatus.possible_duplicate, ParserResultStatus.rejected]
        ),
    )
    if since:
        stmt = stmt.where(ParserResult.updated_at >= since)
    results = db.execute(stmt).scalars().all()

    existing_sources = {
        str(item[0])
        for item in db.execute(
            select(Lead.source_record_id).where(Lead.agency_id == agency_id, Lead.source_record_id.is_not(None))
        ).all()
    }

    outcome = LeadPublishOutcome()
    for result in results:
        if str(result.id) in existing_sources:
            continue
        if not _contact_valid(result):
            continue
        score = float(result.lead_score or 0)
        owner_score = float(result.owner_probability_score or 0)
        below_market = bool(result.below_market_flag)
        should_publish = score >= settings.auto_lead_score_threshold or owner_score >= settings.auto_lead_owner_threshold
        if settings.auto_lead_below_market_enabled and below_market:
            should_publish = True
        if not should_publish:
            continue
        lead = parser_result_to_lead(
            db=db,
            parser_result=result,
            lead_state="auto_created",
            auto_created=True,
        )
        outcome.total += 1
        outcome.leads.append(lead)
        if result.parser_source_id:
            outcome.by_source[result.parser_source_id] = outcome.by_source.get(result.parser_source_id, 0) + 1

    db.commit()
    return outcome


def auto_create_call_center_entries(
    db: Session,
    agency_id: int,
    *,
    leads: list[Lead] | None = None,
) -> CallCenterOutcome:
    settings = get_settings()
    outcome = CallCenterOutcome()
    if not settings.auto_call_center_enabled:
        return outcome

    if leads is None:
        lead_stmt: Select[tuple[Lead]] = select(Lead).where(
            Lead.agency_id == agency_id,
            Lead.auto_created.is_(True),
            Lead.lead_state == "auto_created",
        )
        leads = db.execute(lead_stmt).scalars().all()

    lead_ids = [lead.id for lead in leads]
    if not lead_ids:
        return outcome

    existing_calls = {
        int(item[0])
        for item in db.execute(
            select(CallRecord.lead_id).where(CallRecord.lead_id.in_(lead_ids))
        ).all()
        if item[0] is not None
    }

    result_map = {}
    source_ids = {}
    result_ids: list[int] = []
    for lead in leads:
        raw_id = (lead.source_record_id or "").strip()
        if raw_id.isdigit():
            result_ids.append(int(raw_id))
    parser_results = []
    if result_ids:
        parser_results = (
            db.execute(
                select(ParserResult).where(ParserResult.agency_id == agency_id, ParserResult.id.in_(result_ids))
            )
            .scalars()
            .all()
        )
    for result in parser_results:
        result_map[str(result.id)] = result
        if result.parser_source_id:
            source_ids[str(result.id)] = result.parser_source_id

    now = datetime.utcnow()
    for lead in leads:
        if not lead.contact_phone:
            continue
        if lead.id in existing_calls:
            continue

        call = CallRecord(
            agency_id=agency_id,
            lead_id=lead.id,
            provider="parser_pipeline",
            direction=CallDirection.outbound,
            status=CallStatus.ringing,
            from_number=None,
            to_number=lead.contact_phone,
            started_at=now,
            notes="Автозадача: прозвонить лид из парсера.",
        )
        db.add(call)
        outcome.total += 1
        result_id = lead.source_record_id or ""
        source_id = source_ids.get(result_id)
        if source_id:
            outcome.by_source[source_id] = outcome.by_source.get(source_id, 0) + 1

        if result_id in result_map:
            _mark_contact_identity_call_center(db, agency_id, result_map[result_id])

    db.commit()
    return outcome


def _mark_contact_identity_call_center(db: Session, agency_id: int, result: ParserResult) -> None:
    if not result.contact_phone and not result.contact_email:
        return
    contact_keys = []
    if result.contact_phone:
        normalized = "".join(ch for ch in result.contact_phone if ch.isdigit())
        if normalized.startswith("8") and len(normalized) == 11:
            normalized = "7" + normalized[1:]
        if normalized.startswith("7") and len(normalized) == 11:
            contact_keys.append(("phone", f"+{normalized}"))
    if result.contact_email:
        contact_keys.append(("email", result.contact_email.strip().lower()))
    if not contact_keys:
        return

    conditions = [
        and_(ContactIdentity.key_type == key_type, ContactIdentity.key_value == key_value)
        for key_type, key_value in contact_keys
    ]
    if not conditions:
        return
    stmt: Select[tuple[ContactIdentity]] = select(ContactIdentity).where(
        ContactIdentity.agency_id == agency_id,
        or_(*conditions),
    )
    identity = db.execute(stmt).scalar_one_or_none()
    if identity:
        identity.promoted_to_call_center_at = identity.promoted_to_call_center_at or datetime.utcnow()
        identity.lifecycle_status = "promoted_to_call_center"
