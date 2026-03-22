from sqlalchemy.orm import Session

from app.models.parser_result import ParserResult
from app.schemas.parser import ParserIngestItem
from app.services.dedup import decide_parser_result_status
from app.services.lead_scoring import score_lead


def ingest_parser_item(db: Session, agency_id: int, payload: ParserIngestItem) -> ParserResult:
    decision = decide_parser_result_status(db=db, agency_id=agency_id, item=payload)
    payload_data = dict(payload.payload or {})
    payload_data.update(
        {
            "title": payload.title,
            "description": payload.description,
            "price_rub": float(payload.price_rub) if payload.price_rub is not None else None,
            "area_sqm": float(payload.area_sqm) if payload.area_sqm is not None else None,
            "normalized_address": payload.normalized_address,
            "contact_phone": payload.contact_phone,
            "contact_email": payload.contact_email,
            "contact_candidates": payload.contact_candidates,
            "selected_contact": payload.selected_contact,
            "rejected_contacts": payload.rejected_contacts,
            "contact_rejection_reasons": payload.contact_rejection_reasons,
            "contact_confidence": payload.contact_confidence,
        }
    )
    score = score_lead(payload_data, status=decision.status)
    payload_data["lead_score"] = score.score
    payload_data["lead_score_breakdown"] = score.breakdown.__dict__
    payload_data["lead_score_freshness_hours"] = score.freshness_hours
    payload_data["monetization_tier"] = score.monetization_tier
    payload_data["owner_probability_score"] = payload_data.get("owner_intel_score") or score.breakdown.owner_probability_score
    result = ParserResult(
        agency_id=agency_id,
        parser_source_id=payload.parser_source_id,
        source_channel=payload.source_channel,
        source_external_id=payload.source_external_id,
        raw_url=payload.raw_url,
        telegram_post_url=payload.telegram_post_url,
        title=payload.title,
        description=payload.description,
        listing_type=payload.listing_type,
        image_url=payload.image_url,
        normalized_address=payload.normalized_address,
        address_district=payload.address_district,
        address_street=payload.address_street,
        city=payload.city,
        region_code=payload.region_code,
        latitude=payload.latitude,
        longitude=payload.longitude,
        area_sqm=payload.area_sqm,
        price_rub=payload.price_rub,
        contact_name=payload.contact_name,
        contact_phone=payload.contact_phone,
        contact_email=payload.contact_email,
        contact_candidates=payload.contact_candidates,
        selected_contact=payload.selected_contact,
        rejected_contacts=payload.rejected_contacts,
        contact_rejection_reasons=payload.contact_rejection_reasons,
        contact_confidence=payload.contact_confidence,
        lead_score=score.score,
        owner_probability_score=payload_data.get("owner_probability_score"),
        owner_priority_score=payload_data.get("owner_priority_score"),
        owner_confidence=payload_data.get("owner_confidence"),
        owner_explanation_summary=payload_data.get("owner_explanation_summary"),
        market_median_price=payload_data.get("market_median_price"),
        market_median_price_per_m2=payload_data.get("market_median_price_per_m2"),
        deviation_from_market_pct=payload_data.get("deviation_from_market_pct"),
        below_market_flag=payload_data.get("below_market_flag"),
        intent=payload.intent,
        status=decision.status,
        pipeline_status="parsed",
        duplicate_of_id=decision.duplicate_of_id,
        fingerprint=decision.fingerprint,
        payload=payload_data,
    )
    db.add(result)
    db.flush()
    return result
