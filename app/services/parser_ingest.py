from sqlalchemy.orm import Session

from app.models.parser_result import ParserResult
from app.schemas.parser import ParserIngestItem
from app.services.dedup import decide_parser_result_status


def ingest_parser_item(db: Session, agency_id: int, payload: ParserIngestItem) -> ParserResult:
    decision = decide_parser_result_status(db=db, agency_id=agency_id, item=payload)
    result = ParserResult(
        agency_id=agency_id,
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
        intent=payload.intent,
        status=decision.status,
        duplicate_of_id=decision.duplicate_of_id,
        fingerprint=decision.fingerprint,
        payload=payload.payload,
    )
    db.add(result)
    db.flush()
    return result
