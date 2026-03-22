from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from app.models.deal import Deal
from app.models.enums import DealStatus, LeadStatus, ParserResultStatus, PropertyDealType, PropertyType, SourceChannel
from app.models.lead import Lead
from app.models.parser_result import ParserResult
from app.models.property import Property


def _map_property_type(parser_result: ParserResult) -> PropertyType:
    payload = parser_result.payload or {}
    hint = str(payload.get("property_type") or payload.get("propertyType") or "").lower()
    text = f"{parser_result.title or ''} {parser_result.description or ''}".lower()
    merged = f"{hint} {text}".strip()
    if "склад" in merged:
        return PropertyType.warehouse
    if "офис" in merged:
        return PropertyType.office
    if "производ" in merged or "индустри" in merged:
        return PropertyType.industrial
    if "торгов" in merged or "магаз" in merged:
        return PropertyType.retail
    if "земел" in merged or "участ" in merged:
        return PropertyType.land
    if "свобод" in merged or "псн" in merged:
        return PropertyType.other
    return PropertyType.other


def _map_deal_type(parser_result: ParserResult) -> PropertyDealType:
    value = str(parser_result.listing_type or "").lower()
    if value in ("sale", "sell", "продажа"):
        return PropertyDealType.sale
    if value in ("rent", "lease", "аренда"):
        return PropertyDealType.rent
    text = f"{parser_result.title or ''} {parser_result.description or ''}".lower()
    if "продаж" in text:
        return PropertyDealType.sale
    return PropertyDealType.rent


def _get_or_create_property(db: Session, parser_result: ParserResult) -> Property:
    stmt: Select[tuple[Property]] = select(Property).where(
        Property.agency_id == parser_result.agency_id,
        Property.source_channel == parser_result.source_channel,
        Property.source_external_id == parser_result.source_external_id,
    )
    existing = db.execute(stmt).scalar_one_or_none()
    if existing:
        return existing

    prop = Property(
        agency_id=parser_result.agency_id,
        title=parser_result.title,
        description=parser_result.description,
        address=parser_result.normalized_address or "Без адреса",
        city=parser_result.city or "Не указан",
        region_code=parser_result.region_code or "RU-UDM",
        latitude=parser_result.latitude,
        longitude=parser_result.longitude,
        area_sqm=parser_result.area_sqm,
        price_rub=parser_result.price_rub,
        deal_type=_map_deal_type(parser_result),
        property_type=_map_property_type(parser_result),
        source_channel=parser_result.source_channel,
        source_external_id=parser_result.source_external_id,
    )
    db.add(prop)
    db.flush()
    return prop


def parser_result_to_lead(
    db: Session,
    parser_result: ParserResult,
    title: str | None = None,
    owner_user_id: int | None = None,
    lead_state: str | None = None,
    auto_created: bool = False,
) -> Lead:
    property_obj = _get_or_create_property(db, parser_result)
    source_label_map = {
        SourceChannel.telegram: "Telegram",
        SourceChannel.avito: "Avito",
        SourceChannel.yandex: "Яндекс Недвижимость",
        SourceChannel.bankrupt: "Банкротство",
    }
    lead = Lead(
        agency_id=parser_result.agency_id,
        property_id=property_obj.id,
        owner_user_id=owner_user_id,
        title=title or parser_result.title,
        contact_name=parser_result.contact_name,
        contact_phone=parser_result.contact_phone,
        contact_email=parser_result.contact_email,
        intent=parser_result.intent,
        status=LeadStatus.new_lead,
        source_channel=parser_result.source_channel,
        source_record_id=str(parser_result.id),
        lead_source=source_label_map.get(parser_result.source_channel, "Не выбрано"),
        lead_state=lead_state or "active",
        auto_created=auto_created,
    )
    db.add(lead)
    parser_result.status = ParserResultStatus.converted_to_lead
    db.flush()
    return lead


def parser_result_to_deal(
    db: Session,
    parser_result: ParserResult,
    title: str | None = None,
    owner_user_id: int | None = None,
    value_rub: float | None = None,
) -> Deal:
    lead = parser_result_to_lead(
        db,
        parser_result,
        title=title,
        owner_user_id=owner_user_id,
        lead_state="promoted_to_deal",
        auto_created=False,
    )
    deal = Deal(
        agency_id=parser_result.agency_id,
        property_id=lead.property_id,
        lead_id=lead.id,
        owner_user_id=owner_user_id,
        title=title or lead.title,
        status=DealStatus.new,
        value_rub=value_rub or parser_result.price_rub,
    )
    db.add(deal)
    parser_result.status = ParserResultStatus.converted_to_deal
    db.flush()
    return deal
