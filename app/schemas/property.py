from datetime import datetime

from pydantic import BaseModel, Field

from app.models.enums import PropertyDealType, PropertyType, SourceChannel


class PropertyCreate(BaseModel):
    title: str = Field(min_length=3, max_length=255)
    description: str | None = None
    address: str
    city: str
    region_code: str = "RU-UDM"
    latitude: float | None = None
    longitude: float | None = None
    area_sqm: float | None = None
    price_rub: float | None = None
    deal_type: PropertyDealType = PropertyDealType.rent
    property_type: PropertyType = PropertyType.office
    source_channel: SourceChannel = SourceChannel.manual
    source_external_id: str | None = None


class PropertyRead(BaseModel):
    id: int
    agency_id: int
    title: str
    description: str | None
    address: str
    city: str
    region_code: str
    latitude: float | None
    longitude: float | None
    area_sqm: float | None
    price_rub: float | None
    deal_type: PropertyDealType
    property_type: PropertyType
    source_channel: SourceChannel
    source_external_id: str | None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}

