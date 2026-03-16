from datetime import date, datetime

from pydantic import BaseModel, Field

from app.models.enums import DealStatus


class DealCreate(BaseModel):
    title: str = Field(min_length=3, max_length=255)
    property_id: int | None = None
    lead_id: int | None = None
    owner_user_id: int | None = None
    status: DealStatus = DealStatus.new
    value_rub: float | None = None
    expected_close_date: date | None = None


class DealUpdateStatus(BaseModel):
    status: DealStatus


class DealRead(BaseModel):
    id: int
    agency_id: int
    property_id: int | None
    lead_id: int | None
    owner_user_id: int | None
    title: str
    status: DealStatus
    value_rub: float | None
    expected_close_date: date | None
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}

