from datetime import datetime

from sqlalchemy import DateTime, Enum, Float, ForeignKey, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.models.enums import PropertyDealType, PropertyType, SourceChannel


class Property(Base):
    __tablename__ = "properties"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    agency_id: Mapped[int] = mapped_column(ForeignKey("agencies.id"), nullable=False, index=True)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    address: Mapped[str] = mapped_column(String(512), nullable=False, index=True)
    city: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    region_code: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    latitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    longitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    area_sqm: Mapped[float | None] = mapped_column(Float, nullable=True)
    price_rub: Mapped[float | None] = mapped_column(Numeric(14, 2), nullable=True)
    deal_type: Mapped[PropertyDealType] = mapped_column(Enum(PropertyDealType), default=PropertyDealType.rent, nullable=False)
    property_type: Mapped[PropertyType] = mapped_column(Enum(PropertyType), default=PropertyType.office, nullable=False)
    source_channel: Mapped[SourceChannel] = mapped_column(Enum(SourceChannel), default=SourceChannel.manual, nullable=False)
    source_external_id: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

