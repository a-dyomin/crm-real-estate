from datetime import datetime
from typing import Any

from sqlalchemy import Boolean, DateTime, Enum, Float, ForeignKey, JSON, Numeric, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.models.enums import ContactIntent, ParserResultStatus, SourceChannel


class ParserResult(Base):
    __tablename__ = "parser_results"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    agency_id: Mapped[int] = mapped_column(ForeignKey("agencies.id"), nullable=False, index=True)
    source_channel: Mapped[SourceChannel] = mapped_column(Enum(SourceChannel), nullable=False, index=True)
    source_external_id: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    raw_url: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    telegram_post_url: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    listing_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    image_url: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    normalized_address: Mapped[str | None] = mapped_column(String(512), nullable=True, index=True)
    address_district: Mapped[str | None] = mapped_column(String(128), nullable=True)
    address_street: Mapped[str | None] = mapped_column(String(255), nullable=True)
    city: Mapped[str | None] = mapped_column(String(128), nullable=True)
    region_code: Mapped[str | None] = mapped_column(String(32), nullable=True)
    latitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    longitude: Mapped[float | None] = mapped_column(Float, nullable=True)
    area_sqm: Mapped[float | None] = mapped_column(Float, nullable=True)
    price_rub: Mapped[float | None] = mapped_column(Numeric(14, 2), nullable=True)
    contact_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    contact_phone: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    contact_email: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    contact_candidates: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON, nullable=True)
    selected_contact: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    rejected_contacts: Mapped[list[dict[str, Any]] | None] = mapped_column(JSON, nullable=True)
    contact_rejection_reasons: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    contact_confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    lead_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    owner_probability_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    owner_priority_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    owner_confidence: Mapped[float | None] = mapped_column(Float, nullable=True)
    owner_explanation_summary: Mapped[str | None] = mapped_column(String(512), nullable=True)
    market_median_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    market_median_price_per_m2: Mapped[float | None] = mapped_column(Float, nullable=True)
    deviation_from_market_pct: Mapped[float | None] = mapped_column(Float, nullable=True)
    below_market_flag: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    intent: Mapped[ContactIntent] = mapped_column(Enum(ContactIntent), default=ContactIntent.unknown, nullable=False)
    status: Mapped[ParserResultStatus] = mapped_column(Enum(ParserResultStatus), default=ParserResultStatus.new, nullable=False, index=True)
    duplicate_of_id: Mapped[int | None] = mapped_column(ForeignKey("parser_results.id"), nullable=True, index=True)
    fingerprint: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    payload: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
