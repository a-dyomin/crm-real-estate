from datetime import datetime

from sqlalchemy import DateTime, Enum, ForeignKey, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.models.enums import ContactIntent, LeadStatus, SourceChannel


class Lead(Base):
    __tablename__ = "leads"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    agency_id: Mapped[int] = mapped_column(ForeignKey("agencies.id"), nullable=False, index=True)
    property_id: Mapped[int | None] = mapped_column(ForeignKey("properties.id"), nullable=True, index=True)
    owner_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id"), nullable=True, index=True)
    title: Mapped[str] = mapped_column(String(255), nullable=False)
    contact_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    contact_phone: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    contact_email: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    intent: Mapped[ContactIntent] = mapped_column(Enum(ContactIntent), default=ContactIntent.unknown, nullable=False)
    status: Mapped[LeadStatus] = mapped_column(Enum(LeadStatus), default=LeadStatus.new_lead, nullable=False)
    source_channel: Mapped[SourceChannel] = mapped_column(Enum(SourceChannel), default=SourceChannel.manual, nullable=False)
    source_record_id: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    lead_source: Mapped[str | None] = mapped_column(String(255), nullable=True)
    need_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    search_districts: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    object_address: Mapped[str | None] = mapped_column(String(255), nullable=True)
    property_type: Mapped[str | None] = mapped_column(String(64), nullable=True)
    area_range: Mapped[str | None] = mapped_column(String(64), nullable=True)
    business_activity: Mapped[str | None] = mapped_column(String(128), nullable=True)
    urgency: Mapped[str | None] = mapped_column(String(64), nullable=True)
    source_details: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
