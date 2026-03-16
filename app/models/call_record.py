from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, Enum, ForeignKey, Integer, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.models.enums import CallDirection, CallStatus, TranscriptStatus


class CallRecord(Base):
    __tablename__ = "call_records"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    agency_id: Mapped[int] = mapped_column(ForeignKey("agencies.id"), nullable=False, index=True)
    assigned_user_id: Mapped[int | None] = mapped_column(ForeignKey("users.id"), nullable=True, index=True)
    lead_id: Mapped[int | None] = mapped_column(ForeignKey("leads.id"), nullable=True, index=True)
    deal_id: Mapped[int | None] = mapped_column(ForeignKey("deals.id"), nullable=True, index=True)
    provider: Mapped[str] = mapped_column(String(64), default="generic", nullable=False)
    external_call_id: Mapped[str | None] = mapped_column(String(255), nullable=True, index=True)
    direction: Mapped[CallDirection] = mapped_column(Enum(CallDirection), default=CallDirection.inbound, nullable=False)
    status: Mapped[CallStatus] = mapped_column(Enum(CallStatus), default=CallStatus.ringing, nullable=False, index=True)
    from_number: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    to_number: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    started_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True, index=True)
    ended_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    duration_sec: Mapped[int | None] = mapped_column(Integer, nullable=True)
    recording_url: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    recording_local_path: Mapped[str | None] = mapped_column(String(1024), nullable=True)
    transcript_status: Mapped[TranscriptStatus] = mapped_column(
        Enum(TranscriptStatus), default=TranscriptStatus.none, nullable=False, index=True
    )
    transcript_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    transcript_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    summary_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    extracted_entities: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

