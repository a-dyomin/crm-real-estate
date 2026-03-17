from datetime import datetime
from typing import Any

from sqlalchemy import Boolean, DateTime, Enum, ForeignKey, Integer, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.models.enums import SourceChannel


class ParserSource(Base):
    __tablename__ = "parser_sources"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    agency_id: Mapped[int] = mapped_column(ForeignKey("agencies.id"), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    source_channel: Mapped[SourceChannel] = mapped_column(Enum(SourceChannel), nullable=False, index=True)
    source_url: Mapped[str] = mapped_column(String(1024), nullable=False)
    city: Mapped[str | None] = mapped_column(String(128), nullable=True)
    region_code: Mapped[str] = mapped_column(String(32), default="RU-UDM", nullable=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False, index=True)
    poll_minutes: Mapped[int] = mapped_column(Integer, default=1440, nullable=False)
    max_items_per_run: Mapped[int] = mapped_column(Integer, default=10000, nullable=False)
    extra_config: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    last_run_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
