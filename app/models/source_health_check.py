from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, Enum, ForeignKey, Integer, JSON, String
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.models.enums import SourceHealthStatus


class SourceHealthCheck(Base):
    __tablename__ = "source_health_checks"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    discovered_source_id: Mapped[int] = mapped_column(ForeignKey("discovered_sources.id"), nullable=False, index=True)
    checked_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    status: Mapped[SourceHealthStatus] = mapped_column(Enum(SourceHealthStatus), nullable=False, index=True)
    http_status: Mapped[int | None] = mapped_column(Integer, nullable=True)
    response_time_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    error_message: Mapped[str | None] = mapped_column(String(512), nullable=True)
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
