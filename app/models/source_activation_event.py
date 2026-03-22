from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, Enum, ForeignKey, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.models.enums import ActivationMode


class SourceActivationEvent(Base):
    __tablename__ = "source_activation_events"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    discovered_source_id: Mapped[int | None] = mapped_column(
        ForeignKey("discovered_sources.id"), nullable=True, index=True
    )
    parser_source_id: Mapped[int | None] = mapped_column(ForeignKey("parser_sources.id"), nullable=True, index=True)
    activation_mode: Mapped[ActivationMode] = mapped_column(
        Enum(ActivationMode), default=ActivationMode.manual, nullable=False, index=True
    )
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
