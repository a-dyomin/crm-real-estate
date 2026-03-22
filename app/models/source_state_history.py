from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, Enum, ForeignKey, JSON, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.models.enums import SourceState


class SourceStateHistory(Base):
    __tablename__ = "source_state_history"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    parser_source_id: Mapped[int] = mapped_column(ForeignKey("parser_sources.id"), nullable=False, index=True)
    from_state: Mapped[SourceState | None] = mapped_column(Enum(SourceState), nullable=True)
    to_state: Mapped[SourceState] = mapped_column(Enum(SourceState), nullable=False, index=True)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    metadata_json: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
