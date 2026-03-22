from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, Float, ForeignKey, JSON
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class SourceTemplateMatch(Base):
    __tablename__ = "source_template_matches"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    discovered_source_id: Mapped[int] = mapped_column(ForeignKey("discovered_sources.id"), nullable=False, index=True)
    parser_template_id: Mapped[int] = mapped_column(ForeignKey("parser_templates.id"), nullable=False, index=True)
    confidence: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    generated_parser_config: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    matched_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
