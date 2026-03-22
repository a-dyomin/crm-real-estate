from datetime import datetime

from sqlalchemy import DateTime, Enum, ForeignKey, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.models.enums import SourceLinkType


class SourceLink(Base):
    __tablename__ = "source_links"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    from_source_id: Mapped[int | None] = mapped_column(ForeignKey("discovered_sources.id"), nullable=True, index=True)
    from_url: Mapped[str] = mapped_column(String(1024), nullable=False)
    to_domain: Mapped[str] = mapped_column(String(255), nullable=False, index=True)
    to_url: Mapped[str] = mapped_column(String(1024), nullable=False)
    anchor_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    link_type: Mapped[SourceLinkType] = mapped_column(Enum(SourceLinkType), nullable=False, index=True)
    discovered_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False, index=True)
