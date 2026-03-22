from datetime import datetime

from sqlalchemy import DateTime, Enum, ForeignKey, Integer, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.models.enums import SourceParseStatus


class SourceParseRun(Base):
    __tablename__ = "source_parse_runs"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    parser_source_id: Mapped[int] = mapped_column(ForeignKey("parser_sources.id"), nullable=False, index=True)
    job_run_id: Mapped[int | None] = mapped_column(ForeignKey("job_runs.id"), nullable=True, index=True)
    status: Mapped[SourceParseStatus] = mapped_column(Enum(SourceParseStatus), nullable=False, index=True)
    started_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    fetched_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    listings_parsed: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    inserted_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    duplicate_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    possible_duplicate_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    contacts_extracted: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    contacts_rejected: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    leads_published: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    error_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
