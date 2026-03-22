from datetime import datetime
from typing import Any

from sqlalchemy import DateTime, Float, ForeignKey, Integer, JSON, String
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class ContactIdentity(Base):
    __tablename__ = "contact_identities"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    agency_id: Mapped[int] = mapped_column(ForeignKey("agencies.id"), nullable=False, index=True)
    key_type: Mapped[str] = mapped_column(String(16), nullable=False, index=True)
    key_value: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    display_value: Mapped[str | None] = mapped_column(String(255), nullable=True)
    display_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    owner_probability: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    agent_probability: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    platform_probability: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    final_class: Mapped[str] = mapped_column(String(32), default="unknown", nullable=False)
    confidence: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    owner_priority: Mapped[str | None] = mapped_column(String(32), nullable=True)
    owner_priority_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    explanation: Mapped[dict[str, Any] | None] = mapped_column(JSON, nullable=True)
    organizations: Mapped[list[str] | None] = mapped_column(JSON, nullable=True)
    lifecycle_status: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    published_to_owners_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    promoted_to_call_center_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    total_listings: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    active_listings: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    unique_objects: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    unique_addresses: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    source_diversity: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    repost_rate: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    region_cluster: Mapped[str | None] = mapped_column(String(128), nullable=True)
    first_seen_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    last_seen_at: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
