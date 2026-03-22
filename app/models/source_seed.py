from datetime import datetime

from sqlalchemy import Boolean, DateTime, Enum, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base
from app.models.enums import DiscoverySeedType


class SourceSeed(Base):
    __tablename__ = "source_seeds"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    seed_type: Mapped[DiscoverySeedType] = mapped_column(Enum(DiscoverySeedType), nullable=False, index=True)
    value: Mapped[str] = mapped_column(String(1024), nullable=False, index=True)
    priority: Mapped[int] = mapped_column(Integer, default=0, nullable=False, index=True)
    region: Mapped[str | None] = mapped_column(String(64), nullable=True)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
