from datetime import datetime

from sqlalchemy import DateTime, Float, ForeignKey, Integer, String
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class MarketBenchmark(Base):
    __tablename__ = "market_benchmarks"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    agency_id: Mapped[int] = mapped_column(ForeignKey("agencies.id"), nullable=False, index=True)
    region: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    district: Mapped[str | None] = mapped_column(String(128), nullable=True, index=True)
    property_type: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    deal_type: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    area_band: Mapped[str | None] = mapped_column(String(32), nullable=True, index=True)
    median_price: Mapped[float | None] = mapped_column(Float, nullable=True)
    median_price_per_m2: Mapped[float | None] = mapped_column(Float, nullable=True)
    listing_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    sample_from: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    sample_to: Mapped[datetime | None] = mapped_column(DateTime, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
