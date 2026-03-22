from datetime import datetime

from sqlalchemy import DateTime, Float, ForeignKey, String, Text
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class GraphFuzzyMatch(Base):
    __tablename__ = "graph_fuzzy_matches"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    agency_id: Mapped[int] = mapped_column(ForeignKey("agencies.id"), nullable=False, index=True)
    match_type: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    from_node_id: Mapped[int | None] = mapped_column(ForeignKey("graph_nodes.id"), nullable=True, index=True)
    to_node_id: Mapped[int | None] = mapped_column(ForeignKey("graph_nodes.id"), nullable=True, index=True)
    raw_value_a: Mapped[str] = mapped_column(String(255), nullable=False)
    raw_value_b: Mapped[str] = mapped_column(String(255), nullable=False)
    normalized_value_a: Mapped[str] = mapped_column(String(255), nullable=False)
    normalized_value_b: Mapped[str] = mapped_column(String(255), nullable=False)
    similarity_score: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    accepted_confidence: Mapped[float] = mapped_column(Float, default=0.0, nullable=False)
    explanation: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
