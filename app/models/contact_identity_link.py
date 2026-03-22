from datetime import datetime

from sqlalchemy import DateTime, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class ContactIdentityLink(Base):
    __tablename__ = "contact_identity_links"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    contact_identity_id: Mapped[int] = mapped_column(
        ForeignKey("contact_identities.id"), nullable=False, index=True
    )
    parser_result_id: Mapped[int] = mapped_column(ForeignKey("parser_results.id"), nullable=False, index=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, nullable=False)
