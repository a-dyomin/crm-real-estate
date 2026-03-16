import hashlib
import re
from dataclasses import dataclass
from typing import Protocol

from sqlalchemy import Select, select
from sqlalchemy.orm import Session

from app.models.enums import ParserResultStatus, SourceChannel
from app.models.parser_result import ParserResult


class ParserPayload(Protocol):
    source_channel: SourceChannel
    source_external_id: str | None
    normalized_address: str | None
    area_sqm: float | None
    price_rub: float | None
    contact_phone: str | None
    contact_email: str | None


@dataclass
class DedupDecision:
    status: ParserResultStatus
    duplicate_of_id: int | None
    fingerprint: str | None


def _normalize_phone(value: str | None) -> str:
    if not value:
        return ""
    digits = re.sub(r"\D+", "", value)
    if digits.startswith("8") and len(digits) == 11:
        digits = "7" + digits[1:]
    return digits


def _normalize_email(value: str | None) -> str:
    return (value or "").strip().lower()


def _normalize_address(value: str | None) -> str:
    normalized = (value or "").strip().lower()
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized


def make_fingerprint(item: ParserPayload) -> str | None:
    base = "|".join(
        [
            _normalize_phone(item.contact_phone),
            _normalize_email(item.contact_email),
            _normalize_address(item.normalized_address),
            str(round(item.area_sqm or 0, 0)),
            str(round(item.price_rub or 0, -3)),
        ]
    )
    if base.count("|") == 4 and base.replace("|", "") == "":
        return None
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def _is_close(left: float | int | None, right: float | int | None, tolerance: float) -> bool:
    if left is None or right is None:
        return False
    if right == 0:
        return False
    delta = abs(float(left) - float(right)) / abs(float(right))
    return delta <= tolerance


def decide_parser_result_status(db: Session, agency_id: int, item: ParserPayload) -> DedupDecision:
    if item.source_external_id:
        same_source_stmt: Select[tuple[ParserResult]] = select(ParserResult).where(
            ParserResult.agency_id == agency_id,
            ParserResult.source_channel == item.source_channel,
            ParserResult.source_external_id == item.source_external_id,
        )
        same_source = db.execute(same_source_stmt).scalars().first()
        if same_source:
            return DedupDecision(
                status=ParserResultStatus.duplicate,
                duplicate_of_id=same_source.id,
                fingerprint=same_source.fingerprint,
            )

    fingerprint = make_fingerprint(item)
    if fingerprint:
        by_fingerprint_stmt: Select[tuple[ParserResult]] = select(ParserResult).where(
            ParserResult.agency_id == agency_id, ParserResult.fingerprint == fingerprint
        )
        by_fingerprint = db.execute(by_fingerprint_stmt).scalars().first()
        if by_fingerprint:
            return DedupDecision(
                status=ParserResultStatus.duplicate,
                duplicate_of_id=by_fingerprint.id,
                fingerprint=fingerprint,
            )

    address = _normalize_address(item.normalized_address)
    if address:
        near_stmt: Select[tuple[ParserResult]] = select(ParserResult).where(
            ParserResult.agency_id == agency_id, ParserResult.normalized_address == address
        )
        candidates = db.execute(near_stmt).scalars().all()
        for candidate in candidates:
            if _is_close(item.area_sqm, candidate.area_sqm, tolerance=0.15) and _is_close(
                item.price_rub, candidate.price_rub, tolerance=0.2
            ):
                return DedupDecision(
                    status=ParserResultStatus.possible_duplicate,
                    duplicate_of_id=candidate.id,
                    fingerprint=fingerprint,
                )

    return DedupDecision(status=ParserResultStatus.new, duplicate_of_id=None, fingerprint=fingerprint)
