from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import re
from typing import Any

from app.models.enums import ParserResultStatus


@dataclass(frozen=True)
class LeadScoreBreakdown:
    freshness_score: float
    owner_probability_score: float
    under_market_score: float
    uniqueness_score: float
    urgency_score: float
    completeness_score: float
    spam_risk_score: float


@dataclass(frozen=True)
class LeadScoreResult:
    score: float
    breakdown: LeadScoreBreakdown
    freshness_hours: float
    monetization_tier: str


URGENCY_KEYWORDS = (
    "срочно",
    "urgent",
    "торг",
    "скидк",
    "price reduced",
    "цена сниж",
    "без комиссии",
)

OWNER_KEYWORDS = (
    "собственник",
    "owner",
    "без комиссии",
    "без посредников",
    "без агент",
)

AGENT_KEYWORDS = (
    "агент",
    "риэлтор",
    "агентство",
    "broker",
    "agency",
)

DISCOUNT_KEYWORDS = (
    "скидк",
    "торг",
    "цена сниж",
    "price reduced",
    "дешев",
    "ниже рынка",
)


def _text(value: str | None) -> str:
    return " ".join((value or "").split()).strip().lower()


def _parse_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except Exception:
            return None
    if isinstance(value, str):
        candidate = value.strip()
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(candidate, fmt).replace(tzinfo=timezone.utc)
            except Exception:
                continue
    return None


def _freshness_score(age_hours: float) -> float:
    if age_hours <= 6:
        return 100.0
    if age_hours <= 24:
        return 85.0
    if age_hours <= 72:
        return 70.0
    if age_hours <= 168:
        return 50.0
    if age_hours <= 720:
        return 30.0
    return 10.0


def _owner_probability(text: str, has_phone: bool) -> float:
    score = 20.0
    if has_phone:
        score += 25.0
    if any(keyword in text for keyword in OWNER_KEYWORDS):
        score += 35.0
    if any(keyword in text for keyword in AGENT_KEYWORDS):
        score -= 30.0
    return float(max(0.0, min(100.0, score)))


def _under_market_score(*, text: str, price_per_m2: float | None, deviation_from_market_pct: float | None) -> float:
    if deviation_from_market_pct is not None:
        if deviation_from_market_pct <= -0.2:
            return 90.0
        if deviation_from_market_pct <= -0.1:
            return 75.0
        if deviation_from_market_pct <= -0.05:
            return 65.0
        if deviation_from_market_pct <= 0.05:
            return 50.0
        if deviation_from_market_pct >= 0.2:
            return 25.0
        return 35.0
    base = 20.0
    if price_per_m2 is not None:
        base = 45.0
    if any(keyword in text for keyword in DISCOUNT_KEYWORDS):
        base += 25.0
    return float(max(0.0, min(100.0, base)))


def _uniqueness_score(status: ParserResultStatus) -> float:
    if status == ParserResultStatus.duplicate:
        return 10.0
    if status == ParserResultStatus.possible_duplicate:
        return 40.0
    return 80.0


def _urgency_score(text: str) -> float:
    return 80.0 if any(keyword in text for keyword in URGENCY_KEYWORDS) else 30.0


def _completeness_score(fields: dict[str, Any]) -> float:
    required = [
        fields.get("title"),
        fields.get("price_rub"),
        fields.get("area_sqm"),
        fields.get("normalized_address") or fields.get("address"),
        fields.get("contact_phone") or fields.get("contact_email"),
    ]
    total = len(required)
    present = sum(1 for value in required if value)
    return float(round((present / total) * 100.0, 2))


def _spam_risk_score(text: str, has_contact: bool, status: ParserResultStatus) -> float:
    score = 0.0
    if len(text) < 12:
        score += 30.0
    if not has_contact:
        score += 30.0
    if status in {ParserResultStatus.duplicate, ParserResultStatus.possible_duplicate}:
        score += 25.0
    if re.search(r"[!]{3,}", text):
        score += 15.0
    return float(max(0.0, min(100.0, score)))


def score_lead(payload: dict[str, Any], *, status: ParserResultStatus, now: datetime | None = None) -> LeadScoreResult:
    now = now or datetime.now(timezone.utc)
    published_at = _parse_datetime(payload.get("published_at") or payload.get("publication_date"))
    timestamp = published_at or now
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    age_hours = max(0.0, (now - timestamp).total_seconds() / 3600.0)

    title = _text(payload.get("title"))
    description = _text(payload.get("description"))
    text = " ".join([title, description]).strip()

    has_phone = bool(payload.get("contact_phone"))
    has_contact = has_phone or bool(payload.get("contact_email"))

    price = payload.get("price_rub")
    area = payload.get("area_sqm")
    price_per_m2 = None
    if isinstance(price, (int, float)) and isinstance(area, (int, float)) and area:
        price_per_m2 = float(price) / float(area)

    owner_override = payload.get("owner_probability_score") or payload.get("owner_intel_score")
    owner_probability = (
        float(owner_override)
        if isinstance(owner_override, (int, float)) and not isinstance(owner_override, bool)
        else _owner_probability(text, has_phone)
    )
    deviation = payload.get("deviation_from_market_pct")
    deviation = float(deviation) if isinstance(deviation, (int, float)) and not isinstance(deviation, bool) else None

    freshness = _freshness_score(age_hours)
    under_market = _under_market_score(text=text, price_per_m2=price_per_m2, deviation_from_market_pct=deviation)
    uniqueness = _uniqueness_score(status)
    urgency = _urgency_score(text)
    completeness = _completeness_score(payload)
    spam_risk = _spam_risk_score(text, has_contact, status)

    score = (
        freshness * 0.25
        + owner_probability * 0.20
        + under_market * 0.20
        + uniqueness * 0.10
        + urgency * 0.10
        + completeness * 0.10
        - spam_risk * 0.05
    )
    score = float(max(0.0, min(100.0, round(score, 2))))

    tier = "premium" if age_hours <= 24 * 30 else "archive"
    breakdown = LeadScoreBreakdown(
        freshness_score=freshness,
        owner_probability_score=owner_probability,
        under_market_score=under_market,
        uniqueness_score=uniqueness,
        urgency_score=urgency,
        completeness_score=completeness,
        spam_risk_score=spam_risk,
    )
    return LeadScoreResult(
        score=score,
        breakdown=breakdown,
        freshness_hours=round(age_hours, 2),
        monetization_tier=tier,
    )
