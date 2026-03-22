from __future__ import annotations

from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.parser_result import ParserResult
from app.services.lead_scoring import score_lead
from app.services.market_benchmarks import (
    area_band,
    build_benchmark_index,
    compute_market_deviation,
    lookup_benchmark,
    normalize_deal_type,
    normalize_property_type,
    price_per_m2,
)


def update_parser_scores(db: Session, agency_id: int, *, since: datetime | None = None) -> int:
    index = build_benchmark_index(db, agency_id)
    stmt = select(ParserResult).where(ParserResult.agency_id == agency_id)
    if since:
        stmt = stmt.where(ParserResult.updated_at >= since)
    results = db.execute(stmt).scalars().all()
    updated = 0

    for result in results:
        payload: dict[str, Any] = dict(result.payload or {})

        owner_score = payload.get("owner_intel_score")
        owner_conf = payload.get("owner_intel_confidence")
        owner_summary = payload.get("owner_intel_summary")
        owner_priority_score = payload.get("owner_intel_priority_score")
        if owner_score is not None:
            payload["owner_probability_score"] = owner_score
        if owner_conf is not None:
            payload["owner_confidence"] = owner_conf
        if owner_summary:
            payload["owner_explanation_summary"] = owner_summary
        if owner_priority_score is not None:
            payload["owner_priority_score"] = owner_priority_score

        prop_type = normalize_property_type(payload, result.title, result.description or "")
        deal_type = normalize_deal_type(result.listing_type, result.title, result.description or "")
        band = area_band(result.area_sqm)
        region = result.region_code or result.city
        district = result.address_district
        benchmark = lookup_benchmark(
            index,
            region=region,
            district=district,
            property_type=prop_type,
            deal_type=deal_type,
            band=band,
        )
        price_per_m2_value = price_per_m2(
            float(result.price_rub) if result.price_rub is not None else None,
            float(result.area_sqm) if result.area_sqm is not None else None,
        )
        deviation = compute_market_deviation(price_per_m2_value, benchmark)
        if benchmark:
            payload["market_median_price"] = benchmark.median_price
            payload["market_median_price_per_m2"] = benchmark.median_price_per_m2
        else:
            payload["market_median_price"] = None
            payload["market_median_price_per_m2"] = None
        payload["deviation_from_market_pct"] = deviation
        payload["below_market_flag"] = deviation is not None and deviation <= -0.1

        score = score_lead(payload, status=result.status)
        payload["lead_score"] = score.score
        payload["lead_score_breakdown"] = score.breakdown.__dict__
        payload["lead_score_freshness_hours"] = score.freshness_hours
        payload["monetization_tier"] = score.monetization_tier
        payload["owner_probability_score"] = payload.get("owner_probability_score") or score.breakdown.owner_probability_score

        result.lead_score = score.score
        result.owner_probability_score = payload.get("owner_probability_score")
        result.owner_priority_score = payload.get("owner_priority_score")
        result.owner_confidence = payload.get("owner_confidence")
        result.owner_explanation_summary = payload.get("owner_explanation_summary")
        result.market_median_price = payload.get("market_median_price")
        result.market_median_price_per_m2 = payload.get("market_median_price_per_m2")
        result.deviation_from_market_pct = payload.get("deviation_from_market_pct")
        result.below_market_flag = payload.get("below_market_flag")
        result.payload = payload
        updated += 1

    db.commit()
    return updated
