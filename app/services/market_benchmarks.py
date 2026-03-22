from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from statistics import median
from typing import Any

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.models.market_benchmark import MarketBenchmark
from app.models.parser_result import ParserResult
from app.models.enums import ParserResultStatus


AREA_BANDS = (
    (0, 10, "до 10"),
    (10, 30, "10-30"),
    (30, 80, "30-80"),
    (80, 150, "80-150"),
    (150, 300, "150-300"),
    (300, 500, "300-500"),
    (500, 800, "500-800"),
    (800, 1000, "800-1000"),
    (1000, 1500, "1000-1500"),
    (1500, 2000, "1500-2000"),
    (2000, 10_000_000, "2000+")
)


@dataclass(frozen=True)
class BenchmarkStats:
    median_price: float | None
    median_price_per_m2: float | None
    listing_count: int


def normalize_property_type(payload: dict[str, Any], title: str, description: str) -> str:
    hint = str(payload.get("property_type") or payload.get("propertyType") or "").lower()
    text = f"{title or ''} {description or ''}".lower()
    merged = f"{hint} {text}".strip()
    if "склад" in merged:
        return "warehouse"
    if "офис" in merged:
        return "office"
    if "производ" in merged or "индустри" in merged:
        return "industrial"
    if "торгов" in merged or "магаз" in merged:
        return "retail"
    if "земел" in merged or "участ" in merged:
        return "land"
    if "свобод" in merged or "псн" in merged:
        return "free_purpose"
    return "other"


def normalize_deal_type(listing_type: str | None, title: str, description: str) -> str:
    value = str(listing_type or "").lower()
    if value in ("rent", "lease", "аренда"):
        return "rent"
    if value in ("sale", "sell", "продажа"):
        return "sale"
    text = f"{title or ''} {description or ''}".lower()
    if "аренд" in text:
        return "rent"
    if "продаж" in text:
        return "sale"
    return "rent"


def area_band(area: float | None) -> str | None:
    if area is None:
        return None
    try:
        value = float(area)
    except (TypeError, ValueError):
        return None
    if value <= 0:
        return None
    for min_v, max_v, label in AREA_BANDS:
        if min_v < value <= max_v:
            return label
    return None


def price_per_m2(price: float | None, area: float | None) -> float | None:
    if price is None or area is None:
        return None
    try:
        price_val = float(price)
        area_val = float(area)
    except (TypeError, ValueError):
        return None
    if area_val <= 0:
        return None
    return price_val / area_val


def _segment_key(
    *,
    region: str | None,
    district: str | None,
    property_type: str | None,
    deal_type: str | None,
    band: str | None,
) -> tuple[str | None, str | None, str | None, str | None, str | None]:
    return (
        (region or None),
        (district or None),
        (property_type or None),
        (deal_type or None),
        (band or None),
    )


def recompute_market_benchmarks(db: Session, agency_id: int, *, window_days: int = 180) -> int:
    since = datetime.utcnow() - timedelta(days=window_days)
    results = (
        db.execute(
            select(ParserResult).where(
                ParserResult.agency_id == agency_id,
                ParserResult.price_rub.is_not(None),
                ParserResult.area_sqm.is_not(None),
                ParserResult.status != ParserResultStatus.rejected,
                ParserResult.updated_at >= since,
            )
        )
        .scalars()
        .all()
    )

    buckets: dict[tuple[str | None, str | None, str | None, str | None, str | None], dict[str, list[float]]] = defaultdict(
        lambda: {"prices": [], "per_m2": [], "dates": []}
    )
    for result in results:
        payload = result.payload or {}
        prop_type = normalize_property_type(payload, result.title, result.description or "")
        deal_type = normalize_deal_type(result.listing_type, result.title, result.description or "")
        band = area_band(result.area_sqm)
        region = result.region_code or result.city
        district = result.address_district
        key = _segment_key(
            region=region,
            district=district,
            property_type=prop_type,
            deal_type=deal_type,
            band=band,
        )
        buckets[key]["prices"].append(float(result.price_rub))
        per_m2 = price_per_m2(float(result.price_rub), float(result.area_sqm))
        if per_m2 is not None:
            buckets[key]["per_m2"].append(per_m2)
        if result.updated_at:
            buckets[key]["dates"].append(result.updated_at)

    db.execute(delete(MarketBenchmark).where(MarketBenchmark.agency_id == agency_id))
    db.commit()

    created = 0
    for key, values in buckets.items():
        prices = values["prices"]
        per_m2_values = values["per_m2"]
        if len(prices) < 3:
            continue
        region, district, prop_type, deal_type, band = key
        bench = MarketBenchmark(
            agency_id=agency_id,
            region=region,
            district=district,
            property_type=prop_type,
            deal_type=deal_type,
            area_band=band,
            median_price=median(prices) if prices else None,
            median_price_per_m2=median(per_m2_values) if per_m2_values else None,
            listing_count=len(prices),
            sample_from=min(values["dates"]) if values["dates"] else None,
            sample_to=max(values["dates"]) if values["dates"] else None,
        )
        db.add(bench)
        created += 1

    db.commit()
    return created


def build_benchmark_index(db: Session, agency_id: int) -> dict[tuple[str | None, str | None, str | None, str | None, str | None], MarketBenchmark]:
    rows = db.execute(select(MarketBenchmark).where(MarketBenchmark.agency_id == agency_id)).scalars().all()
    return {
        _segment_key(
            region=row.region,
            district=row.district,
            property_type=row.property_type,
            deal_type=row.deal_type,
            band=row.area_band,
        ): row
        for row in rows
    }


def lookup_benchmark(
    index: dict[tuple[str | None, str | None, str | None, str | None, str | None], MarketBenchmark],
    *,
    region: str | None,
    district: str | None,
    property_type: str | None,
    deal_type: str | None,
    band: str | None,
) -> MarketBenchmark | None:
    candidates = [
        _segment_key(region=region, district=district, property_type=property_type, deal_type=deal_type, band=band),
        _segment_key(region=region, district=None, property_type=property_type, deal_type=deal_type, band=band),
        _segment_key(region=None, district=None, property_type=property_type, deal_type=deal_type, band=band),
        _segment_key(region=region, district=None, property_type=property_type, deal_type=deal_type, band=None),
        _segment_key(region=None, district=None, property_type=property_type, deal_type=deal_type, band=None),
    ]
    for key in candidates:
        match = index.get(key)
        if match and (match.median_price_per_m2 or match.median_price):
            return match
    return None


def compute_market_deviation(price_per_m2: float | None, benchmark: MarketBenchmark | None) -> float | None:
    if price_per_m2 is None or benchmark is None or benchmark.median_price_per_m2 is None:
        return None
    if benchmark.median_price_per_m2 == 0:
        return None
    return (price_per_m2 - benchmark.median_price_per_m2) / benchmark.median_price_per_m2
