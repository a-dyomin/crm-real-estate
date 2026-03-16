import hashlib
import json
import re
import xml.etree.ElementTree as ET
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup

from app.core.config import get_settings
from app.models.enums import ContactIntent, SourceChannel
from app.models.parser_source import ParserSource
from app.schemas.parser import ParserIngestItem

settings = get_settings()
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)
BLOCKED_STATUS_CODES = {401, 403, 429}
BLOCKED_TEXT_MARKERS = (
    "captcha",
    "\u0434\u043e\u0441\u0442\u0443\u043f \u043e\u0433\u0440\u0430\u043d\u0438\u0447\u0435\u043d",
    "__qrator",
    "qauth_utm",
    "access denied",
)
URL_RE = re.compile(r"https?://[^\s<>\")\]]+")
PHONE_RE = re.compile(r"(?:\+7|8)[\s\-\(\)]*\d{3}[\s\-\(\)]*\d{3}[\s\-]*\d{2}[\s\-]*\d{2}")
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PRICE_RE = re.compile(r"(\d[\d\s]{2,})\s*(?:\u0440\u0443\u0431|\u20bd)", re.IGNORECASE)
AREA_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*(?:\u043c2|\u043c\u00b2|\u043a\u0432\.?\s*\u043c|sqm|sq\.?\s*m)", re.IGNORECASE)
ADDRESS_RE = re.compile(r"(?:\u0430\u0434\u0440\u0435\u0441|location)\s*[:\-]\s*([^\n\r|;]{8,150})", re.IGNORECASE)
DEFAULT_TELEGRAM_COMMERCIAL_KEYWORDS = (
    "\u043a\u043e\u043c\u043c\u0435\u0440\u0447\u0435\u0441\u043a",
    "\u043e\u0444\u0438\u0441",
    "\u0441\u043a\u043b\u0430\u0434",
    "\u0442\u043e\u0440\u0433\u043e\u0432",
    "\u043f\u043e\u043c\u0435\u0449\u0435\u043d",
    "\u0430\u0440\u0435\u043d\u0434\u0430",
    "\u043f\u0440\u043e\u0434\u0430\u0436\u0430",
    "business center",
    "warehouse",
    "retail",
    "office",
)
DEFAULT_TELEGRAM_UDMURTIA_KEYWORDS = (
    "\u0443\u0434\u043c\u0443\u0440\u0442",
    "\u0443\u0434\u043c\u0443\u0440\u0442\u0438\u044f",
    "\u0438\u0436\u0435\u0432\u0441\u043a",
    "\u0441\u0430\u0440\u0430\u043f\u0443\u043b",
    "\u0433\u043b\u0430\u0437\u043e\u0432",
    "\u0432\u043e\u0442\u043a\u0438\u043d\u0441\u043a",
    "\u043c\u043e\u0436\u0433\u0430",
    "\u043a\u0430\u043c\u0431\u0430\u0440\u043a\u0430",
    "\u0431\u0430\u043b\u0435\u0437\u0438\u043d\u043e",
    "udmurt",
    "udmurtia",
    "izhevsk",
)


def _normalize_text(value: str | None) -> str:
    return " ".join((value or "").split()).strip()


def _looks_like_html(value: str) -> bool:
    probe = value[:1000].lower()
    return "<html" in probe or "<body" in probe or "<!doctype html" in probe


def _build_mirror_url(url: str) -> str:
    parsed = urlparse(url)
    target = f"http://{parsed.netloc}{parsed.path or ''}"
    if parsed.query:
        target = f"{target}?{parsed.query}"
    mirror_base = settings.parser_mirror_base_url.strip()
    if "{url}" in mirror_base:
        return mirror_base.format(url=target)
    if mirror_base.endswith("/"):
        return f"{mirror_base}{target}"
    return f"{mirror_base}/{target}"


def _is_blocked_response(response: requests.Response) -> bool:
    if response.status_code in BLOCKED_STATUS_CODES:
        return True
    sample = response.text[:5000].lower()
    return any(marker in sample for marker in BLOCKED_TEXT_MARKERS)


def _fetch_text(url: str) -> str:
    direct_error: Exception | None = None
    try:
        response = requests.get(
            url,
            headers={"User-Agent": USER_AGENT, "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8"},
            timeout=settings.parser_request_timeout_sec,
        )
        if _is_blocked_response(response):
            raise ValueError(f"Source access blocked for URL: {url}")
        response.raise_for_status()
        text = response.text or ""
        if not text.strip():
            raise ValueError("Source returned empty response body.")
        return text
    except Exception as exc:
        direct_error = exc

    if not settings.parser_mirror_fallback_enabled:
        raise direct_error

    mirror_url = _build_mirror_url(url)
    try:
        response = requests.get(
            mirror_url,
            headers={"User-Agent": USER_AGENT, "Accept-Language": "ru-RU,ru;q=0.9,en;q=0.8"},
            timeout=settings.parser_request_timeout_sec,
        )
        response.raise_for_status()
        text = response.text or ""
        if not text.strip():
            raise ValueError("Mirror returned empty response body.")
        return text
    except Exception as mirror_exc:
        raise ValueError(
            f"Source fetch failed for '{url}'. Direct error: {direct_error}. Mirror error: {mirror_exc}."
        ) from mirror_exc


def _extract_first(pattern: re.Pattern[str], text: str) -> str | None:
    match = pattern.search(text)
    if not match:
        return None
    value = match.group(1) if match.lastindex else match.group(0)
    return _normalize_text(value)


def _extract_price(text: str) -> float | None:
    match = PRICE_RE.search(text)
    if not match:
        return None
    digits = re.sub(r"\D+", "", match.group(1))
    return float(digits) if digits else None


def _extract_area(text: str) -> float | None:
    match = AREA_RE.search(text)
    if not match:
        return None
    raw = match.group(1).replace(",", ".")
    try:
        return float(raw)
    except ValueError:
        return None


def _to_float(value: object) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip().replace(",", ".")
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def _detect_intent(text: str) -> ContactIntent:
    normalized = text.lower()
    if any(
        keyword in normalized
        for keyword in (
            "\u0438\u043d\u0432\u0435\u0441\u0442",
            "\u0434\u043e\u0445\u043e\u0434\u043d\u043e\u0441\u0442",
            "\u043e\u043a\u0443\u043f\u0430\u0435\u043c",
            "cap rate",
        )
    ):
        return ContactIntent.investor
    if any(
        keyword in normalized
        for keyword in (
            "\u0441\u043d\u0438\u043c\u0443",
            "\u0438\u0449\u0443 \u0430\u0440\u0435\u043d\u0434\u0443",
            "\u0430\u0440\u0435\u043d\u0434\u0443\u044e",
            "take lease",
        )
    ):
        return ContactIntent.tenant
    if any(
        keyword in normalized
        for keyword in (
            "\u043f\u0440\u043e\u0434\u0430\u043c",
            "\u043f\u0440\u043e\u0434\u0430\u0436\u0430",
            "\u0441\u043e\u0431\u0441\u0442\u0432\u0435\u043d\u043d\u0438\u043a",
            "owner",
        )
    ):
        return ContactIntent.seller
    return ContactIntent.unknown


def _extract_external_id(url: str, fallback_seed: str) -> str:
    parsed = urlparse(url)
    match = re.search(r"(\d{5,})", parsed.path)
    if match:
        return match.group(1)
    return hashlib.sha1(fallback_seed.encode("utf-8")).hexdigest()[:20]


def _normalize_url_candidate(candidate: str) -> str:
    return candidate.rstrip(").,]")


def _is_listing_url(source_channel: SourceChannel, resolved_url: str) -> bool:
    parsed = urlparse(resolved_url)
    host = parsed.netloc.lower()
    path = parsed.path.lower()
    if source_channel == SourceChannel.avito:
        return "avito.ru" in host and bool(re.search(r"_\d{5,}", path))
    if source_channel == SourceChannel.cian:
        return "cian.ru" in host and bool(re.search(r"/(?:rent|sale)/commercial/\d+/?$", path))
    if source_channel == SourceChannel.domclick:
        return "domclick.ru" in host and ("/card/" in path or "commerce" in path)
    return False


def _canonical_listing_url(url: str) -> str:
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"


def _extract_listing_links(source_channel: SourceChannel, base_url: str, source_text: str) -> list[str]:
    links: set[str] = set()
    soup = BeautifulSoup(source_text, "html.parser")
    for anchor in soup.select("a[href]"):
        href = (anchor.get("href") or "").strip()
        if not href:
            continue
        resolved = urljoin(base_url, href)
        if _is_listing_url(source_channel, resolved):
            links.add(_canonical_listing_url(resolved))

    if links:
        return sorted(links)

    for raw_url in URL_RE.findall(source_text):
        normalized = _normalize_url_candidate(raw_url)
        resolved = urljoin(base_url, normalized)
        if _is_listing_url(source_channel, resolved):
            links.add(_canonical_listing_url(resolved))
    return sorted(links)


def _json_ld_text(soup: BeautifulSoup) -> str:
    chunks: list[str] = []
    for script in soup.select('script[type="application/ld+json"]'):
        raw = script.string or script.get_text(" ", strip=True)
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        chunks.append(_normalize_text(json.dumps(data, ensure_ascii=False)))
    return " ".join(chunks)


def _strip_markdown(value: str) -> str:
    text = re.sub(r"\[([^\]]+)\]\([^)]+\)", r"\1", value)
    text = re.sub(r"[*`>#_]+", " ", text)
    return _normalize_text(text)


def _extract_markdown_title(value: str) -> str | None:
    title_match = re.search(r"^\s*Title:\s*(.+?)\s*$", value, re.IGNORECASE | re.MULTILINE)
    if title_match:
        title = _normalize_text(title_match.group(1))
        if title:
            return title
    for line in value.splitlines():
        candidate = _normalize_text(line)
        if not candidate:
            continue
        if candidate.lower().startswith(("url source:", "markdown content:")):
            continue
        return candidate
    return None


def _build_item_from_detail(source: ParserSource, listing_url: str, raw_text: str) -> ParserIngestItem:
    if _looks_like_html(raw_text):
        soup = BeautifulSoup(raw_text, "html.parser")
        title_node = soup.select_one("h1")
        page_title = (
            title_node.get_text(" ", strip=True)
            if title_node
            else soup.title.get_text(" ", strip=True)
            if soup.title
            else ""
        )
        description_node = soup.select_one('meta[name="description"]')
        description = description_node.get("content", "") if description_node else ""
        body_text = soup.get_text(" ", strip=True)
        text = _normalize_text(" ".join([page_title, description, body_text[:8000], _json_ld_text(soup)]))
    else:
        page_title = _extract_markdown_title(raw_text) or f"{source.source_channel.value} listing"
        text = _strip_markdown(raw_text)[:12000]

    address_match = ADDRESS_RE.search(text)
    address = _normalize_text(address_match.group(1)) if address_match else None

    return ParserIngestItem(
        source_channel=source.source_channel,
        source_external_id=_extract_external_id(listing_url, listing_url),
        raw_url=listing_url,
        title=page_title[:255] if page_title else f"{source.source_channel.value} listing",
        description=text[:4000],
        normalized_address=address,
        city=source.city,
        region_code=source.region_code,
        area_sqm=_extract_area(text),
        price_rub=_extract_price(text),
        contact_name=source.name,
        contact_phone=_extract_first(PHONE_RE, text),
        contact_email=_extract_first(EMAIL_RE, text),
        intent=_detect_intent(text),
        payload={"source_name": source.name, "source_url": source.source_url, "parser": "html_scraper"},
    )


def _collect_marketplace_items(source: ParserSource) -> list[ParserIngestItem]:
    source_text = _fetch_text(source.source_url)
    links = _extract_listing_links(source.source_channel, source.source_url, source_text)
    if not links:
        raise ValueError("No listing links were extracted from source page.")

    max_items = min(source.max_items_per_run, settings.parser_max_items_per_source, len(links))
    detail_limit = min(max_items, settings.parser_detail_fetch_limit)
    items: list[ParserIngestItem] = []
    parse_errors: list[str] = []
    for link in links[:detail_limit]:
        try:
            detail_text = _fetch_text(link)
            items.append(_build_item_from_detail(source, link, detail_text))
        except Exception as exc:
            parse_errors.append(f"{link}: {type(exc).__name__}")

    if not items:
        summary = "; ".join(parse_errors[:3])
        if summary:
            raise ValueError(f"No listing details parsed from source links. {summary}")
        raise ValueError("No listing details parsed from source links.")
    return items


def _normalize_telegram_url(source_url: str) -> str:
    candidate = source_url.strip()
    if candidate.startswith("https://t.me/s/"):
        return candidate
    if candidate.startswith("https://t.me/"):
        handle = candidate.replace("https://t.me/", "", 1).split("?", maxsplit=1)[0].strip("/")
        return f"https://t.me/s/{handle}"
    if candidate.startswith("@"):
        return f"https://t.me/s/{candidate[1:]}"
    return f"https://t.me/s/{candidate}"


def _telegram_filter_keywords(raw_value: object, defaults: tuple[str, ...]) -> tuple[str, ...]:
    if isinstance(raw_value, list):
        cleaned = [str(item).strip().lower() for item in raw_value if str(item).strip()]
        if cleaned:
            return tuple(cleaned)
    return defaults


def _passes_telegram_filters(text: str, source: ParserSource) -> bool:
    normalized = text.lower()
    extra_config = source.extra_config if isinstance(source.extra_config, dict) else {}
    raw_filters = extra_config.get("telegram_filters")
    filters = raw_filters if isinstance(raw_filters, dict) else {}
    commercial_only = bool(filters.get("commercial_only", True))
    udmurtia_only = bool(filters.get("udmurtia_only", True))
    commercial_keywords = _telegram_filter_keywords(
        filters.get("commercial_keywords"), DEFAULT_TELEGRAM_COMMERCIAL_KEYWORDS
    )
    udmurtia_keywords = _telegram_filter_keywords(filters.get("region_keywords"), DEFAULT_TELEGRAM_UDMURTIA_KEYWORDS)

    if commercial_only and not any(keyword in normalized for keyword in commercial_keywords):
        return False
    if udmurtia_only and not any(keyword in normalized for keyword in udmurtia_keywords):
        return False
    return True


def _collect_telegram_items(source: ParserSource) -> list[ParserIngestItem]:
    source_url = _normalize_telegram_url(source.source_url)
    html = _fetch_text(source_url)
    soup = BeautifulSoup(html, "html.parser")
    messages = soup.select("div.tgme_widget_message")
    if not messages:
        raise ValueError("No telegram messages found on source page.")

    items: list[ParserIngestItem] = []
    max_items = min(source.max_items_per_run, settings.parser_max_items_per_source)
    for message in messages:
        if len(items) >= max_items:
            break
        external_id = message.get("data-post") or ""
        date_link = message.select_one("a.tgme_widget_message_date")
        message_url = date_link.get("href") if date_link else source_url
        text_node = message.select_one(".tgme_widget_message_text")
        text = _normalize_text(text_node.get_text(" ", strip=True) if text_node else "")
        if not text:
            continue
        if not _passes_telegram_filters(text, source):
            continue
        title = text[:120]
        items.append(
            ParserIngestItem(
                source_channel=SourceChannel.telegram,
                source_external_id=external_id or _extract_external_id(message_url, text),
                raw_url=message_url,
                telegram_post_url=message_url,
                title=title,
                description=text[:4000],
                normalized_address=_extract_first(ADDRESS_RE, text),
                city=source.city,
                region_code=source.region_code,
                area_sqm=_extract_area(text),
                price_rub=_extract_price(text),
                contact_name=source.name,
                contact_phone=_extract_first(PHONE_RE, text),
                contact_email=_extract_first(EMAIL_RE, text),
                intent=_detect_intent(text),
                payload={"source_name": source.name, "source_url": source_url, "parser": "telegram_channel"},
            )
        )
    return items


def _collect_rss_items(source: ParserSource) -> list[ParserIngestItem]:
    xml_text = _fetch_text(source.source_url)
    root = ET.fromstring(xml_text)
    max_items = min(source.max_items_per_run, settings.parser_max_items_per_source)
    items: list[ParserIngestItem] = []
    for node in root.findall(".//item")[:max_items]:
        title = _normalize_text(node.findtext("title") or "")
        link = _normalize_text(node.findtext("link") or source.source_url)
        description = _normalize_text(node.findtext("description") or node.findtext("content") or "")
        text = " ".join([title, description])
        if not title:
            continue
        items.append(
            ParserIngestItem(
                source_channel=source.source_channel,
                source_external_id=_extract_external_id(link, f"{title}:{link}"),
                raw_url=link,
                title=title[:255],
                description=description[:4000],
                normalized_address=_extract_first(ADDRESS_RE, text),
                city=source.city,
                region_code=source.region_code,
                area_sqm=_extract_area(text),
                price_rub=_extract_price(text),
                contact_name=source.name,
                contact_phone=_extract_first(PHONE_RE, text),
                contact_email=_extract_first(EMAIL_RE, text),
                intent=_detect_intent(text),
                payload={"source_name": source.name, "source_url": source.source_url, "parser": "rss"},
            )
        )
    return items


def _collect_json_api_items(source: ParserSource) -> list[ParserIngestItem]:
    extra = source.extra_config or {}
    headers = extra.get("headers") if isinstance(extra.get("headers"), dict) else {}
    params = extra.get("params") if isinstance(extra.get("params"), dict) else {}
    response = requests.get(
        source.source_url,
        headers={"User-Agent": USER_AGENT, **headers},
        params=params,
        timeout=settings.parser_request_timeout_sec,
    )
    response.raise_for_status()
    payload = response.json()
    items_key = str(extra.get("items_key") or "items")
    if isinstance(payload, list):
        records = payload
    elif isinstance(payload, dict) and isinstance(payload.get(items_key), list):
        records = payload[items_key]
    else:
        raise ValueError("JSON API response has unsupported structure.")

    max_items = min(source.max_items_per_run, settings.parser_max_items_per_source, len(records))
    parsed: list[ParserIngestItem] = []
    for record in records[:max_items]:
        if not isinstance(record, dict):
            continue
        title = _normalize_text(str(record.get("title") or record.get("name") or ""))
        raw_url = _normalize_text(str(record.get("url") or record.get("link") or source.source_url))
        description = _normalize_text(str(record.get("description") or record.get("text") or ""))
        text = " ".join([title, description])
        if not title:
            continue
        explicit_price = _to_float(record.get("price"))
        explicit_area = _to_float(record.get("area") or record.get("area_sqm"))
        parsed.append(
            ParserIngestItem(
                source_channel=source.source_channel,
                source_external_id=str(record.get("id") or record.get("external_id") or _extract_external_id(raw_url, title)),
                raw_url=raw_url,
                title=title[:255],
                description=description[:4000],
                normalized_address=_normalize_text(str(record.get("address") or "")) or _extract_first(ADDRESS_RE, text),
                city=_normalize_text(str(record.get("city") or source.city or "")) or source.city,
                region_code=_normalize_text(str(record.get("region_code") or source.region_code or "")) or source.region_code,
                area_sqm=explicit_area if explicit_area is not None else _extract_area(text),
                price_rub=explicit_price if explicit_price is not None else _extract_price(text),
                contact_name=_normalize_text(str(record.get("contact_name") or source.name)),
                contact_phone=_normalize_text(str(record.get("phone") or record.get("contact_phone") or ""))
                or _extract_first(PHONE_RE, text),
                contact_email=_normalize_text(str(record.get("email") or record.get("contact_email") or ""))
                or _extract_first(EMAIL_RE, text),
                intent=_detect_intent(text),
                payload={"source_name": source.name, "source_url": source.source_url, "parser": "json_api"},
            )
        )
    return parsed


def collect_items_for_source(source: ParserSource) -> list[ParserIngestItem]:
    mode = str((source.extra_config or {}).get("mode") or "html").lower()
    if mode == "rss":
        return _collect_rss_items(source)
    if mode == "json_api":
        return _collect_json_api_items(source)
    if source.source_channel == SourceChannel.telegram:
        return _collect_telegram_items(source)
    if source.source_channel in (SourceChannel.avito, SourceChannel.cian, SourceChannel.domclick):
        return _collect_marketplace_items(source)
    raise ValueError(f"Source channel '{source.source_channel.value}' is not supported for auto parsing.")
