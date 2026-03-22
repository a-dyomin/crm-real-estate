import asyncio
import hashlib
import json
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from typing import Any
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
PHONE_JSON_RE = re.compile(
    r'"(?:phone|phones|telephone|mobile|contactPhone|contact_phone|sellerPhone)"\s*:\s*"([^"]{10,60})"',
    re.IGNORECASE,
)
TEL_RE = re.compile(r"tel:\s*([+\d][\d\s\-()]{9,})", re.IGNORECASE)
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
IMAGE_EXT_RE = re.compile(r"\.(?:jpg|jpeg|png|webp)(?:\?|$)", re.IGNORECASE)
CHARSET_META_RE = re.compile(rb"charset=['\"]?([A-Za-z0-9._-]+)", re.IGNORECASE)
PRICE_RE = re.compile(r"(\d[\d\s]{2,})\s*(?:\u0440\u0443\u0431|\u20bd)", re.IGNORECASE)
AREA_RE = re.compile(r"(\d+(?:[.,]\d+)?)\s*(?:\u043c2|\u043c\u00b2|\u043a\u0432\.?\s*\u043c|sqm|sq\.?\s*m)", re.IGNORECASE)
ADDRESS_RE = re.compile(r"(?:\u0430\u0434\u0440\u0435\u0441|location)\s*[:\-]\s*([^\n\r|;]{8,150})", re.IGNORECASE)
STREET_RE = re.compile(
    r"\b(?:ул\.?|улица|пр-?кт|проспект|пер\.?|переулок|шоссе|бульвар|наб\.?|набережная|проезд|пл\.?|площадь|мкр\.?|микрорайон)\s*[^,;\n]{3,80}",
    re.IGNORECASE,
)
CONTACT_EMAIL_REJECT_PREFIXES = (
    "help",
    "support",
    "info",
    "no-reply",
    "noreply",
    "feedback",
    "admin",
    "press",
    "hr",
    "jobs",
    "career",
    "marketing",
    "ads",
    "advert",
    "sales",
)
CONTACT_SUPPORT_KEYWORDS = (
    "\u043f\u043e\u0434\u0434\u0435\u0440\u0436",
    "\u0441\u043b\u0443\u0436\u0431\u0430 \u043f\u043e\u0434\u0434\u0435\u0440\u0436\u043a\u0438",
    "\u0442\u0435\u0445\u043f\u043e\u0434\u0434\u0435\u0440\u0436",
    "\u043a\u043e\u043d\u0442\u0430\u043a\u0442\u043d\u044b\u0439 \u0446\u0435\u043d\u0442\u0440",
    "\u0433\u043e\u0440\u044f\u0447\u0430\u044f \u043b\u0438\u043d\u0438\u044f",
    "\u043e\u0431\u0440\u0430\u0442\u043d\u0430\u044f \u0441\u0432\u044f\u0437\u044c",
    "\u0440\u0435\u043a\u043b\u0430\u043c",
    "\u0440\u0430\u0437\u043c\u0435\u0449\u0435\u043d",
    "support",
    "help",
    "feedback",
    "helpdesk",
    "customer support",
    "call center",
)
CONTACT_OWNER_KEYWORDS = (
    "\u0441\u043e\u0431\u0441\u0442\u0432\u0435\u043d\u043d\u0438\u043a",
    "\u0432\u043b\u0430\u0434\u0435\u043b",
    "\u043f\u0440\u044f\u043c\u043e\u0439",
    "\u0431\u0435\u0437 \u043a\u043e\u043c\u0438\u0441\u0441\u0438\u0438",
    "\u043f\u0440\u043e\u0434\u0430\u0432\u0435\u0446",
    "\u0430\u0440\u0435\u043d\u0434\u043e\u0434\u0430\u0442\u0435\u043b\u044c",
    "owner",
)
CONTACT_AGENT_KEYWORDS = (
    "\u0430\u0433\u0435\u043d\u0442",
    "\u0440\u0438\u044d\u043b\u0442\u043e\u0440",
    "\u0431\u0440\u043e\u043a\u0435\u0440",
    "\u043c\u0435\u043d\u0435\u0434\u0436\u0435\u0440",
    "agent",
    "realtor",
    "broker",
)
CONTACT_SELLER_HINT_KEYWORDS = CONTACT_OWNER_KEYWORDS + CONTACT_AGENT_KEYWORDS + (
    "\u043a\u043e\u043d\u0442\u0430\u043a\u0442",
    "\u0442\u0435\u043b\u0435\u0444\u043e\u043d",
    "phone",
    "whatsapp",
    "telegram",
)
UDMURTIA_DISTRICTS = (
    "\u0423\u0441\u0442\u0438\u043d\u043e\u0432\u0441\u043a\u0438\u0439",
    "\u041f\u0435\u0440\u0432\u043e\u043c\u0430\u0439\u0441\u043a\u0438\u0439",
    "\u0418\u043d\u0434\u0443\u0441\u0442\u0440\u0438\u0430\u043b\u044c\u043d\u044b\u0439",
    "\u041b\u0435\u043d\u0438\u043d\u0441\u043a\u0438\u0439",
    "\u041e\u043a\u0442\u044f\u0431\u0440\u044c\u0441\u043a\u0438\u0439",
    "\u0417\u0430\u0432\u044c\u044f\u043b\u043e\u0432\u0441\u043a\u0438\u0439",
)
DEFAULT_TELEGRAM_COMMERCIAL_KEYWORDS = (
    "\u043a\u043e\u043c\u043c\u0435\u0440\u0447\u0435\u0441\u043a",
    "\u043d\u0435\u0434\u0432\u0438\u0436\u0438\u043c",
    "\u043d\u0435\u0436\u0438\u043b",
    "\u043e\u0444\u0438\u0441",
    "\u0441\u043a\u043b\u0430\u0434",
    "\u0442\u043e\u0440\u0433\u043e\u0432",
    "\u0430\u0440\u0435\u043d\u0434\u0430",
    "\u043f\u0440\u043e\u0434\u0430\u0436\u0430",
    "business center",
    "warehouse",
    "retail",
    "office",
)
DEFAULT_TELEGRAM_TRANSACTION_KEYWORDS = (
    "\u0430\u0440\u0435\u043d\u0434",
    "\u043f\u0440\u043e\u0434\u0430\u0436",
    "\u043f\u0440\u043e\u0434\u0430\u043c",
    "\u0441\u0434\u0430\u043c",
    "\u0441\u043d\u0438\u043c\u0443",
    "\u043a\u0443\u043f\u043b\u044e",
    "\u0438\u043d\u0432\u0435\u0441\u0442",
    "\u0438\u043f\u043e\u0442\u0435\u043a",
    "lease",
    "rent",
    "sale",
)
LISTING_RENT_KEYWORDS = (
    "\u0430\u0440\u0435\u043d\u0434",
    "\u0441\u0434\u0430\u043c",
    "\u0441\u0434\u0430\u0435\u0442\u0441\u044f",
    "\u0441\u0434\u0430\u0435\u043c",
    "\u0441\u043d\u0438\u043c\u0443",
    "\u0430\u0440\u0435\u043d\u0434\u0443\u044e",
    "rent",
    "lease",
    "sublease",
)
LISTING_SALE_KEYWORDS = (
    "\u043f\u0440\u043e\u0434\u0430\u0436",
    "\u043f\u0440\u043e\u0434\u0430\u0435\u0442",
    "\u043f\u0440\u043e\u0434\u0430\u043c",
    "\u043a\u0443\u043f\u043b\u044e",
    "\u043a\u0443\u043f\u0438\u0442\u044c",
    "sale",
    "sell",
)
DEFAULT_TELEGRAM_REAL_ESTATE_KEYWORDS = (
    "\u043d\u0435\u0434\u0432\u0438\u0436\u0438\u043c",
    "\u043e\u0444\u0438\u0441",
    "\u043f\u043e\u043c\u0435\u0449\u0435\u043d",
    "\u0442\u043e\u0440\u0433\u043e\u0432",
    "\u043d\u0435\u0436\u0438\u043b",
    "\u0431\u0438\u0437\u043d\u0435\u0441-\u0446\u0435\u043d\u0442\u0440",
    "\u0431\u0438\u0437\u043d\u0435\u0441 \u0446\u0435\u043d\u0442\u0440",
    "\u0437\u0435\u043c\u0435\u043b\u044c\u043d",
    "\u043f\u043b\u043e\u0449\u0430\u0434",
    "\u043a\u0432.\u043c",
    "\u043a\u0432 \u043c",
    "\u043c2",
    "\u043c\u00b2",
)
DEFAULT_TELEGRAM_EXCLUDE_KEYWORDS = (
    "\u043a\u0432\u0430\u0440\u0442\u0438\u0440",
    "\u0436\u0438\u043b\u043e\u0439",
    "\u0436\u0438\u043b\u044c\u0435",
    "\u043d\u043e\u0432\u043e\u0441\u0442\u0440\u043e\u0439",
    "\u0438\u043f\u043e\u0442\u0435\u043a",
    "\u043a\u0440\u0438\u043f\u0442",
    "\u043c\u0430\u0439\u043d\u0438\u043d\u0433",
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
DEFAULT_TELEGRAM_SEARCH_QUERIES = (
    "#\u043a\u043e\u043c\u043c\u0435\u0440\u0447\u0435\u0441\u043a\u0430\u044f\u043d\u0435\u0434\u0432\u0438\u0436\u0438\u043c\u043e\u0441\u0442\u044c",
    "#\u043d\u0435\u0434\u0432\u0438\u0436\u0438\u043c\u043e\u0441\u0442\u044c\u0438\u0436\u0435\u0432\u0441\u043a",
    "\u043a\u043e\u043c\u043c\u0435\u0440\u0447\u0435\u0441\u043a\u0430\u044f \u043d\u0435\u0434\u0432\u0438\u0436\u0438\u043c\u043e\u0441\u0442\u044c \u0443\u0434\u043c\u0443\u0440\u0442\u0438\u044f",
    "\u0430\u0440\u0435\u043d\u0434\u0430 \u043e\u0444\u0438\u0441 \u0438\u0436\u0435\u0432\u0441\u043a",
    "\u0441\u043a\u043b\u0430\u0434 \u0438\u0436\u0435\u0432\u0441\u043a",
    "\u043f\u0440\u043e\u0434\u0430\u0436\u0430 \u043a\u043e\u043c\u043c\u0435\u0440\u0447\u0435\u0441\u043a\u043e\u0439 \u043d\u0435\u0434\u0432\u0438\u0436\u0438\u043c\u043e\u0441\u0442\u0438",
)
TELEGRAM_BG_IMAGE_RE = re.compile(r"url\(['\"]?([^'\")]+)['\"]?\)")
DEFAULT_AVITO_ITEM_STATUSES = ("active",)
AVITO_ALLOWED_ITEM_STATUSES = {"active", "removed", "old", "blocked", "rejected"}
_AVITO_TOKEN_CACHE: dict[str, Any] = {"client_id": "", "access_token": "", "expires_at": None}


def _normalize_text(value: str | None) -> str:
    return " ".join((value or "").split()).strip()


def _normalize_contact_phone(value: str | None) -> str:
    if not value:
        return ""
    digits = re.sub(r"\D+", "", value)
    if digits.startswith("8") and len(digits) == 11:
        digits = "7" + digits[1:]
    if digits.startswith("7") and len(digits) == 11:
        return f"+{digits}"
    return value.strip()


def _normalize_contact_email(value: str | None) -> str:
    return (value or "").strip().lower()


def _source_domain(source_url: str) -> str:
    host = urlparse(source_url or "").netloc.lower()
    return host.replace("www.", "").strip()


def _has_any_keyword(text: str, keywords: tuple[str, ...]) -> bool:
    return any(keyword in text for keyword in keywords)


def _element_location(element: Any) -> str:
    if not element:
        return "text"
    for parent in getattr(element, "parents", []):
        tag = getattr(parent, "name", "") or ""
        if tag.lower() == "footer":
            return "footer"
        if tag.lower() == "header":
            return "header"
    classes = " ".join(getattr(element, "get", lambda _k, _d=None: _d)("class", []) or [])
    element_id = getattr(element, "get", lambda _k, _d=None: _d)("id", "") or ""
    meta = f"{classes} {element_id}".lower()
    if "footer" in meta or "подвал" in meta:
        return "footer"
    if "header" in meta or "шапк" in meta:
        return "header"
    if any(token in meta for token in ("seller", "owner", "agent", "contact", "phone", "realtor", "broker")):
        return "seller_block"
    return "listing_body"


def _add_contact_candidate(
    candidates: list[dict[str, Any]],
    seen: set[tuple[str, str]],
    *,
    value: str,
    candidate_type: str,
    source_location: str,
    context: str | None = None,
    origin: str | None = None,
) -> None:
    if not value:
        return
    normalized = _normalize_contact_phone(value) if candidate_type == "phone" else _normalize_contact_email(value)
    if not normalized:
        return
    key = (candidate_type, normalized)
    if key in seen:
        return
    seen.add(key)
    candidates.append(
        {
            "value": value.strip(),
            "normalized": normalized,
            "type": candidate_type,
            "source_location": source_location,
            "context": _normalize_text(context)[:240] if context else None,
            "origin": origin,
        }
    )


def _extract_contact_candidates_from_payload(payload: Any, candidates: list[dict[str, Any]], seen: set[tuple[str, str]]) -> None:
    if payload is None:
        return
    if isinstance(payload, dict):
        for key, value in payload.items():
            key_lower = str(key).lower()
            if isinstance(value, str):
                if "mail" in key_lower or "email" in key_lower:
                    for email in EMAIL_RE.findall(value):
                        _add_contact_candidate(
                            candidates,
                            seen,
                            value=email,
                            candidate_type="email",
                            source_location="json_payload",
                            context=key_lower,
                            origin="json_payload",
                        )
                if "phone" in key_lower or "tel" in key_lower or "contact" in key_lower:
                    for phone in PHONE_RE.findall(value):
                        _add_contact_candidate(
                            candidates,
                            seen,
                            value=phone,
                            candidate_type="phone",
                            source_location="json_payload",
                            context=key_lower,
                            origin="json_payload",
                        )
            _extract_contact_candidates_from_payload(value, candidates, seen)
    elif isinstance(payload, list):
        for item in payload:
            _extract_contact_candidates_from_payload(item, candidates, seen)
    elif isinstance(payload, str):
        for phone in PHONE_RE.findall(payload):
            _add_contact_candidate(
                candidates,
                seen,
                value=phone,
                candidate_type="phone",
                source_location="json_payload",
                context="payload_text",
                origin="json_payload",
            )
        for email in EMAIL_RE.findall(payload):
            _add_contact_candidate(
                candidates,
                seen,
                value=email,
                candidate_type="email",
                source_location="json_payload",
                context="payload_text",
                origin="json_payload",
            )


def _extract_contact_candidates_from_text(
    text: str,
    candidates: list[dict[str, Any]],
    seen: set[tuple[str, str]],
    *,
    source_location: str,
    context: str | None = None,
    origin: str | None = None,
) -> None:
    for phone in PHONE_RE.findall(text or ""):
        _add_contact_candidate(
            candidates,
            seen,
            value=phone,
            candidate_type="phone",
            source_location=source_location,
            context=context or text,
            origin=origin,
        )
    for email in EMAIL_RE.findall(text or ""):
        _add_contact_candidate(
            candidates,
            seen,
            value=email,
            candidate_type="email",
            source_location=source_location,
            context=context or text,
            origin=origin,
        )


def _extract_contact_candidates_from_soup(
    soup: BeautifulSoup,
    candidates: list[dict[str, Any]],
    seen: set[tuple[str, str]],
) -> None:
    for link in soup.select("a[href^='tel:']"):
        raw = link.get("href") or ""
        match = TEL_RE.search(raw)
        phone = match.group(1) if match else _extract_first(PHONE_RE, raw)
        context = link.get_text(" ", strip=True) or (link.parent.get_text(" ", strip=True) if link.parent else "")
        _add_contact_candidate(
            candidates,
            seen,
            value=phone or "",
            candidate_type="phone",
            source_location=_element_location(link),
            context=context,
            origin="tel_link",
        )
    for link in soup.select("a[href^='mailto:']"):
        raw = link.get("href") or ""
        email = raw.split("mailto:", 1)[-1].split("?", 1)[0]
        context = link.get_text(" ", strip=True) or (link.parent.get_text(" ", strip=True) if link.parent else "")
        _add_contact_candidate(
            candidates,
            seen,
            value=email,
            candidate_type="email",
            source_location=_element_location(link),
            context=context,
            origin="mailto_link",
        )
    for attr in ("data-phone", "data-tel", "data-contact-phone", "data-qa-phone", "data-phone-number"):
        for element in soup.select(f"[{attr}]"):
            raw = element.get(attr) or ""
            phone = _extract_first(PHONE_RE, raw)
            context = element.get_text(" ", strip=True)
            _add_contact_candidate(
                candidates,
                seen,
                value=phone or raw,
                candidate_type="phone",
                source_location=_element_location(element),
                context=context,
                origin=f"attr:{attr}",
            )

    seller_selectors = (
        ".seller",
        ".agent",
        ".owner",
        ".realtor",
        ".broker",
        ".contact",
        ".contacts",
        ".phone",
        "[class*='seller']",
        "[class*='agent']",
        "[class*='owner']",
        "[class*='contact']",
        "[class*='phone']",
        "[id*='seller']",
        "[id*='agent']",
        "[id*='owner']",
        "[id*='contact']",
    )
    for element in soup.select(",".join(seller_selectors)):
        text = element.get_text(" ", strip=True)
        _extract_contact_candidates_from_text(
            text,
            candidates,
            seen,
            source_location=_element_location(element),
            context=text,
            origin="seller_block",
        )


def _classify_contact_candidate(candidate: dict[str, Any], source_domain: str) -> dict[str, Any]:
    value = str(candidate.get("value") or "")
    context = str(candidate.get("context") or "").lower()
    location = str(candidate.get("source_location") or "")
    candidate_type = str(candidate.get("type") or "")
    reasons: list[str] = []

    has_seller_hint = _has_any_keyword(context, CONTACT_SELLER_HINT_KEYWORDS)
    has_owner_hint = _has_any_keyword(context, CONTACT_OWNER_KEYWORDS)
    has_agent_hint = _has_any_keyword(context, CONTACT_AGENT_KEYWORDS)

    if candidate_type == "email":
        normalized = _normalize_contact_email(value)
        local_part, _, domain = normalized.partition("@")
        if local_part and any(local_part.startswith(prefix) for prefix in CONTACT_EMAIL_REJECT_PREFIXES):
            reasons.append("email_prefix_blocked")
        if domain and source_domain and domain.endswith(source_domain) and not has_seller_hint:
            reasons.append("platform_domain")
    if _has_any_keyword(context, CONTACT_SUPPORT_KEYWORDS):
        reasons.append("support_context")
    if location in ("header", "footer") and not has_seller_hint:
        reasons.append("header_footer")
    if "\u0440\u0435\u043a\u043b\u0430\u043c" in context or "advert" in context or "ads" in context:
        reasons.append("ad_contact")

    rejected = bool(reasons)
    contact_class = "unknown_contact"
    if rejected:
        if "support" in " ".join(reasons) or "email_prefix_blocked" in reasons:
            contact_class = "support_contact"
        else:
            contact_class = "platform_contact"
    else:
        if has_owner_hint:
            contact_class = "owner_candidate"
        elif has_agent_hint:
            contact_class = "agent_candidate"
        elif location in ("seller_block", "listing_meta", "detail_block"):
            contact_class = "organization_contact"

    base_confidence = {
        "owner_candidate": 0.9,
        "agent_candidate": 0.75,
        "organization_contact": 0.6,
        "unknown_contact": 0.35,
        "platform_contact": 0.1,
        "support_contact": 0.1,
    }.get(contact_class, 0.3)
    if candidate_type == "phone":
        base_confidence += 0.05
    if location in ("seller_block", "listing_meta", "detail_block"):
        base_confidence += 0.05
    if location in ("header", "footer"):
        base_confidence -= 0.2
    confidence = max(0.05, min(1.0, base_confidence))

    candidate["class"] = contact_class
    candidate["confidence"] = round(confidence, 3)
    candidate["rejected"] = rejected
    candidate["rejection_reasons"] = reasons
    return candidate


def _select_best_contact(candidates: list[dict[str, Any]]) -> dict[str, Any] | None:
    valid = [candidate for candidate in candidates if not candidate.get("rejected")]
    if not valid:
        for candidate in candidates:
            candidate["is_selected_for_lead"] = False
        return None
    valid.sort(
        key=lambda candidate: (
            float(candidate.get("confidence") or 0),
            1 if candidate.get("type") == "phone" else 0,
        ),
        reverse=True,
    )
    selected = valid[0]
    for candidate in candidates:
        candidate["is_selected_for_lead"] = candidate is selected
    return selected


def _contact_label(contact_class: str | None) -> str | None:
    mapping = {
        "owner_candidate": "\u0421\u043e\u0431\u0441\u0442\u0432\u0435\u043d\u043d\u0438\u043a",
        "agent_candidate": "\u0410\u0433\u0435\u043d\u0442",
        "organization_contact": "\u041e\u0440\u0433\u0430\u043d\u0438\u0437\u0430\u0446\u0438\u044f",
        "unknown_contact": "\u041a\u043e\u043d\u0442\u0430\u043a\u0442",
    }
    return mapping.get(contact_class or "")


def _build_contact_pipeline(
    *,
    source: ParserSource,
    raw_text: str,
    soup: BeautifulSoup | None,
    json_payloads: list[Any] | None = None,
    context_text: str | None = None,
    seed_contacts: list[tuple[str, str]] | None = None,
) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    if seed_contacts:
        for candidate_type, value in seed_contacts:
            _add_contact_candidate(
                candidates,
                seen,
                value=value,
                candidate_type=candidate_type,
                source_location="seed",
                context=context_text,
                origin="seed",
            )
    for payload in json_payloads or []:
        _extract_contact_candidates_from_payload(payload, candidates, seen)
    if soup is not None:
        _extract_contact_candidates_from_soup(soup, candidates, seen)
    if context_text:
        _extract_contact_candidates_from_text(
            context_text,
            candidates,
            seen,
            source_location="listing_body",
            context=context_text,
            origin="listing_body",
        )
    if raw_text:
        _extract_contact_candidates_from_text(
            raw_text[:4000],
            candidates,
            seen,
            source_location="page_text",
            context=context_text,
            origin="page_text",
        )

    domain = _source_domain(source.source_url)
    for candidate in candidates:
        _classify_contact_candidate(candidate, domain)

    selected = _select_best_contact(candidates)
    rejected = [candidate for candidate in candidates if candidate.get("rejected")]
    rejection_reasons: list[str] = []
    for candidate in rejected:
        rejection_reasons.extend(candidate.get("rejection_reasons") or [])
    rejection_reasons = sorted(set(rejection_reasons))
    if selected:
        selected["label"] = _contact_label(str(selected.get("class")))

    return {
        "contact_candidates": candidates,
        "selected_contact": selected,
        "rejected_contacts": rejected,
        "contact_rejection_reasons": rejection_reasons,
        "contact_confidence": float(selected.get("confidence")) if selected else None,
    }


def _looks_like_html(value: str) -> bool:
    probe = value[:1000].lower()
    return "<html" in probe or "<body" in probe or "<!doctype html" in probe


def _looks_like_mojibake(value: str) -> bool:
    sample = value[:2000]
    return "\u00d0" in sample or "\u00d1" in sample


def _decode_response_text(response: requests.Response) -> str:
    content = response.content or b""
    if not content:
        return response.text or ""
    encoding_candidates: list[str] = []
    meta_match = CHARSET_META_RE.search(content[:4096])
    if meta_match:
        try:
            encoding_candidates.append(meta_match.group(1).decode("ascii", errors="ignore"))
        except Exception:
            pass
    if response.encoding:
        encoding_candidates.append(response.encoding)
    if response.apparent_encoding:
        encoding_candidates.append(response.apparent_encoding)
    encoding_candidates.extend(["utf-8", "cp1251"])
    tried: set[str] = set()
    for encoding in encoding_candidates:
        normalized = str(encoding or "").strip().lower()
        if not normalized or normalized in tried:
            continue
        tried.add(normalized)
        try:
            text = content.decode(normalized)
        except Exception:
            continue
        if _looks_like_mojibake(text):
            try:
                repaired = text.encode("latin1", errors="ignore").decode("utf-8", errors="ignore")
            except Exception:
                repaired = ""
            if repaired and not _looks_like_mojibake(repaired):
                return repaired
            continue
        return text
    return content.decode("utf-8", errors="replace")


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
        text = _decode_response_text(response)
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
        text = _decode_response_text(response)
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


def _extract_phone_from_payload(payload: Any, *, depth: int = 0) -> str | None:
    if payload is None or depth > 4:
        return None
    if isinstance(payload, str):
        return _extract_first(PHONE_RE, payload)
    if isinstance(payload, dict):
        for key in (
            "phone",
            "phones",
            "telephone",
            "mobile",
            "contactPhone",
            "contact_phone",
            "sellerPhone",
            "calltracking",
            "calltrackingNumber",
            "formattedPhone",
            "tel",
        ):
            if key in payload:
                phone = _extract_phone_from_payload(payload.get(key), depth=depth + 1)
                if phone:
                    return phone
        for value in payload.values():
            phone = _extract_phone_from_payload(value, depth=depth + 1)
            if phone:
                return phone
    if isinstance(payload, list):
        for item in payload[:50]:
            phone = _extract_phone_from_payload(item, depth=depth + 1)
            if phone:
                return phone
    return None


def _extract_image_from_payload(payload: Any, *, base_url: str = "", depth: int = 0) -> str | None:
    if payload is None or depth > 4:
        return None
    if isinstance(payload, str):
        if IMAGE_EXT_RE.search(payload):
            return urljoin(base_url, payload)
        return None
    if isinstance(payload, dict):
        for key in (
            "image",
            "image_url",
            "imageUrl",
            "photo",
            "photo_url",
            "photoUrl",
            "preview",
            "thumbnail",
            "cover",
            "gallery",
            "images",
            "photos",
        ):
            if key in payload:
                image = _extract_image_from_payload(payload.get(key), base_url=base_url, depth=depth + 1)
                if image:
                    return image
        for value in payload.values():
            image = _extract_image_from_payload(value, base_url=base_url, depth=depth + 1)
            if image:
                return image
    if isinstance(payload, list):
        for item in payload[:50]:
            image = _extract_image_from_payload(item, base_url=base_url, depth=depth + 1)
            if image:
                return image
    return None


def _extract_phone_from_html(raw_text: str, soup: BeautifulSoup) -> str | None:
    for anchor in soup.select('a[href^="tel:"]'):
        href = (anchor.get("href") or "").strip()
        match = TEL_RE.search(href)
        if match:
            return _normalize_text(match.group(1))

    for attr in ("data-phone", "data-tel", "data-contact-phone", "data-qa-phone"):
        for node in soup.select(f'[{attr}]'):
            value = (node.get(attr) or "").strip()
            if value:
                match = PHONE_RE.search(value)
                if match:
                    return _normalize_text(match.group(0))

    json_match = PHONE_JSON_RE.search(raw_text)
    if json_match:
        phone_candidate = json_match.group(1)
        match = PHONE_RE.search(phone_candidate)
        if match:
            return _normalize_text(match.group(0))

    for script in soup.select('script[type="application/ld+json"], script[type="application/json"]'):
        raw = script.string or script.get_text(" ", strip=True)
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        phone = _extract_phone_from_payload(data)
        if phone:
            return phone

    return _extract_first(PHONE_RE, raw_text)


def _extract_image_url(soup: BeautifulSoup, base_url: str) -> str | None:
    meta = soup.select_one('meta[property="og:image"], meta[name="og:image"], meta[property="twitter:image"], meta[name="twitter:image"]')
    if meta:
        content = (meta.get("content") or "").strip()
        if content:
            return urljoin(base_url, content)

    for img in soup.select("img[src]"):
        src = (img.get("src") or "").strip()
        if not src or src.startswith("data:"):
            continue
        resolved = urljoin(base_url, src)
        if resolved:
            return resolved
    return None


def _extract_image_from_soup(soup: BeautifulSoup, base_url: str) -> str | None:
    image = _extract_image_url(soup, base_url)
    if image:
        return image
    for script in soup.select('script[type="application/ld+json"], script[type="application/json"]'):
        raw = script.string or script.get_text(" ", strip=True)
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        image = _extract_image_from_payload(data, base_url=base_url)
        if image:
            return image
    return None


def _is_address_candidate(value: str) -> bool:
    if not value:
        return False
    trimmed = _normalize_text(value)
    if not trimmed:
        return False
    lowered = trimmed.lower()
    if lowered.startswith("http"):
        return False
    if len(trimmed) < 6:
        return False
    if re.search(r"\d", trimmed):
        return True
    return any(
        token in lowered
        for token in (
            "ул",
            "улиц",
            "просп",
            "пр-кт",
            "шоссе",
            "район",
            "street",
            "st.",
            "ave",
        )
    )


def _extract_address_from_payload(payload: Any, *, depth: int = 0) -> str | None:
    if payload is None or depth > 4:
        return None
    if isinstance(payload, str):
        return _normalize_text(payload) if _is_address_candidate(payload) else None
    if isinstance(payload, dict):
        address_value = payload.get("address")
        if address_value:
            address_candidate = _extract_address_from_payload(address_value, depth=depth + 1)
            if address_candidate:
                return address_candidate
        parts: list[str] = []
        for key in ("addressRegion", "addressLocality", "streetAddress", "addressLine"):
            value = payload.get(key)
            if isinstance(value, str) and value.strip():
                parts.append(_normalize_text(value))
        if parts:
            unique_parts = [part for idx, part in enumerate(parts) if part and part not in parts[:idx]]
            return ", ".join(unique_parts)
        for key, value in payload.items():
            key_lower = str(key).lower()
            if key_lower.startswith("@"):
                continue
            if not any(token in key_lower for token in ("address", "location", "geo", "place", "region", "locality", "street")):
                continue
            address_candidate = _extract_address_from_payload(value, depth=depth + 1)
            if address_candidate:
                return address_candidate
    if isinstance(payload, list):
        for item in payload[:50]:
            address_candidate = _extract_address_from_payload(item, depth=depth + 1)
            if address_candidate:
                return address_candidate
    return None


def _extract_address_from_soup(soup: BeautifulSoup) -> str | None:
    street_node = soup.select_one('[itemprop="streetAddress"]')
    locality_node = soup.select_one('[itemprop="addressLocality"]')
    region_node = soup.select_one('[itemprop="addressRegion"]')
    parts = [
        _normalize_text(node.get_text(" ", strip=True)) if node else ""
        for node in (region_node, locality_node, street_node)
    ]
    parts = [part for part in parts if part]
    if parts:
        return ", ".join(parts)
    return None


def _passes_region_filter(source: ParserSource, text: str, normalized_address: str | None) -> bool:
    if not source.city and not source.region_code:
        return True
    if normalized_address:
        address_lower = normalized_address.lower()
        if source.city:
            city = source.city.lower()
            if city and city in address_lower:
                return True
        if source.region_code == "RU-UDM":
            if any(keyword.lower() in address_lower for keyword in DEFAULT_TELEGRAM_UDMURTIA_KEYWORDS):
                return True
            return False
    combined = _normalize_text(text).lower()
    if source.city:
        city = source.city.lower()
        if city and city in combined:
            return True
    if source.region_code == "RU-UDM":
        if any(keyword.lower() in combined for keyword in DEFAULT_TELEGRAM_UDMURTIA_KEYWORDS):
            return True
        return False
    return True


def _detect_listing_type(text: str, url: str = "") -> str | None:
    url_normalized = _normalize_text(url).lower()
    if any(token in url_normalized for token in ("snyat", "rent", "arenda", "lease", "/arenda", "/rent")):
        return "rent"
    if any(token in url_normalized for token in ("kupit", "sale", "sell", "prodazha", "/sale")):
        return "sale"
    normalized = _normalize_text(text).lower()
    if any(keyword in normalized for keyword in LISTING_RENT_KEYWORDS):
        return "rent"
    if any(keyword in normalized for keyword in LISTING_SALE_KEYWORDS):
        return "sale"
    return None


def _extract_district(text: str | None) -> str | None:
    if not text:
        return None
    normalized = text.lower()
    for district in UDMURTIA_DISTRICTS:
        if district.lower() in normalized:
            return district
    match = re.search(r"([А-ЯЁа-яё-]{4,})\s+район", text)
    if match:
        return _normalize_text(match.group(1))
    return None


def _extract_street(text: str | None) -> str | None:
    if not text:
        return None
    match = STREET_RE.search(text)
    if match:
        return _normalize_text(match.group(0))
    return None


def _extract_address_parts(text: str, normalized_address: str | None) -> tuple[str | None, str | None]:
    district = _extract_district(text)
    street = _extract_street(normalized_address or text)
    if not street and normalized_address:
        street = _normalize_text(normalized_address)
    return district, street


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
    if source_channel == SourceChannel.yandex:
        if "realty.yandex.ru" not in host:
            return False
        return bool(re.search(r"/(?:offer|commercial)/\d+/?$", path))
    if source_channel == SourceChannel.bankrupt:
        return True
    return False


def _canonical_listing_url(url: str) -> str:
    parsed = urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"


def _bankrupt_link_keywords(source: ParserSource) -> tuple[str, ...]:
    extra = source.extra_config or {}
    raw_keywords = extra.get("link_keywords")
    if isinstance(raw_keywords, list):
        cleaned = [str(item).strip().lower() for item in raw_keywords if str(item).strip()]
        if cleaned:
            return tuple(cleaned)
    return ("bankrot", "банкрот", "торги", "torgi", "auction")


def _extract_listing_links(source: ParserSource, base_url: str, source_text: str) -> list[str]:
    source_channel = source.source_channel
    link_keywords = _bankrupt_link_keywords(source) if source_channel == SourceChannel.bankrupt else ()
    links: set[str] = set()
    soup = BeautifulSoup(source_text, "html.parser")
    for anchor in soup.select("a[href]"):
        href = (anchor.get("href") or "").strip()
        if not href:
            continue
        resolved = urljoin(base_url, href)
        if source_channel == SourceChannel.bankrupt and link_keywords:
            anchor_text = _normalize_text(anchor.get_text(" ", strip=True)).lower()
            resolved_lower = resolved.lower()
            if not any(keyword in resolved_lower or keyword in anchor_text for keyword in link_keywords):
                continue
            links.add(_canonical_listing_url(resolved))
            continue
        if _is_listing_url(source_channel, resolved):
            links.add(_canonical_listing_url(resolved))

    if links:
        return sorted(links)

    for raw_url in URL_RE.findall(source_text):
        normalized = _normalize_url_candidate(raw_url)
        resolved = urljoin(base_url, normalized)
        if source_channel == SourceChannel.bankrupt and link_keywords:
            resolved_lower = resolved.lower()
            if any(keyword in resolved_lower for keyword in link_keywords):
                links.add(_canonical_listing_url(resolved))
            continue
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


def _extract_json_payloads(soup: BeautifulSoup) -> list[Any]:
    payloads: list[Any] = []
    for script in soup.select('script[type="application/ld+json"], script[type="application/json"]'):
        raw = script.string or script.get_text(" ", strip=True)
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        if isinstance(data, list):
            payloads.extend(data)
        else:
            payloads.append(data)
    return payloads


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


def _build_item_from_detail(source: ParserSource, listing_url: str, raw_text: str) -> ParserIngestItem | None:
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
        json_ld_text = _json_ld_text(soup)
        text = _normalize_text(" ".join([page_title, description, body_text[:8000], json_ld_text]))
        json_payloads = _extract_json_payloads(soup)
        address_from_payload = None
        for payload in json_payloads:
            address_from_payload = _extract_address_from_payload(payload)
            if address_from_payload:
                break
        address_from_soup = _extract_address_from_soup(soup)
        image_url = _extract_image_from_soup(soup, listing_url)
        listing_hint = _normalize_text(" ".join([page_title, description]))
    else:
        page_title = _extract_markdown_title(raw_text) or f"{source.source_channel.value} listing"
        text = _strip_markdown(raw_text)[:12000]
        image_url = None
        listing_hint = text
        address_from_payload = None
        address_from_soup = None

    address_match = ADDRESS_RE.search(text)
    address_raw = address_from_payload or address_from_soup or (address_match.group(1) if address_match else "")
    address = _normalize_text(address_raw) if address_raw else None
    if not _passes_region_filter(source, text, address):
        return None
    address_district, address_street = _extract_address_parts(text, address)
    listing_type = _detect_listing_type(listing_hint, listing_url)

    contact_info = _build_contact_pipeline(
        source=source,
        raw_text=raw_text,
        soup=soup if _looks_like_html(raw_text) else None,
        json_payloads=json_payloads if _looks_like_html(raw_text) else None,
        context_text=listing_hint or text,
    )
    selected_contact = contact_info.get("selected_contact") if contact_info else None
    contact_phone = None
    contact_email = None
    contact_name = None
    if isinstance(selected_contact, dict):
        if selected_contact.get("type") == "phone":
            contact_phone = selected_contact.get("normalized") or selected_contact.get("value")
        if selected_contact.get("type") == "email":
            contact_email = selected_contact.get("normalized") or selected_contact.get("value")
        contact_name = selected_contact.get("label")

    return ParserIngestItem(
        source_channel=source.source_channel,
        source_external_id=_extract_external_id(listing_url, listing_url),
        raw_url=listing_url,
        title=page_title[:255] if page_title else f"{source.source_channel.value} listing",
        description=text[:4000],
        listing_type=listing_type,
        image_url=image_url,
        normalized_address=address,
        address_district=address_district,
        address_street=address_street,
        city=source.city,
        region_code=source.region_code,
        area_sqm=_extract_area(text),
        price_rub=_extract_price(text),
        contact_name=contact_name or None,
        contact_phone=contact_phone,
        contact_email=contact_email,
        contact_candidates=contact_info.get("contact_candidates") if contact_info else None,
        selected_contact=contact_info.get("selected_contact") if contact_info else None,
        rejected_contacts=contact_info.get("rejected_contacts") if contact_info else None,
        contact_rejection_reasons=contact_info.get("contact_rejection_reasons") if contact_info else None,
        contact_confidence=contact_info.get("contact_confidence") if contact_info else None,
        intent=_detect_intent(text),
        payload={"source_name": source.name, "source_url": source.source_url, "parser": "html_scraper"},
    )


def _is_listing_record(record: dict[str, Any]) -> bool:
    keys = {str(key).lower() for key in record.keys()}
    score = 0
    if keys & {"title", "name", "heading"}:
        score += 1
    if keys & {"price", "cost", "price_value", "amount"}:
        score += 1
    if keys & {"address", "location", "addressraw", "streetaddress"}:
        score += 1
    if keys & {"area", "area_sqm", "square", "sqm", "floorarea"}:
        score += 1
    if keys & {"url", "link", "href", "detail_url"}:
        score += 1
    return score >= 2


def _extract_listing_records(payloads: list[Any]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    seen: set[str] = set()

    def walk(node: Any) -> None:
        if isinstance(node, dict):
            if _is_listing_record(node):
                record_id = str(node.get("id") or node.get("external_id") or node.get("url") or node.get("link") or "")
                if record_id and record_id in seen:
                    return
                if record_id:
                    seen.add(record_id)
                records.append(node)
                return
            for value in node.values():
                walk(value)
        elif isinstance(node, list):
            for item in node:
                walk(item)

    for payload in payloads:
        walk(payload)
    return records


def _build_item_from_record(source: ParserSource, record: dict[str, Any], base_url: str) -> ParserIngestItem | None:
    title = _normalize_text(str(record.get("title") or record.get("name") or record.get("heading") or ""))
    raw_url = _normalize_text(str(record.get("url") or record.get("link") or record.get("href") or base_url))
    description = _normalize_text(str(record.get("description") or record.get("text") or record.get("summary") or ""))
    text = " ".join([title, description])
    if not title:
        return None
    normalized_address = _normalize_text(str(record.get("address") or record.get("location") or "")) or _extract_first(
        ADDRESS_RE, text
    )
    if not _passes_region_filter(source, text, normalized_address):
        return None
    address_district, address_street = _extract_address_parts(text, normalized_address)
    listing_type = _detect_listing_type(text, raw_url)
    explicit_price = _to_float(record.get("price") or record.get("cost") or record.get("amount"))
    explicit_area = _to_float(record.get("area") or record.get("area_sqm") or record.get("sqm"))
    seed_contacts: list[tuple[str, str]] = []
    raw_phone = _normalize_text(str(record.get("phone") or record.get("contact_phone") or ""))
    if raw_phone:
        seed_contacts.append(("phone", raw_phone))
    raw_email = _normalize_text(str(record.get("email") or record.get("contact_email") or ""))
    if raw_email:
        seed_contacts.append(("email", raw_email))
    contact_info = _build_contact_pipeline(
        source=source,
        raw_text=text,
        soup=None,
        json_payloads=[record],
        context_text=text,
        seed_contacts=seed_contacts or None,
    )
    selected = contact_info.get("selected_contact") if contact_info else None
    contact_phone = None
    contact_email = None
    contact_name = None
    if isinstance(selected, dict):
        if selected.get("type") == "phone":
            contact_phone = selected.get("normalized") or selected.get("value")
        if selected.get("type") == "email":
            contact_email = selected.get("normalized") or selected.get("value")
        contact_name = selected.get("label")

    return ParserIngestItem(
        source_channel=source.source_channel,
        source_external_id=str(record.get("id") or record.get("external_id") or _extract_external_id(raw_url, title)),
        raw_url=raw_url,
        title=title[:255],
        description=description[:4000],
        listing_type=listing_type,
        image_url=_extract_image_from_payload(record, base_url=raw_url),
        normalized_address=normalized_address,
        address_district=address_district,
        address_street=address_street,
        city=_normalize_text(str(record.get("city") or source.city or "")) or source.city,
        region_code=_normalize_text(str(record.get("region_code") or source.region_code or "")) or source.region_code,
        area_sqm=explicit_area if explicit_area is not None else _extract_area(text),
        price_rub=explicit_price if explicit_price is not None else _extract_price(text),
        contact_name=contact_name or None,
        contact_phone=contact_phone,
        contact_email=contact_email,
        contact_candidates=contact_info.get("contact_candidates") if contact_info else None,
        selected_contact=contact_info.get("selected_contact") if contact_info else None,
        rejected_contacts=contact_info.get("rejected_contacts") if contact_info else None,
        contact_rejection_reasons=contact_info.get("contact_rejection_reasons") if contact_info else None,
        contact_confidence=contact_info.get("contact_confidence") if contact_info else None,
        intent=_detect_intent(text),
        payload={"source_name": source.name, "source_url": source.source_url, "parser": "embedded_json"},
    )


def _extract_listing_links_with_config(
    soup: BeautifulSoup,
    base_url: str,
    list_config: dict[str, Any],
) -> list[str]:
    item_selector = str(list_config.get("item_selector") or "")
    link_selector = str(list_config.get("link_selector") or "a")
    link_attr = str(list_config.get("link_attr") or "href")
    items = soup.select(item_selector) if item_selector else []
    candidates = items if items else soup.select(link_selector)
    links: set[str] = set()
    for item in candidates:
        targets = item.select(link_selector) if item is not None else []
        if not targets and item_selector:
            targets = [item]
        for target in targets:
            href = target.get(link_attr) or target.get("href") or ""
            if not href:
                continue
            resolved = urljoin(base_url, href)
            if resolved:
                links.add(_canonical_listing_url(resolved))
    return sorted(links)


def _collect_items_with_auto_config(source: ParserSource, auto_config: dict[str, Any]) -> list[ParserIngestItem]:
    start_urls = auto_config.get("start_urls") or [source.source_url]
    list_config = auto_config.get("list") if isinstance(auto_config.get("list"), dict) else {}
    detail_limit = settings.parser_detail_fetch_limit
    links: list[str] = []
    for start_url in start_urls:
        try:
            html = _fetch_text(start_url)
        except Exception:
            continue
        soup = BeautifulSoup(html, "html.parser")
        links.extend(_extract_listing_links_with_config(soup, start_url, list_config))
        if len(links) >= source.max_items_per_run:
            break
    if not links:
        return []
    max_items = min(source.max_items_per_run, settings.parser_max_items_per_source, len(links))
    detail_limit = min(max_items, detail_limit)
    items: list[ParserIngestItem] = []
    for link in links[:detail_limit]:
        try:
            detail_text = _fetch_text(link)
            item = _build_item_from_detail(source, link, detail_text)
            if item:
                items.append(item)
        except Exception:
            continue
    return items


def _collect_marketplace_items(source: ParserSource) -> list[ParserIngestItem]:
    auto_config = (source.extra_config or {}).get("auto_config")
    if isinstance(auto_config, dict):
        configured_items = _collect_items_with_auto_config(source, auto_config)
        if configured_items:
            return configured_items
    source_text = _fetch_text(source.source_url)
    links = _extract_listing_links(source, source.source_url, source_text)
    if not links:
        soup = BeautifulSoup(source_text, "html.parser")
        payloads = _extract_json_payloads(soup)
        records = _extract_listing_records(payloads)
        if records:
            max_items = min(source.max_items_per_run, settings.parser_max_items_per_source, len(records))
            items: list[ParserIngestItem] = []
            for record in records[:max_items]:
                if not isinstance(record, dict):
                    continue
                item = _build_item_from_record(source, record, source.source_url)
                if item:
                    items.append(item)
            if items:
                return items
        raise ValueError("No listing links were extracted from source page.")

    max_items = min(source.max_items_per_run, settings.parser_max_items_per_source, len(links))
    detail_limit = min(max_items, settings.parser_detail_fetch_limit)
    items: list[ParserIngestItem] = []
    parse_errors: list[str] = []
    for link in links[:detail_limit]:
        try:
            detail_text = _fetch_text(link)
            item = _build_item_from_detail(source, link, detail_text)
            if item:
                items.append(item)
        except Exception as exc:
            parse_errors.append(f"{link}: {type(exc).__name__}")

    if not items:
        summary = "; ".join(parse_errors[:3])
        if summary:
            raise ValueError(f"No listing details parsed from source links. {summary}")
        raise ValueError("No listing details parsed from source links.")
    return items


def _collect_avito_official_items(source: ParserSource) -> list[ParserIngestItem]:
    avito_config = _avito_api_config(source)
    client_id, client_secret, user_id = _require_avito_api_credentials(avito_config)
    token = _get_avito_access_token(client_id, client_secret)
    max_items = min(source.max_items_per_run, settings.parser_max_items_per_source)

    items: list[ParserIngestItem] = []
    details_fetched = 0
    page = 1
    while len(items) < max_items and page <= int(avito_config["max_pages"]):
        per_page = min(int(avito_config["per_page"]), max_items - len(items), 100)
        params: dict[str, Any] = {"per_page": per_page, "page": page}
        status_values: tuple[str, ...] = avito_config["status"]
        if status_values:
            params["status"] = ",".join(status_values)
        updated_at_from = _normalize_text(str(avito_config["updated_at_from"] or ""))
        if updated_at_from:
            params["updatedAtFrom"] = updated_at_from
        category = int(avito_config["category"] or 0)
        if category > 0:
            params["category"] = category

        response = _avito_api_request("GET", "/core/v1/items", token, params=params)
        resources = response.get("resources")
        if not isinstance(resources, list) or not resources:
            break

        for raw_item in resources:
            if not isinstance(raw_item, dict):
                continue
            raw_item_id = raw_item.get("id")
            external_id = _normalize_text(str(raw_item_id or ""))
            title = _normalize_text(str(raw_item.get("title") or ""))
            if not title:
                title = f"Avito item {external_id}" if external_id else "Avito item"
            address = _normalize_text(str(raw_item.get("address") or "")) or None
            raw_url = _normalize_text(str(raw_item.get("url") or "")) or source.source_url
            status = _normalize_text(str(raw_item.get("status") or ""))
            category_payload = raw_item.get("category") if isinstance(raw_item.get("category"), dict) else {}
            category_name = _normalize_text(str(category_payload.get("name") or ""))
            price_rub = _to_float(raw_item.get("price"))
            item_payload: dict[str, Any] = {
                "source_name": source.name,
                "source_url": source.source_url,
                "parser": "avito_official_api",
                "status": status,
                "category": category_payload,
                "list_page": page,
            }
            if (
                bool(avito_config.get("with_item_details", True))
                and user_id
                and external_id
                and details_fetched < int(avito_config["details_limit"])
            ):
                try:
                    item_payload["item_detail"] = _avito_api_request(
                        "GET",
                        f"/core/v1/accounts/{user_id}/items/{external_id}/",
                        token,
                    )
                    details_fetched += 1
                except Exception:
                    # Soft-fail on detail enrichment: keep base listing data.
                    pass

            analysis_text = _normalize_text(" ".join([title, address or "", category_name, status]))
            address_district, address_street = _extract_address_parts(analysis_text, address)
            listing_type = _detect_listing_type(analysis_text, raw_url)
            contact_phone_seed = _extract_phone_from_payload(item_payload)
            seed_contacts: list[tuple[str, str]] = []
            if contact_phone_seed:
                seed_contacts.append(("phone", contact_phone_seed))
            contact_info = _build_contact_pipeline(
                source=source,
                raw_text=analysis_text,
                soup=None,
                json_payloads=[item_payload],
                context_text=analysis_text,
                seed_contacts=seed_contacts or None,
            )
            selected = contact_info.get("selected_contact") if contact_info else None
            contact_phone = None
            contact_email = None
            contact_name = None
            if isinstance(selected, dict):
                if selected.get("type") == "phone":
                    contact_phone = selected.get("normalized") or selected.get("value")
                if selected.get("type") == "email":
                    contact_email = selected.get("normalized") or selected.get("value")
                contact_name = selected.get("label")
            image_url = _extract_image_from_payload(item_payload, base_url=raw_url)
            items.append(
                ParserIngestItem(
                    source_channel=SourceChannel.avito,
                    source_external_id=external_id or _extract_external_id(raw_url, title),
                    raw_url=raw_url,
                    title=title[:255],
                    description=analysis_text[:4000] or title[:255],
                    listing_type=listing_type,
                    image_url=image_url,
                    normalized_address=address,
                    address_district=address_district,
                    address_street=address_street,
                    city=source.city,
                    region_code=source.region_code,
                    area_sqm=None,
                    price_rub=price_rub,
                    contact_name=contact_name or None,
                    contact_phone=contact_phone,
                    contact_email=contact_email,
                    contact_candidates=contact_info.get("contact_candidates") if contact_info else None,
                    selected_contact=contact_info.get("selected_contact") if contact_info else None,
                    rejected_contacts=contact_info.get("rejected_contacts") if contact_info else None,
                    contact_rejection_reasons=contact_info.get("contact_rejection_reasons") if contact_info else None,
                    contact_confidence=contact_info.get("contact_confidence") if contact_info else None,
                    intent=_detect_intent(analysis_text),
                    payload=item_payload,
                )
            )
            if len(items) >= max_items:
                break

        if len(resources) < per_page:
            break
        page += 1

    if not items:
        raise ValueError(
            "Avito official API returned no items. Check AVITO credentials, account rights, and avito_api filters."
        )
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


def _telegram_search_queries(raw_value: object) -> tuple[str, ...]:
    if isinstance(raw_value, str):
        parts = [segment.strip() for segment in raw_value.replace("\n", ",").split(",")]
    elif isinstance(raw_value, list):
        parts = [str(segment).strip() for segment in raw_value]
    else:
        parts = []

    unique: list[str] = []
    seen: set[str] = set()
    for part in parts:
        if not part:
            continue
        normalized = part.lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        unique.append(part)
        if len(unique) >= 20:
            break
    if unique:
        return tuple(unique)
    return DEFAULT_TELEGRAM_SEARCH_QUERIES


def _normalize_telegram_channel(value: object) -> str:
    candidate = str(value or "").strip().lstrip("@")
    if not candidate:
        return ""
    return candidate.split("/", maxsplit=1)[0].lower()


def _telegram_channel_list(raw_value: object) -> tuple[str, ...]:
    if isinstance(raw_value, str):
        parts = [segment.strip() for segment in raw_value.replace("\n", ",").split(",")]
    elif isinstance(raw_value, list):
        parts = [str(segment).strip() for segment in raw_value]
    else:
        parts = []
    unique: list[str] = []
    seen: set[str] = set()
    for part in parts:
        normalized = _normalize_telegram_channel(part)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        unique.append(normalized)
    return tuple(unique)


def _safe_int(value: object, default: int, minimum: int, maximum: int) -> int:
    try:
        number = int(value)  # type: ignore[arg-type]
    except Exception:
        number = default
    return max(minimum, min(maximum, number))


def _avito_status_list(raw_value: object) -> tuple[str, ...]:
    if isinstance(raw_value, str):
        parts = [segment.strip().lower() for segment in raw_value.replace("\n", ",").split(",")]
    elif isinstance(raw_value, list):
        parts = [str(segment).strip().lower() for segment in raw_value]
    else:
        parts = []

    unique: list[str] = []
    seen: set[str] = set()
    for part in parts:
        if not part or part in seen or part not in AVITO_ALLOWED_ITEM_STATUSES:
            continue
        seen.add(part)
        unique.append(part)
    return tuple(unique) if unique else DEFAULT_AVITO_ITEM_STATUSES


def _avito_api_config(source: ParserSource) -> dict[str, Any]:
    extra_config = source.extra_config if isinstance(source.extra_config, dict) else {}
    raw_avito = extra_config.get("avito_api")
    avito = raw_avito if isinstance(raw_avito, dict) else {}
    return {
        "client_id": _normalize_text(str(avito.get("client_id") or settings.avito_client_id)),
        "client_secret": _normalize_text(str(avito.get("client_secret") or settings.avito_client_secret)),
        "user_id": _normalize_text(str(avito.get("user_id") or settings.avito_user_id)),
        "per_page": _safe_int(avito.get("per_page"), default=100, minimum=1, maximum=100),
        "max_pages": _safe_int(avito.get("max_pages"), default=200, minimum=1, maximum=2000),
        "status": _avito_status_list(avito.get("status")),
        "updated_at_from": _normalize_text(str(avito.get("updated_at_from") or "")),
        "category": _safe_int(avito.get("category"), default=0, minimum=0, maximum=10_000_000),
        "with_item_details": bool(avito.get("with_item_details", True)),
        "details_limit": _safe_int(avito.get("details_limit"), default=300, minimum=0, maximum=10000),
    }


def _require_avito_api_credentials(avito_config: dict[str, Any]) -> tuple[str, str, str]:
    client_id = _normalize_text(str(avito_config.get("client_id") or ""))
    client_secret = _normalize_text(str(avito_config.get("client_secret") or ""))
    user_id = _normalize_text(str(avito_config.get("user_id") or ""))
    if not client_id or not client_secret:
        raise ValueError(
            "Avito API credentials are missing. Set AVITO_CLIENT_ID and AVITO_CLIENT_SECRET in .env "
            "or source.extra_config.avito_api."
        )
    return client_id, client_secret, user_id


def _get_avito_access_token(client_id: str, client_secret: str) -> str:
    now = datetime.now(timezone.utc)
    cache_client_id = str(_AVITO_TOKEN_CACHE.get("client_id") or "")
    cache_token = str(_AVITO_TOKEN_CACHE.get("access_token") or "")
    cache_expires_at = _AVITO_TOKEN_CACHE.get("expires_at")
    if (
        cache_token
        and cache_client_id == client_id
        and isinstance(cache_expires_at, datetime)
        and cache_expires_at > now
    ):
        return cache_token

    response = requests.post(
        settings.avito_token_url,
        data={
            "grant_type": "client_credentials",
            "client_id": client_id,
            "client_secret": client_secret,
        },
        headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
        timeout=settings.avito_request_timeout_sec,
    )
    try:
        response.raise_for_status()
    except Exception as exc:
        snippet = _normalize_text(response.text[:300]) if response.text else ""
        raise ValueError(f"Avito OAuth token request failed ({response.status_code}): {snippet}") from exc

    payload = response.json() if response.text else {}
    token = _normalize_text(str(payload.get("access_token") or ""))
    if not token:
        raise ValueError("Avito OAuth token response does not contain access_token.")
    expires_in = _safe_int(payload.get("expires_in"), default=3600, minimum=60, maximum=86_400)
    _AVITO_TOKEN_CACHE["client_id"] = client_id
    _AVITO_TOKEN_CACHE["access_token"] = token
    _AVITO_TOKEN_CACHE["expires_at"] = now + timedelta(seconds=max(30, expires_in - 30))
    return token


def _avito_api_request(
    method: str,
    path: str,
    token: str,
    *,
    params: dict[str, Any] | None = None,
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_path = path if path.startswith("/") else f"/{path}"
    base_url = settings.avito_api_base_url.rstrip("/")
    response = requests.request(
        method=method.upper(),
        url=f"{base_url}{normalized_path}",
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
        },
        params=params,
        json=payload,
        timeout=settings.avito_request_timeout_sec,
    )
    if response.status_code >= 400:
        snippet = _normalize_text(response.text[:300]) if response.text else ""
        raise ValueError(f"Avito API request failed ({response.status_code}) {normalized_path}: {snippet}")
    if not response.text:
        return {}
    try:
        parsed = response.json()
    except Exception as exc:
        raise ValueError(f"Avito API returned non-JSON response for {normalized_path}.") from exc
    if isinstance(parsed, dict):
        return parsed
    raise ValueError(f"Avito API returned unexpected payload type for {normalized_path}.")


def _telegram_search_config(source: ParserSource) -> dict[str, Any]:
    extra_config = source.extra_config if isinstance(source.extra_config, dict) else {}
    raw_search = extra_config.get("telegram_search")
    search = raw_search if isinstance(raw_search, dict) else {}
    return {
        "queries": _telegram_search_queries(search.get("queries")),
        "discover_channels": bool(search.get("discover_channels", True)),
        "channels_limit": _safe_int(
            search.get("channels_limit"),
            settings.telegram_channel_discovery_limit,
            minimum=1,
            maximum=10000,
        ),
        "posts_limit_per_query": _safe_int(
            search.get("posts_limit_per_query"),
            settings.telegram_search_limit_per_query,
            minimum=1,
            maximum=10000,
        ),
        "days_back": _safe_int(
            search.get("days_back"),
            settings.telegram_search_days_back,
            minimum=1,
            maximum=365,
        ),
        "whitelist_enabled": bool(search.get("whitelist_enabled", False)),
        "allowed_channels": _telegram_channel_list(search.get("allowed_channels")),
    }


def _passes_telegram_filters(text: str, source: ParserSource, region_context: str | None = None) -> bool:
    normalized = text.lower()
    region_normalized = (region_context or text).lower()
    extra_config = source.extra_config if isinstance(source.extra_config, dict) else {}
    raw_filters = extra_config.get("telegram_filters")
    filters = raw_filters if isinstance(raw_filters, dict) else {}
    commercial_only = bool(filters.get("commercial_only", True))
    udmurtia_only = bool(filters.get("udmurtia_only", True))
    require_transaction_keyword = bool(filters.get("require_transaction_keyword", True))
    require_real_estate_keyword = bool(filters.get("require_real_estate_keyword", True))
    commercial_keywords = _telegram_filter_keywords(
        filters.get("commercial_keywords"), DEFAULT_TELEGRAM_COMMERCIAL_KEYWORDS
    )
    transaction_keywords = _telegram_filter_keywords(
        filters.get("transaction_keywords"), DEFAULT_TELEGRAM_TRANSACTION_KEYWORDS
    )
    real_estate_keywords = _telegram_filter_keywords(
        filters.get("real_estate_keywords"), DEFAULT_TELEGRAM_REAL_ESTATE_KEYWORDS
    )
    exclude_keywords = _telegram_filter_keywords(
        filters.get("exclude_keywords"), DEFAULT_TELEGRAM_EXCLUDE_KEYWORDS
    )
    udmurtia_keywords = _telegram_filter_keywords(filters.get("region_keywords"), DEFAULT_TELEGRAM_UDMURTIA_KEYWORDS)

    if any(keyword in normalized for keyword in exclude_keywords):
        return False
    if commercial_only and not any(keyword in normalized for keyword in commercial_keywords):
        return False
    if commercial_only and require_real_estate_keyword and not any(keyword in normalized for keyword in real_estate_keywords):
        return False
    if commercial_only and require_transaction_keyword and not any(keyword in normalized for keyword in transaction_keywords):
        return False
    if udmurtia_only and not any(keyword in region_normalized for keyword in udmurtia_keywords):
        return False
    return True


def _is_relevant_telegram_channel(username: str, title: str, query: str, source: ParserSource) -> bool:
    composite = _normalize_text(" ".join([username, title]))
    region_context = _normalize_text(" ".join([composite, query]))
    normalized = composite.lower()
    region_normalized = region_context.lower()
    extra_config = source.extra_config if isinstance(source.extra_config, dict) else {}
    raw_filters = extra_config.get("telegram_filters")
    filters = raw_filters if isinstance(raw_filters, dict) else {}
    commercial_keywords = _telegram_filter_keywords(
        filters.get("commercial_keywords"), DEFAULT_TELEGRAM_COMMERCIAL_KEYWORDS
    )
    require_real_estate_keyword = bool(filters.get("require_real_estate_keyword", True))
    real_estate_keywords = _telegram_filter_keywords(
        filters.get("real_estate_keywords"), DEFAULT_TELEGRAM_REAL_ESTATE_KEYWORDS
    )
    exclude_keywords = _telegram_filter_keywords(
        filters.get("exclude_keywords"), DEFAULT_TELEGRAM_EXCLUDE_KEYWORDS
    )
    udmurtia_only = bool(filters.get("udmurtia_only", True))
    udmurtia_keywords = _telegram_filter_keywords(filters.get("region_keywords"), DEFAULT_TELEGRAM_UDMURTIA_KEYWORDS)

    if any(keyword in normalized for keyword in exclude_keywords):
        return False
    if not any(keyword in normalized for keyword in commercial_keywords):
        return False
    if require_real_estate_keyword and not any(keyword in normalized for keyword in real_estate_keywords):
        return False
    if udmurtia_only and not any(keyword in region_normalized for keyword in udmurtia_keywords):
        return False
    return True


def _build_telegram_item(
    source: ParserSource,
    *,
    text: str,
    message_url: str,
    source_external_id: str,
    channel_name: str,
    image_url: str | None,
    payload: dict[str, Any],
) -> ParserIngestItem:
    address_district, address_street = _extract_address_parts(text, _extract_first(ADDRESS_RE, text))
    listing_type = _detect_listing_type(text, message_url)
    contact_info = _build_contact_pipeline(
        source=source,
        raw_text=text,
        soup=None,
        json_payloads=None,
        context_text=text,
    )
    selected = contact_info.get("selected_contact") if contact_info else None
    contact_phone = None
    contact_email = None
    contact_name = None
    if isinstance(selected, dict):
        if selected.get("type") == "phone":
            contact_phone = selected.get("normalized") or selected.get("value")
        if selected.get("type") == "email":
            contact_email = selected.get("normalized") or selected.get("value")
        contact_name = selected.get("label")
    return ParserIngestItem(
        source_channel=SourceChannel.telegram,
        source_external_id=source_external_id,
        raw_url=message_url,
        telegram_post_url=message_url,
        title=text[:120],
        description=text[:4000],
        listing_type=listing_type,
        image_url=image_url,
        normalized_address=_extract_first(ADDRESS_RE, text),
        address_district=address_district,
        address_street=address_street,
        city=source.city,
        region_code=source.region_code,
        area_sqm=_extract_area(text),
        price_rub=_extract_price(text),
        contact_name=contact_name or None,
        contact_phone=contact_phone,
        contact_email=contact_email,
        contact_candidates=contact_info.get("contact_candidates") if contact_info else None,
        selected_contact=contact_info.get("selected_contact") if contact_info else None,
        rejected_contacts=contact_info.get("rejected_contacts") if contact_info else None,
        contact_rejection_reasons=contact_info.get("contact_rejection_reasons") if contact_info else None,
        contact_confidence=contact_info.get("contact_confidence") if contact_info else None,
        intent=_detect_intent(text),
        payload=payload,
    )


def _extract_telegram_image_url(message_node: BeautifulSoup, fallback_base: str) -> str | None:
    photo_node = message_node.select_one(".tgme_widget_message_photo_wrap, .tgme_widget_message_photo")
    if photo_node is None:
        return None
    style = (photo_node.get("style") or "").strip()
    if style:
        match = TELEGRAM_BG_IMAGE_RE.search(style)
        if match:
            return urljoin(fallback_base, match.group(1))
    data_src = (photo_node.get("data-src") or "").strip()
    if data_src:
        return urljoin(fallback_base, data_src)
    img = photo_node.select_one("img[src]")
    if img is not None:
        src = (img.get("src") or "").strip()
        if src:
            return urljoin(fallback_base, src)
    return None


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
        image_url = _extract_telegram_image_url(message, source_url)
        if not text:
            continue
        if not _passes_telegram_filters(text, source):
            continue
        items.append(
            _build_telegram_item(
                source,
                text=text,
                message_url=message_url,
                source_external_id=external_id or _extract_external_id(message_url, text),
                channel_name=source.name,
                image_url=image_url,
                payload={"source_name": source.name, "source_url": source_url, "parser": "telegram_channel"},
            )
        )
    return items


async def _telegram_message_permalink(message: Any) -> tuple[str | None, str]:
    chat = getattr(message, "chat", None)
    if chat is None:
        try:
            chat = await message.get_chat()
        except Exception:
            chat = None
    username = getattr(chat, "username", None) if chat else None
    if not username:
        return None, ""
    message_id = getattr(message, "id", None)
    if not message_id:
        return None, ""
    return f"https://t.me/{username}/{message_id}", f"{username}/{message_id}"


def _require_telegram_api_credentials() -> tuple[int, str, str]:
    api_id_raw = settings.telegram_api_id.strip()
    api_hash = settings.telegram_api_hash.strip()
    session_string = settings.telegram_session_string.strip()
    if not api_id_raw or not api_hash:
        raise ValueError(
            "Telegram API credentials are missing. Set TELEGRAM_API_ID and TELEGRAM_API_HASH in .env."
        )
    if not session_string:
        raise ValueError(
            "Telegram session is missing. Set TELEGRAM_SESSION_STRING (StringSession from authorized account)."
        )
    try:
        api_id = int(api_id_raw)
    except ValueError as exc:
        raise ValueError("TELEGRAM_API_ID must be an integer.") from exc
    return api_id, api_hash, session_string


def _persist_telegram_channel_catalog(
    source: ParserSource,
    discovered_channels: dict[str, dict[str, Any]],
    search_config: dict[str, Any],
) -> None:
    extra_config = dict(source.extra_config or {})
    raw_search = extra_config.get("telegram_search")
    search = dict(raw_search) if isinstance(raw_search, dict) else {}

    allowed_channels = {channel for channel in search_config.get("allowed_channels", ())}
    existing_rows = search.get("discovered_channels")
    existing: dict[str, dict[str, Any]] = {}
    if isinstance(existing_rows, list):
        for row in existing_rows:
            if not isinstance(row, dict):
                continue
            username = _normalize_telegram_channel(row.get("username"))
            if not username:
                continue
            title = _normalize_text(str(row.get("title") or f"@{username}")) or f"@{username}"
            matched_queries = row.get("matched_queries") if isinstance(row.get("matched_queries"), list) else []
            if username not in allowed_channels:
                candidate_queries = [
                    _normalize_text(str(item))
                    for item in matched_queries
                    if _normalize_text(str(item))
                ] or [""]
                if not any(_is_relevant_telegram_channel(username, title, query, source) for query in candidate_queries):
                    continue
            existing[username] = {
                "username": username,
                "title": title,
                "first_seen_at": row.get("first_seen_at"),
                "last_seen_at": row.get("last_seen_at"),
                "matched_queries": matched_queries,
            }

    now_iso = datetime.now(timezone.utc).isoformat()
    for username, payload in discovered_channels.items():
        title = _normalize_text(str(payload.get("title") or f"@{username}")) or f"@{username}"
        queries = payload.get("queries") if isinstance(payload.get("queries"), set) else set()
        row = existing.get(username) or {
            "username": username,
            "title": title,
            "first_seen_at": now_iso,
            "last_seen_at": now_iso,
            "matched_queries": [],
        }
        row["title"] = title
        row["last_seen_at"] = now_iso
        historical_queries = {
            _normalize_text(str(item))
            for item in (row.get("matched_queries") if isinstance(row.get("matched_queries"), list) else [])
            if _normalize_text(str(item))
        }
        historical_queries.update({_normalize_text(str(item)) for item in queries if _normalize_text(str(item))})
        row["matched_queries"] = sorted(historical_queries)[:25]
        existing[username] = row

    search["discovered_channels"] = [existing[key] for key in sorted(existing.keys())][:500]
    search["whitelist_enabled"] = bool(search_config.get("whitelist_enabled", False))
    search["allowed_channels"] = list(_telegram_channel_list(search.get("allowed_channels")))
    extra_config["telegram_search"] = search
    source.extra_config = extra_config


async def _collect_telegram_api_search_items_async(source: ParserSource) -> list[ParserIngestItem]:
    api_id, api_hash, session_string = _require_telegram_api_credentials()
    try:
        from telethon import TelegramClient  # type: ignore[import-not-found]
        from telethon.sessions import StringSession  # type: ignore[import-not-found]
        from telethon.tl.functions.contacts import SearchRequest  # type: ignore[import-not-found]
    except Exception as exc:
        raise ValueError(
            "Telethon dependency is not installed. Reinstall dependencies: pip install -e .[dev]"
        ) from exc

    search_config = _telegram_search_config(source)
    query_list: tuple[str, ...] = search_config["queries"]
    max_items = min(source.max_items_per_run, settings.parser_max_items_per_source)
    min_message_date = datetime.now(timezone.utc) - timedelta(days=int(search_config["days_back"]))

    discovered_channels: dict[str, dict[str, Any]] = {}
    items: list[ParserIngestItem] = []
    seen_message_ids: set[str] = set()
    whitelist_enabled = bool(search_config.get("whitelist_enabled", False))
    allowed_channels = {channel for channel in search_config.get("allowed_channels", ())}

    client = TelegramClient(
        session=StringSession(session_string),
        api_id=api_id,
        api_hash=api_hash,
        receive_updates=False,
    )
    await client.connect()
    try:
        if not await client.is_user_authorized():
            raise ValueError(
                "Telegram session is not authorized. Recreate TELEGRAM_SESSION_STRING from a logged-in account."
            )

        def remember_channel(username_raw: object, title_raw: object, query: str | None = None) -> str:
            username = _normalize_telegram_channel(username_raw)
            if not username:
                return ""
            title = _normalize_text(str(title_raw or f"@{username}")) or f"@{username}"
            payload = discovered_channels.get(username)
            if not isinstance(payload, dict):
                payload = {"title": title, "queries": set()}
            payload["title"] = title
            queries = payload.get("queries")
            if not isinstance(queries, set):
                queries = set()
            if query:
                queries.add(_normalize_text(query))
            payload["queries"] = queries
            discovered_channels[username] = payload
            return username

        async def consume_message(message: Any, query: str, channel_hint: str | None = None) -> None:
            if len(items) >= max_items:
                return
            text = _normalize_text(getattr(message, "message", "") or "")
            if not text:
                return
            message_date = getattr(message, "date", None)
            if message_date:
                if message_date.tzinfo is None:
                    message_date = message_date.replace(tzinfo=timezone.utc)
                if message_date < min_message_date:
                    return

            message_url, external_id = await _telegram_message_permalink(message)
            if not message_url or not external_id:
                return
            if external_id in seen_message_ids:
                return

            username = remember_channel(external_id.split("/", maxsplit=1)[0], channel_hint, query)
            if not username:
                return
            if whitelist_enabled and username not in allowed_channels:
                return
            payload = discovered_channels.get(username) or {}
            channel_title = _normalize_text(str(payload.get("title") or channel_hint or f"@{username}")) or f"@{username}"
            commercial_context = _normalize_text(" ".join([text, username, channel_title]))
            region_context = _normalize_text(" ".join([commercial_context, query]))
            if not _passes_telegram_filters(commercial_context, source, region_context=region_context):
                return

            seen_message_ids.add(external_id)
            items.append(
                _build_telegram_item(
                    source,
                    text=text,
                    message_url=message_url,
                    source_external_id=external_id,
                    channel_name=f"@{username}",
                    image_url=None,
                    payload={
                        "source_name": source.name,
                        "source_url": source.source_url,
                        "parser": "telegram_api_search",
                        "query": query,
                        "channel": username,
                        "channel_title": channel_title,
                    },
                )
            )

        if bool(search_config["discover_channels"]):
            for query in query_list:
                search_result = await client(SearchRequest(q=query, limit=int(search_config["channels_limit"])))
                for chat in getattr(search_result, "chats", []) or []:
                    username = getattr(chat, "username", None)
                    if not username:
                        continue
                    if not bool(getattr(chat, "broadcast", False) or getattr(chat, "megagroup", False)):
                        continue
                    title = _normalize_text(str(getattr(chat, "title", "") or f"@{username}"))
                    if not _is_relevant_telegram_channel(str(username), title, query, source):
                        continue
                    remember_channel(username, title, query)

        for query in query_list:
            async for message in client.iter_messages(
                entity=None,
                search=query,
                limit=int(search_config["posts_limit_per_query"]),
            ):
                if len(items) >= max_items:
                    break
                await consume_message(message, query)
            if len(items) >= max_items:
                break

        # Secondary pass: scan discovered channels directly by query, then recent history.
        per_channel_limit = int(search_config["posts_limit_per_query"])
        for username, channel_payload in discovered_channels.items():
            if len(items) >= max_items:
                break
            channel_title = _normalize_text(str(channel_payload.get("title") or f"@{username}")) or f"@{username}"
            if whitelist_enabled and username not in allowed_channels:
                continue
            channel_had_hits = False
            for query in query_list:
                try:
                    async for message in client.iter_messages(
                        entity=username,
                        search=query,
                        limit=per_channel_limit,
                    ):
                        before = len(items)
                        await consume_message(message, query, channel_hint=channel_title)
                        if len(items) > before:
                            channel_had_hits = True
                        if len(items) >= max_items:
                            break
                except Exception:
                    continue
                if len(items) >= max_items:
                    break

            if len(items) >= max_items:
                break
            if not channel_had_hits:
                continue
        _persist_telegram_channel_catalog(source, discovered_channels, search_config)
        return items
    finally:
        await client.disconnect()


def _collect_telegram_api_search_items(source: ParserSource) -> list[ParserIngestItem]:
    return asyncio.run(_collect_telegram_api_search_items_async(source))


def _collect_rss_items(source: ParserSource) -> list[ParserIngestItem]:
    xml_text = _fetch_text(source.source_url)
    root = ET.fromstring(xml_text)
    max_items = min(source.max_items_per_run, settings.parser_max_items_per_source)
    items: list[ParserIngestItem] = []
    for node in root.findall(".//item")[:max_items]:
        title = _normalize_text(node.findtext("title") or "")
        link = _normalize_text(node.findtext("link") or source.source_url)
        raw_description = node.findtext("description") or node.findtext("content") or ""
        description = _normalize_text(raw_description)
        text = " ".join([title, description])
        if not title:
            continue
        image_url: str | None = None
        enclosure = node.find("enclosure")
        if enclosure is not None:
            image_url = _normalize_text(enclosure.get("url") or "") or None
        if not image_url:
            for media_tag in (
                "{http://search.yahoo.com/mrss/}content",
                "{http://search.yahoo.com/mrss/}thumbnail",
            ):
                media_node = node.find(media_tag)
                if media_node is not None:
                    image_url = _normalize_text(media_node.get("url") or "") or None
                    if image_url:
                        break
        if not image_url and raw_description and "<" in raw_description:
            try:
                soup = BeautifulSoup(raw_description, "html.parser")
                image_url = _extract_image_url(soup, link)
                if soup.get_text(" ", strip=True):
                    description = _normalize_text(soup.get_text(" ", strip=True))
                    text = " ".join([title, description])
            except Exception:
                pass
        normalized_address = _extract_first(ADDRESS_RE, text)
        address_district, address_street = _extract_address_parts(text, normalized_address)
        listing_type = _detect_listing_type(text, link)
        contact_info = _build_contact_pipeline(
            source=source,
            raw_text=description or text,
            soup=None,
            json_payloads=None,
            context_text=text,
        )
        selected = contact_info.get("selected_contact") if contact_info else None
        contact_phone = None
        contact_email = None
        contact_name = None
        if isinstance(selected, dict):
            if selected.get("type") == "phone":
                contact_phone = selected.get("normalized") or selected.get("value")
            if selected.get("type") == "email":
                contact_email = selected.get("normalized") or selected.get("value")
            contact_name = selected.get("label")
        items.append(
            ParserIngestItem(
                source_channel=source.source_channel,
                source_external_id=_extract_external_id(link, f"{title}:{link}"),
                raw_url=link,
                title=title[:255],
                description=description[:4000],
                listing_type=listing_type,
                image_url=image_url,
                normalized_address=normalized_address,
                address_district=address_district,
                address_street=address_street,
                city=source.city,
                region_code=source.region_code,
                area_sqm=_extract_area(text),
                price_rub=_extract_price(text),
                contact_name=contact_name or None,
                contact_phone=contact_phone,
                contact_email=contact_email,
                contact_candidates=contact_info.get("contact_candidates") if contact_info else None,
                selected_contact=contact_info.get("selected_contact") if contact_info else None,
                rejected_contacts=contact_info.get("rejected_contacts") if contact_info else None,
                contact_rejection_reasons=contact_info.get("contact_rejection_reasons") if contact_info else None,
                contact_confidence=contact_info.get("contact_confidence") if contact_info else None,
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
        normalized_address = _normalize_text(str(record.get("address") or "")) or _extract_first(ADDRESS_RE, text)
        address_district, address_street = _extract_address_parts(text, normalized_address)
        listing_type = _detect_listing_type(text, raw_url)
        seed_contacts: list[tuple[str, str]] = []
        raw_phone = _normalize_text(str(record.get("phone") or record.get("contact_phone") or ""))
        if raw_phone:
            seed_contacts.append(("phone", raw_phone))
        raw_email = _normalize_text(str(record.get("email") or record.get("contact_email") or ""))
        if raw_email:
            seed_contacts.append(("email", raw_email))
        contact_info = _build_contact_pipeline(
            source=source,
            raw_text=text,
            soup=None,
            json_payloads=[record],
            context_text=text,
            seed_contacts=seed_contacts or None,
        )
        selected = contact_info.get("selected_contact") if contact_info else None
        contact_phone = None
        contact_email = None
        contact_name = None
        if isinstance(selected, dict):
            if selected.get("type") == "phone":
                contact_phone = selected.get("normalized") or selected.get("value")
            if selected.get("type") == "email":
                contact_email = selected.get("normalized") or selected.get("value")
            contact_name = selected.get("label")
        parsed.append(
            ParserIngestItem(
                source_channel=source.source_channel,
                source_external_id=str(record.get("id") or record.get("external_id") or _extract_external_id(raw_url, title)),
                raw_url=raw_url,
                title=title[:255],
                description=description[:4000],
                listing_type=listing_type,
                image_url=_extract_image_from_payload(record, base_url=raw_url),
                normalized_address=normalized_address,
                address_district=address_district,
                address_street=address_street,
                city=_normalize_text(str(record.get("city") or source.city or "")) or source.city,
                region_code=_normalize_text(str(record.get("region_code") or source.region_code or "")) or source.region_code,
                area_sqm=explicit_area if explicit_area is not None else _extract_area(text),
                price_rub=explicit_price if explicit_price is not None else _extract_price(text),
                contact_name=contact_name or None,
                contact_phone=contact_phone,
                contact_email=contact_email,
                contact_candidates=contact_info.get("contact_candidates") if contact_info else None,
                selected_contact=contact_info.get("selected_contact") if contact_info else None,
                rejected_contacts=contact_info.get("rejected_contacts") if contact_info else None,
                contact_rejection_reasons=contact_info.get("contact_rejection_reasons") if contact_info else None,
                contact_confidence=contact_info.get("contact_confidence") if contact_info else None,
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
    if source.source_channel == SourceChannel.avito and mode in {"avito_official_api", "avito_api"}:
        return _collect_avito_official_items(source)
    if source.source_channel == SourceChannel.telegram:
        if mode == "telegram_api_search":
            return _collect_telegram_api_search_items(source)
        return _collect_telegram_items(source)
    if source.source_channel in (
        SourceChannel.avito,
        SourceChannel.cian,
        SourceChannel.domclick,
        SourceChannel.yandex,
        SourceChannel.bankrupt,
        SourceChannel.web,
    ):
        return _collect_marketplace_items(source)
    raise ValueError(f"Source channel '{source.source_channel.value}' is not supported for auto parsing.")
