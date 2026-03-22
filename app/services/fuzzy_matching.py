from __future__ import annotations

import re
from difflib import SequenceMatcher
from typing import Iterable

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.models.graph_fuzzy_match import GraphFuzzyMatch
from app.models.graph_node import GraphNode

LEGAL_FORMS = (
    "ооо",
    "ип",
    "ао",
    "пao",
    "пао",
    "зао",
    "оао",
    "гк",
    "тоо",
)


def _normalize_name(value: str) -> str:
    text = value.lower()
    text = re.sub(r"[\"'«»()]+", " ", text)
    for form in LEGAL_FORMS:
        text = re.sub(rf"\b{re.escape(form)}\b", " ", text)
    text = re.sub(r"[^a-zа-я0-9\s]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _similarity(a: str, b: str) -> float:
    return SequenceMatcher(a=a, b=b).ratio()


def _candidate_pairs(items: list[tuple[int | None, str, str]]) -> Iterable[tuple[tuple[int | None, str, str], tuple[int | None, str, str]]]:
    buckets: dict[str, list[tuple[int | None, str, str]]] = {}
    for entry in items:
        normalized = entry[2]
        key = normalized[:3]
        buckets.setdefault(key, []).append(entry)
    for group in buckets.values():
        for i in range(len(group)):
            for j in range(i + 1, len(group)):
                yield group[i], group[j]


def refresh_fuzzy_matches(db: Session, agency_id: int) -> int:
    db.execute(delete(GraphFuzzyMatch).where(GraphFuzzyMatch.agency_id == agency_id))
    db.commit()

    org_nodes = (
        db.execute(
            select(GraphNode).where(GraphNode.agency_id == agency_id, GraphNode.node_type == "organization")
        )
        .scalars()
        .all()
    )
    contact_nodes = (
        db.execute(
            select(GraphNode).where(GraphNode.agency_id == agency_id, GraphNode.node_type == "contact")
        )
        .scalars()
        .all()
    )

    org_items = []
    for node in org_nodes:
        if not node.label:
            continue
        normalized = _normalize_name(node.label)
        if len(normalized) < 4:
            continue
        org_items.append((node.id, node.label, normalized))

    contact_items = []
    for node in contact_nodes:
        label = (node.metadata_json or {}).get("contact_name") or node.label
        if not label:
            continue
        normalized = _normalize_name(label)
        if len(normalized) < 4:
            continue
        contact_items.append((node.id, label, normalized))

    created = 0

    for a, b in _candidate_pairs(org_items):
        if a[0] == b[0]:
            continue
        if a[2] == b[2]:
            similarity = 1.0
        else:
            similarity = _similarity(a[2], b[2])
        if similarity < 0.92:
            continue
        db.add(
            GraphFuzzyMatch(
                agency_id=agency_id,
                match_type="organization",
                from_node_id=a[0],
                to_node_id=b[0],
                raw_value_a=a[1],
                raw_value_b=b[1],
                normalized_value_a=a[2],
                normalized_value_b=b[2],
                similarity_score=similarity,
                accepted_confidence=min(0.9, similarity),
                explanation="Высокое текстовое сходство организаций",
            )
        )
        created += 1

    for contact in contact_items:
        for org in org_items:
            if contact[2][:3] != org[2][:3]:
                continue
            similarity = _similarity(contact[2], org[2])
            if similarity < 0.93:
                continue
            db.add(
                GraphFuzzyMatch(
                    agency_id=agency_id,
                    match_type="contact_organization",
                    from_node_id=contact[0],
                    to_node_id=org[0],
                    raw_value_a=contact[1],
                    raw_value_b=org[1],
                    normalized_value_a=contact[2],
                    normalized_value_b=org[2],
                    similarity_score=similarity,
                    accepted_confidence=min(0.88, similarity),
                    explanation="Контакт похож на организацию по названию",
                )
            )
            created += 1

    db.commit()
    return created
