from __future__ import annotations

import re
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.graph_edge import GraphEdge
from app.models.graph_edge_evidence import GraphEdgeEvidence
from app.models.graph_node import GraphNode
from app.models.parser_result import ParserResult

ORG_RE = re.compile(
    r"\b(ООО|ИП|АО|ПАО|ЗАО|ОАО|ГК|ТОО)\s+\"?([A-Za-zА-Яа-я0-9 .\-]{2,})\"?",
    re.IGNORECASE,
)

EDGE_CONFIDENCE = {
    "contact_to_listing": 0.9,
    "listing_to_object": 0.8,
    "object_to_address": 0.7,
    "contact_to_organization": 0.6,
    "organization_to_object": 0.6,
    "contact_to_address": 0.6,
    "listing_to_source": 0.7,
}


def _normalize_phone(value: str | None) -> str:
    if not value:
        return ""
    digits = re.sub(r"\D+", "", value)
    if digits.startswith("8") and len(digits) == 11:
        digits = "7" + digits[1:]
    if digits.startswith("7") and len(digits) == 11:
        return f"+{digits}"
    return ""


def _normalize_email(value: str | None) -> str:
    return (value or "").strip().lower()


def _normalize_text(value: str | None) -> str:
    return " ".join((value or "").split()).strip().lower()


def _extract_orgs(text: str) -> list[str]:
    names: list[str] = []
    for match in ORG_RE.finditer(text or ""):
        prefix = match.group(1).upper()
        name = " ".join(match.group(2).split())
        if name:
            names.append(f"{prefix} {name}")
    return names


def _object_key(result: ParserResult) -> str:
    if result.fingerprint:
        return result.fingerprint
    parts = [
        result.normalized_address or "",
        str(result.area_sqm or ""),
        result.listing_type or "",
    ]
    return "|".join(parts)


def _contact_key(result: ParserResult) -> str | None:
    phone = _normalize_phone(result.contact_phone)
    if phone:
        return f"phone:{phone}"
    email = _normalize_email(result.contact_email)
    if email:
        return f"email:{email}"
    return None


def _get_or_create_node(
    db: Session,
    *,
    agency_id: int,
    node_type: str,
    entity_id: str,
    label: str | None = None,
    metadata: dict[str, Any] | None = None,
) -> GraphNode:
    node = (
        db.execute(
            select(GraphNode).where(
                GraphNode.agency_id == agency_id,
                GraphNode.node_type == node_type,
                GraphNode.entity_id == entity_id,
            )
        )
        .scalars()
        .first()
    )
    now = datetime.utcnow()
    if node:
        if label:
            node.label = label
        if metadata:
            node.metadata_json = {**(node.metadata_json or {}), **metadata}
        node.updated_at = now
        return node

    node = GraphNode(
        agency_id=agency_id,
        node_type=node_type,
        entity_id=entity_id,
        label=label,
        metadata_json=metadata,
        created_at=now,
        updated_at=now,
    )
    db.add(node)
    db.flush()
    return node


def _get_or_create_edge(
    db: Session,
    *,
    agency_id: int,
    from_node_id: int,
    to_node_id: int,
    edge_type: str,
    confidence: float,
    metadata: dict[str, Any] | None = None,
) -> GraphEdge:
    edge = (
        db.execute(
            select(GraphEdge).where(
                GraphEdge.agency_id == agency_id,
                GraphEdge.from_node_id == from_node_id,
                GraphEdge.to_node_id == to_node_id,
                GraphEdge.edge_type == edge_type,
            )
        )
        .scalars()
        .first()
    )
    now = datetime.utcnow()
    if edge:
        edge.last_seen_at = now
        edge.confidence = max(edge.confidence, confidence)
        if metadata:
            edge.metadata_json = {**(edge.metadata_json or {}), **metadata}
        return edge

    edge = GraphEdge(
        agency_id=agency_id,
        from_node_id=from_node_id,
        to_node_id=to_node_id,
        edge_type=edge_type,
        confidence=confidence,
        evidence_count=0,
        first_seen_at=now,
        last_seen_at=now,
        metadata_json=metadata,
    )
    db.add(edge)
    db.flush()
    return edge


def _add_edge_evidence(
    db: Session,
    *,
    agency_id: int,
    edge_id: int,
    evidence_type: str,
    source_entity_type: str,
    source_entity_id: str,
    description: str | None,
    weight: float,
) -> bool:
    existing = (
        db.execute(
            select(GraphEdgeEvidence.id).where(
                GraphEdgeEvidence.agency_id == agency_id,
                GraphEdgeEvidence.edge_id == edge_id,
                GraphEdgeEvidence.evidence_type == evidence_type,
                GraphEdgeEvidence.source_entity_type == source_entity_type,
                GraphEdgeEvidence.source_entity_id == source_entity_id,
            )
        )
        .scalars()
        .first()
    )
    if existing:
        return False
    db.add(
        GraphEdgeEvidence(
            agency_id=agency_id,
            edge_id=edge_id,
            evidence_type=evidence_type,
            source_entity_type=source_entity_type,
            source_entity_id=source_entity_id,
            description=description,
            weight=weight,
            observed_at=datetime.utcnow(),
        )
    )
    return True


def build_graph_for_agency(db: Session, agency_id: int, since: datetime | None = None) -> dict[str, int]:
    stmt = select(ParserResult).where(ParserResult.agency_id == agency_id)
    if since:
        stmt = stmt.where(ParserResult.updated_at >= since)
    results = db.execute(stmt).scalars().all()

    counters = {"nodes": 0, "edges": 0, "evidence": 0}

    for result in results:
        listing_node = _get_or_create_node(
            db,
            agency_id=agency_id,
            node_type="listing",
            entity_id=str(result.id),
            label=result.title,
            metadata={
                "status": result.status.value,
                "updated_at": result.updated_at.isoformat() if result.updated_at else None,
                "source_channel": result.source_channel.value,
                "monetization_tier": (result.payload or {}).get("monetization_tier"),
                "region_code": result.region_code,
                "city": result.city,
                "district": result.address_district,
                "listing_type": result.listing_type,
                "area_sqm": float(result.area_sqm) if result.area_sqm is not None else None,
                "price_rub": float(result.price_rub) if result.price_rub is not None else None,
            },
        )

        object_key = _object_key(result)
        object_node = _get_or_create_node(
            db,
            agency_id=agency_id,
            node_type="object",
            entity_id=object_key,
            label=result.title,
            metadata={
                "address": result.normalized_address,
                "area_sqm": float(result.area_sqm) if result.area_sqm is not None else None,
                "listing_type": result.listing_type,
            },
        )

        address_label = result.normalized_address or ", ".join(
            [item for item in [result.city, result.address_street] if item]
        )
        address_node = None
        if address_label:
            address_node = _get_or_create_node(
                db,
                agency_id=agency_id,
                node_type="address",
                entity_id=address_label,
                label=address_label,
                metadata={"city": result.city, "district": result.address_district},
            )

        contact_node = None
        contact_key = _contact_key(result)
        if contact_key:
            contact_label = result.contact_phone or result.contact_email
            contact_node = _get_or_create_node(
                db,
                agency_id=agency_id,
                node_type="contact",
                entity_id=contact_key,
                label=contact_label,
                metadata={"contact_name": result.contact_name, "contact_key": contact_key},
            )

        source_node = _get_or_create_node(
            db,
            agency_id=agency_id,
            node_type="source",
            entity_id=result.source_channel.value,
            label=result.source_channel.value,
            metadata=None,
        )

        org_nodes: list[GraphNode] = []
        text = _normalize_text(f"{result.title} {result.description or ''}")
        for org in _extract_orgs(text):
            org_nodes.append(
                _get_or_create_node(
                    db,
                    agency_id=agency_id,
                    node_type="organization",
                    entity_id=org,
                    label=org,
                    metadata=None,
                )
            )

        listing_object = _get_or_create_edge(
            db,
            agency_id=agency_id,
            from_node_id=listing_node.id,
            to_node_id=object_node.id,
            edge_type="listing_to_object",
            confidence=EDGE_CONFIDENCE["listing_to_object"],
        )
        if _add_edge_evidence(
            db,
            agency_id=agency_id,
            edge_id=listing_object.id,
            evidence_type="parser_result",
            source_entity_type="parser_result",
            source_entity_id=str(result.id),
            description=result.raw_url,
            weight=1.0,
        ):
            listing_object.evidence_count += 1
            counters["evidence"] += 1

        if address_node:
            object_address = _get_or_create_edge(
                db,
                agency_id=agency_id,
                from_node_id=object_node.id,
                to_node_id=address_node.id,
                edge_type="object_to_address",
                confidence=EDGE_CONFIDENCE["object_to_address"],
            )
            if _add_edge_evidence(
                db,
                agency_id=agency_id,
                edge_id=object_address.id,
                evidence_type="parser_result",
                source_entity_type="parser_result",
                source_entity_id=str(result.id),
                description=address_label,
                weight=0.8,
            ):
                object_address.evidence_count += 1
                counters["evidence"] += 1

        listing_source = _get_or_create_edge(
            db,
            agency_id=agency_id,
            from_node_id=listing_node.id,
            to_node_id=source_node.id,
            edge_type="listing_to_source",
            confidence=EDGE_CONFIDENCE["listing_to_source"],
        )
        if _add_edge_evidence(
            db,
            agency_id=agency_id,
            edge_id=listing_source.id,
            evidence_type="parser_result",
            source_entity_type="parser_result",
            source_entity_id=str(result.id),
            description=result.source_channel.value,
            weight=0.6,
        ):
            listing_source.evidence_count += 1
            counters["evidence"] += 1

        if contact_node:
            contact_listing = _get_or_create_edge(
                db,
                agency_id=agency_id,
                from_node_id=contact_node.id,
                to_node_id=listing_node.id,
                edge_type="contact_to_listing",
                confidence=EDGE_CONFIDENCE["contact_to_listing"],
            )
            if _add_edge_evidence(
                db,
                agency_id=agency_id,
                edge_id=contact_listing.id,
                evidence_type="parser_result",
                source_entity_type="parser_result",
                source_entity_id=str(result.id),
                description=result.raw_url,
                weight=1.0,
            ):
                contact_listing.evidence_count += 1
                counters["evidence"] += 1

            if address_node:
                contact_address = _get_or_create_edge(
                    db,
                    agency_id=agency_id,
                    from_node_id=contact_node.id,
                    to_node_id=address_node.id,
                    edge_type="contact_to_address",
                    confidence=EDGE_CONFIDENCE["contact_to_address"],
                )
                if _add_edge_evidence(
                    db,
                    agency_id=agency_id,
                    edge_id=contact_address.id,
                    evidence_type="parser_result",
                    source_entity_type="parser_result",
                    source_entity_id=str(result.id),
                    description=address_label,
                    weight=0.7,
                ):
                    contact_address.evidence_count += 1
                    counters["evidence"] += 1

            for org_node in org_nodes:
                contact_org = _get_or_create_edge(
                    db,
                    agency_id=agency_id,
                    from_node_id=contact_node.id,
                    to_node_id=org_node.id,
                    edge_type="contact_to_organization",
                    confidence=EDGE_CONFIDENCE["contact_to_organization"],
                )
                if _add_edge_evidence(
                    db,
                    agency_id=agency_id,
                    edge_id=contact_org.id,
                    evidence_type="parser_result",
                    source_entity_type="parser_result",
                    source_entity_id=str(result.id),
                    description=org_node.label,
                    weight=0.6,
                ):
                    contact_org.evidence_count += 1
                    counters["evidence"] += 1

        for org_node in org_nodes:
            org_object = _get_or_create_edge(
                db,
                agency_id=agency_id,
                from_node_id=org_node.id,
                to_node_id=object_node.id,
                edge_type="organization_to_object",
                confidence=EDGE_CONFIDENCE["organization_to_object"],
            )
            if _add_edge_evidence(
                db,
                agency_id=agency_id,
                edge_id=org_object.id,
                evidence_type="parser_result",
                source_entity_type="parser_result",
                source_entity_id=str(result.id),
                description=org_node.label,
                weight=0.6,
            ):
                org_object.evidence_count += 1
                counters["evidence"] += 1

    db.commit()
    return counters
