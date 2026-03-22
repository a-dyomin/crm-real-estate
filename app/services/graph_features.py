from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.models.graph_edge import GraphEdge
from app.models.graph_feature_snapshot import GraphFeatureSnapshot
from app.models.graph_node import GraphNode
from app.models.graph_edge_evidence import GraphEdgeEvidence
from app.models.graph_fuzzy_match import GraphFuzzyMatch


EDGE_TYPES = {
    "contact_to_listing",
    "listing_to_object",
    "object_to_address",
    "contact_to_organization",
    "organization_to_object",
    "contact_to_address",
    "listing_to_source",
}


def _contact_node(db: Session, agency_id: int, contact_key: str) -> GraphNode | None:
    return (
        db.execute(
            select(GraphNode).where(
                GraphNode.agency_id == agency_id,
                GraphNode.node_type == "contact",
                GraphNode.entity_id == contact_key,
            )
        )
        .scalars()
        .first()
    )


def _fetch_nodes(db: Session, node_ids: list[int]) -> dict[int, GraphNode]:
    if not node_ids:
        return {}
    nodes = db.execute(select(GraphNode).where(GraphNode.id.in_(node_ids))).scalars().all()
    return {node.id: node for node in nodes}


def _edge_map(db: Session, agency_id: int, edge_type: str, from_ids: list[int]) -> list[GraphEdge]:
    if not from_ids:
        return []
    return (
        db.execute(
            select(GraphEdge).where(
                GraphEdge.agency_id == agency_id,
                GraphEdge.edge_type == edge_type,
                GraphEdge.from_node_id.in_(from_ids),
            )
        )
        .scalars()
        .all()
    )


def compute_contact_graph_features(db: Session, agency_id: int, contact_node: GraphNode) -> dict[str, Any]:
    listing_edges = (
        db.execute(
            select(GraphEdge).where(
                GraphEdge.agency_id == agency_id,
                GraphEdge.edge_type == "contact_to_listing",
                GraphEdge.from_node_id == contact_node.id,
            )
        )
        .scalars()
        .all()
    )
    listing_ids = [edge.to_node_id for edge in listing_edges]
    listing_nodes = _fetch_nodes(db, listing_ids)

    listing_meta = [node.metadata_json or {} for node in listing_nodes.values()]
    listing_count = len(listing_nodes)
    active_listing_count = sum(1 for meta in listing_meta if meta.get("status") not in ("rejected", "archive"))
    archived_listing_count = sum(1 for meta in listing_meta if meta.get("monetization_tier") == "archive")

    listing_to_object_edges = _edge_map(db, agency_id, "listing_to_object", listing_ids)
    object_ids = [edge.to_node_id for edge in listing_to_object_edges]
    object_nodes = _fetch_nodes(db, object_ids)

    object_to_address_edges = _edge_map(db, agency_id, "object_to_address", object_ids)
    address_ids = [edge.to_node_id for edge in object_to_address_edges]
    address_nodes = _fetch_nodes(db, address_ids)

    contact_to_org_edges = (
        db.execute(
            select(GraphEdge).where(
                GraphEdge.agency_id == agency_id,
                GraphEdge.edge_type == "contact_to_organization",
                GraphEdge.from_node_id == contact_node.id,
            )
        )
        .scalars()
        .all()
    )
    org_ids = [edge.to_node_id for edge in contact_to_org_edges]
    org_nodes = _fetch_nodes(db, org_ids)

    listing_to_source_edges = _edge_map(db, agency_id, "listing_to_source", listing_ids)
    source_ids = [edge.to_node_id for edge in listing_to_source_edges]
    source_nodes = _fetch_nodes(db, source_ids)

    linked_listing_count = len(listing_nodes)
    linked_object_count = len(object_nodes)
    linked_address_count = len(address_nodes)
    linked_organization_count = len(org_nodes)
    source_diversity = len(source_nodes)

    object_counts: Counter[int] = Counter()
    object_sources: dict[int, set[int]] = defaultdict(set)
    listing_to_objects: dict[int, list[int]] = defaultdict(list)
    for edge in listing_to_object_edges:
        listing_to_objects[edge.from_node_id].append(edge.to_node_id)
        object_counts[edge.to_node_id] += 1

    listing_to_sources: dict[int, list[int]] = defaultdict(list)
    for edge in listing_to_source_edges:
        listing_to_sources[edge.from_node_id].append(edge.to_node_id)

    for listing_id, object_list in listing_to_objects.items():
        sources = listing_to_sources.get(listing_id, [])
        for object_id in object_list:
            for source_id in sources:
                object_sources[object_id].add(source_id)

    max_object_cluster = max(object_counts.values()) if object_counts else 0
    single_asset_concentration = max_object_cluster / max(1, linked_listing_count)

    objects_multi_source = sum(1 for sources in object_sources.values() if len(sources) > 1)
    cross_source_consistency = objects_multi_source / max(1, linked_object_count)

    geographic_spread = linked_address_count / max(1, linked_listing_count)
    hub_score = min(100.0, linked_listing_count * max(1, source_diversity) * geographic_spread * 5)
    cluster_density = linked_listing_count / max(1, linked_object_count)

    return {
        "linked_listing_count": linked_listing_count,
        "linked_object_count": linked_object_count,
        "linked_address_count": linked_address_count,
        "linked_organization_count": linked_organization_count,
        "active_listing_count": active_listing_count,
        "archived_listing_count": archived_listing_count,
        "source_diversity": source_diversity,
        "geographic_spread_score": round(geographic_spread, 3),
        "single_asset_concentration_score": round(single_asset_concentration, 3),
        "hub_score": round(hub_score, 2),
        "cross_source_consistency_score": round(cross_source_consistency, 3),
        "cluster_density_score": round(cluster_density, 3),
    }


def refresh_graph_features(db: Session, agency_id: int) -> int:
    contact_nodes = (
        db.execute(
            select(GraphNode).where(GraphNode.agency_id == agency_id, GraphNode.node_type == "contact")
        )
        .scalars()
        .all()
    )
    updated = 0
    for contact_node in contact_nodes:
        features = compute_contact_graph_features(db, agency_id, contact_node)
        snapshot = (
            db.execute(
                select(GraphFeatureSnapshot).where(
                    GraphFeatureSnapshot.agency_id == agency_id,
                    GraphFeatureSnapshot.node_id == contact_node.id,
                    GraphFeatureSnapshot.feature_type == "contact",
                )
            )
            .scalars()
            .first()
        )
        if snapshot:
            snapshot.payload_json = features
            snapshot.computed_at = datetime.utcnow()
        else:
            db.add(
                GraphFeatureSnapshot(
                    agency_id=agency_id,
                    node_id=contact_node.id,
                    feature_type="contact",
                    payload_json=features,
                    computed_at=datetime.utcnow(),
                )
            )
        updated += 1
    db.commit()
    return updated


def load_graph_features(db: Session, agency_id: int, contact_key: str) -> dict[str, Any] | None:
    node = _contact_node(db, agency_id, contact_key)
    if not node:
        return None
    snapshot = (
        db.execute(
            select(GraphFeatureSnapshot).where(
                GraphFeatureSnapshot.agency_id == agency_id,
                GraphFeatureSnapshot.node_id == node.id,
                GraphFeatureSnapshot.feature_type == "contact",
            )
        )
        .scalars()
        .first()
    )
    return snapshot.payload_json if snapshot else None


def load_contact_graph_details(db: Session, agency_id: int, contact_key: str) -> dict[str, Any] | None:
    node = _contact_node(db, agency_id, contact_key)
    if not node:
        return None
    features = load_graph_features(db, agency_id, contact_key) or {}

    contact_to_org_edges = (
        db.execute(
            select(GraphEdge).where(
                GraphEdge.agency_id == agency_id,
                GraphEdge.edge_type == "contact_to_organization",
                GraphEdge.from_node_id == node.id,
            )
        )
        .scalars()
        .all()
    )
    org_nodes = _fetch_nodes(db, [edge.to_node_id for edge in contact_to_org_edges])

    contact_to_address_edges = (
        db.execute(
            select(GraphEdge).where(
                GraphEdge.agency_id == agency_id,
                GraphEdge.edge_type == "contact_to_address",
                GraphEdge.from_node_id == node.id,
            )
        )
        .scalars()
        .all()
    )
    contact_to_listing_edges = (
        db.execute(
            select(GraphEdge).where(
                GraphEdge.agency_id == agency_id,
                GraphEdge.edge_type == "contact_to_listing",
                GraphEdge.from_node_id == node.id,
            )
        )
        .scalars()
        .all()
    )
    address_nodes = _fetch_nodes(db, [edge.to_node_id for edge in contact_to_address_edges])

    edge_ids = [edge.id for edge in contact_to_org_edges + contact_to_address_edges + contact_to_listing_edges]
    evidence_rows = []
    if edge_ids:
        evidence_rows = (
            db.execute(
                select(GraphEdgeEvidence)
                .where(GraphEdgeEvidence.agency_id == agency_id, GraphEdgeEvidence.edge_id.in_(edge_ids))
                .order_by(GraphEdgeEvidence.observed_at.desc())
                .limit(50)
            )
            .scalars()
            .all()
        )
    fuzzy_rows = (
        db.execute(
            select(GraphFuzzyMatch).where(
                GraphFuzzyMatch.agency_id == agency_id,
                (GraphFuzzyMatch.from_node_id == node.id) | (GraphFuzzyMatch.to_node_id == node.id),
            )
        )
        .scalars()
        .all()
    )
    evidence = [
        {
            "evidence_type": row.evidence_type,
            "source_entity_type": row.source_entity_type,
            "source_entity_id": row.source_entity_id,
            "description": row.description,
            "observed_at": row.observed_at.isoformat(),
        }
        for row in evidence_rows
    ]
    evidence.extend(
        [
            {
                "evidence_type": f"fuzzy:{row.match_type}",
                "source_entity_type": "fuzzy_match",
                "source_entity_id": str(row.id),
                "description": f"{row.raw_value_a} ~ {row.raw_value_b}",
                "observed_at": row.updated_at.isoformat(),
            }
            for row in fuzzy_rows
        ]
    )

    return {
        "features": features,
        "linked_organizations": [
            {"label": node.label, "entity_id": node.entity_id} for node in org_nodes.values() if node.label
        ],
        "linked_addresses": [
            {"label": node.label, "entity_id": node.entity_id} for node in address_nodes.values() if node.label
        ],
        "evidence": evidence,
    }
