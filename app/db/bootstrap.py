from sqlalchemy import text
from sqlalchemy.engine import Engine


def _sqlite_table_exists(engine: Engine, table_name: str) -> bool:
    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name=:name;"),
            {"name": table_name},
        ).fetchone()
    return row is not None


def _sqlite_columns(engine: Engine, table_name: str) -> set[str]:
    with engine.connect() as conn:
        rows = conn.execute(text(f"PRAGMA table_info({table_name});")).fetchall()
    return {str(row[1]) for row in rows}


def _add_column_if_missing(engine: Engine, table_name: str, column_name: str, ddl_type: str) -> None:
    existing = _sqlite_columns(engine, table_name)
    if column_name in existing:
        return
    with engine.begin() as conn:
        conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {ddl_type};"))


def _normalize_legacy_lead_statuses(engine: Engine) -> None:
    if not _sqlite_table_exists(engine, "leads"):
        return
    mapping = {
        "new": "new_lead",
        "qualified": "qualification",
        "appointment_set": "sent_to_commission",
        "disqualified": "poor_quality_lead",
        "converted": "high_quality_lead",
    }
    with engine.begin() as conn:
        for old_status, new_status in mapping.items():
            conn.execute(
                text("UPDATE leads SET status = :new_status WHERE status = :old_status"),
                {"new_status": new_status, "old_status": old_status},
            )


def apply_runtime_migrations(engine: Engine) -> None:
    if not str(engine.url).startswith("sqlite"):
        return
    if not _sqlite_table_exists(engine, "users"):
        return

    _add_column_if_missing(engine, "users", "password_hash", "VARCHAR(255) DEFAULT ''")
    _add_column_if_missing(engine, "users", "is_active", "BOOLEAN DEFAULT 1")
    _add_column_if_missing(engine, "users", "last_login_at", "DATETIME")
    _add_column_if_missing(engine, "users", "updated_at", "DATETIME")
    _normalize_legacy_lead_statuses(engine)
    if _sqlite_table_exists(engine, "leads"):
        _add_column_if_missing(engine, "leads", "lead_source", "VARCHAR(255)")
        _add_column_if_missing(engine, "leads", "lead_state", "VARCHAR(32)")
        _add_column_if_missing(engine, "leads", "auto_created", "BOOLEAN DEFAULT 0")
        _add_column_if_missing(engine, "leads", "need_type", "VARCHAR(64)")
        _add_column_if_missing(engine, "leads", "search_districts", "JSON")
        _add_column_if_missing(engine, "leads", "object_address", "VARCHAR(255)")
        _add_column_if_missing(engine, "leads", "property_type", "VARCHAR(64)")
        _add_column_if_missing(engine, "leads", "area_range", "VARCHAR(64)")
        _add_column_if_missing(engine, "leads", "business_activity", "VARCHAR(128)")
        _add_column_if_missing(engine, "leads", "urgency", "VARCHAR(64)")
        _add_column_if_missing(engine, "leads", "source_details", "TEXT")

    if _sqlite_table_exists(engine, "parser_results"):
        _add_column_if_missing(engine, "parser_results", "parser_source_id", "INTEGER")
        _add_column_if_missing(engine, "parser_results", "telegram_post_url", "VARCHAR(1024)")
        _add_column_if_missing(engine, "parser_results", "listing_type", "VARCHAR(32)")
        _add_column_if_missing(engine, "parser_results", "image_url", "VARCHAR(1024)")
        _add_column_if_missing(engine, "parser_results", "address_district", "VARCHAR(128)")
        _add_column_if_missing(engine, "parser_results", "address_street", "VARCHAR(255)")
        _add_column_if_missing(engine, "parser_results", "contact_candidates", "JSON")
        _add_column_if_missing(engine, "parser_results", "selected_contact", "JSON")
        _add_column_if_missing(engine, "parser_results", "rejected_contacts", "JSON")
        _add_column_if_missing(engine, "parser_results", "contact_rejection_reasons", "JSON")
        _add_column_if_missing(engine, "parser_results", "contact_confidence", "FLOAT")
        _add_column_if_missing(engine, "parser_results", "lead_score", "FLOAT")
        _add_column_if_missing(engine, "parser_results", "owner_probability_score", "FLOAT")
        _add_column_if_missing(engine, "parser_results", "owner_priority_score", "FLOAT")
        _add_column_if_missing(engine, "parser_results", "owner_confidence", "FLOAT")
        _add_column_if_missing(engine, "parser_results", "owner_explanation_summary", "VARCHAR(512)")
        _add_column_if_missing(engine, "parser_results", "market_median_price", "FLOAT")
        _add_column_if_missing(engine, "parser_results", "market_median_price_per_m2", "FLOAT")
        _add_column_if_missing(engine, "parser_results", "deviation_from_market_pct", "FLOAT")
        _add_column_if_missing(engine, "parser_results", "below_market_flag", "BOOLEAN")
        _add_column_if_missing(engine, "parser_results", "pipeline_status", "VARCHAR(32)")
        _add_column_if_missing(engine, "parser_results", "published_at", "DATETIME")
        _add_column_if_missing(engine, "parser_results", "property_id", "INTEGER")

    if _sqlite_table_exists(engine, "parser_sources"):
        _add_column_if_missing(engine, "parser_sources", "source_state", "VARCHAR(64)")
        _add_column_if_missing(engine, "parser_sources", "activation_mode", "VARCHAR(64)")
        _add_column_if_missing(engine, "parser_sources", "auto_discovered", "BOOLEAN DEFAULT 0")
        _add_column_if_missing(engine, "parser_sources", "parse_frequency_minutes", "INTEGER DEFAULT 1440")
        _add_column_if_missing(engine, "parser_sources", "parse_priority", "INTEGER DEFAULT 50")
        _add_column_if_missing(engine, "parser_sources", "last_discovery_at", "DATETIME")
        _add_column_if_missing(engine, "parser_sources", "last_parsed_at", "DATETIME")
        _add_column_if_missing(engine, "parser_sources", "next_scheduled_parse_at", "DATETIME")
        _add_column_if_missing(engine, "parser_sources", "last_fetch_at", "DATETIME")
        _add_column_if_missing(engine, "parser_sources", "last_error_at", "DATETIME")
        _add_column_if_missing(engine, "parser_sources", "health_status", "VARCHAR(64)")
        _add_column_if_missing(engine, "parser_sources", "failure_count", "INTEGER DEFAULT 0")
        _add_column_if_missing(engine, "parser_sources", "consecutive_success_count", "INTEGER DEFAULT 0")
        _add_column_if_missing(engine, "parser_sources", "scheduler_lock_key", "VARCHAR(128)")
        _add_column_if_missing(engine, "parser_sources", "auto_activation_reason", "TEXT")
        _add_column_if_missing(engine, "parser_sources", "listings_parsed_last_run", "INTEGER DEFAULT 0")
        _add_column_if_missing(engine, "parser_sources", "contacts_extracted_last_run", "INTEGER DEFAULT 0")
        _add_column_if_missing(engine, "parser_sources", "contacts_rejected_last_run", "INTEGER DEFAULT 0")
        _add_column_if_missing(engine, "parser_sources", "leads_published_last_run", "INTEGER DEFAULT 0")

    if _sqlite_table_exists(engine, "source_parse_runs"):
        _add_column_if_missing(engine, "source_parse_runs", "listings_parsed", "INTEGER DEFAULT 0")
        _add_column_if_missing(engine, "source_parse_runs", "contacts_extracted", "INTEGER DEFAULT 0")
        _add_column_if_missing(engine, "source_parse_runs", "contacts_rejected", "INTEGER DEFAULT 0")
        _add_column_if_missing(engine, "source_parse_runs", "leads_published", "INTEGER DEFAULT 0")

    if _sqlite_table_exists(engine, "contact_identities"):
        _add_column_if_missing(engine, "contact_identities", "owner_priority_score", "FLOAT")
        _add_column_if_missing(engine, "contact_identities", "lifecycle_status", "VARCHAR(32)")
        _add_column_if_missing(engine, "contact_identities", "published_to_owners_at", "DATETIME")
        _add_column_if_missing(engine, "contact_identities", "promoted_to_call_center_at", "DATETIME")

    if _sqlite_table_exists(engine, "parser_runs"):
        _add_column_if_missing(engine, "parser_runs", "objects_resolved", "INTEGER DEFAULT 0")
        _add_column_if_missing(engine, "parser_runs", "identities_scored", "INTEGER DEFAULT 0")
        _add_column_if_missing(engine, "parser_runs", "owners_published", "INTEGER DEFAULT 0")
        _add_column_if_missing(engine, "parser_runs", "leads_auto_created", "INTEGER DEFAULT 0")
        _add_column_if_missing(engine, "parser_runs", "call_center_created", "INTEGER DEFAULT 0")
        _add_column_if_missing(engine, "parser_runs", "rejected_count", "INTEGER DEFAULT 0")
