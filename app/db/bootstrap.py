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
        _add_column_if_missing(engine, "leads", "need_type", "VARCHAR(64)")
        _add_column_if_missing(engine, "leads", "search_districts", "JSON")
        _add_column_if_missing(engine, "leads", "object_address", "VARCHAR(255)")
        _add_column_if_missing(engine, "leads", "property_type", "VARCHAR(64)")
        _add_column_if_missing(engine, "leads", "area_range", "VARCHAR(64)")
        _add_column_if_missing(engine, "leads", "business_activity", "VARCHAR(128)")
        _add_column_if_missing(engine, "leads", "urgency", "VARCHAR(64)")
        _add_column_if_missing(engine, "leads", "source_details", "TEXT")

    if _sqlite_table_exists(engine, "parser_results"):
        _add_column_if_missing(engine, "parser_results", "telegram_post_url", "VARCHAR(1024)")
        _add_column_if_missing(engine, "parser_results", "listing_type", "VARCHAR(32)")
        _add_column_if_missing(engine, "parser_results", "image_url", "VARCHAR(1024)")
        _add_column_if_missing(engine, "parser_results", "address_district", "VARCHAR(128)")
        _add_column_if_missing(engine, "parser_results", "address_street", "VARCHAR(255)")
