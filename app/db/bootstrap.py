from sqlalchemy import text
from sqlalchemy.engine import Engine


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


def apply_runtime_migrations(engine: Engine) -> None:
    if not str(engine.url).startswith("sqlite"):
        return
    with engine.connect() as conn:
        users_exists = conn.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name='users';")
        ).fetchone()
    if not users_exists:
        return

    _add_column_if_missing(engine, "users", "password_hash", "VARCHAR(255) DEFAULT ''")
    _add_column_if_missing(engine, "users", "is_active", "BOOLEAN DEFAULT 1")
    _add_column_if_missing(engine, "users", "last_login_at", "DATETIME")
    _add_column_if_missing(engine, "users", "updated_at", "DATETIME")

