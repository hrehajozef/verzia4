"""Read-only access to remote utb_authors_limited."""

from __future__ import annotations

from sqlalchemy import text

from src.config.settings import settings
from src.db.engines import get_remote_engine

_SCHEMA = settings.remote_schema
_TABLE = "utb_authors_limited"
_UTB_FILTER_SQL = "COALESCE(utb, '') ILIKE 'ano'"


def _rows_to_authors(rows) -> list[dict]:
    result = []
    for row in rows:
        raw = row.display_name or ""
        variants = [value.strip() for value in raw.split("||") if value.strip()]
        result.append({
            "display_name": raw,
            "variants": variants,
            "primary": variants[0] if variants else raw,
        })
    return result


def get_all_authors(engine=None) -> list[dict]:
    """Return all internal authors from remote utb_authors_limited."""
    engine = engine or get_remote_engine()
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT display_name
            FROM "{_SCHEMA}"."{_TABLE}"
            WHERE {_UTB_FILTER_SQL}
            ORDER BY display_name
        """)).fetchall()
    return _rows_to_authors(rows)


def search_authors(query: str, engine=None) -> list[dict]:
    """Search internal authors in remote utb_authors_limited."""
    engine = engine or get_remote_engine()
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT display_name
            FROM "{_SCHEMA}"."{_TABLE}"
            WHERE {_UTB_FILTER_SQL}
              AND display_name ILIKE :q
            ORDER BY display_name
            LIMIT 50
        """), {"q": f"%{query}%"}).fetchall()
    return _rows_to_authors(rows)


def add_author(display_name: str, engine=None) -> None:
    """Temporarily kept for compatibility; inserts a UTB author row."""
    engine = engine or get_remote_engine()
    with engine.begin() as conn:
        conn.execute(text(f"""
            INSERT INTO "{_SCHEMA}"."{_TABLE}" (display_name, utb)
            VALUES (:name, 'ano')
            ON CONFLICT DO NOTHING
        """), {"name": display_name.strip()})


def remove_author(display_name: str, engine=None) -> None:
    """Temporarily kept for compatibility; removes one UTB author row."""
    engine = engine or get_remote_engine()
    with engine.begin() as conn:
        conn.execute(text(f"""
            DELETE FROM "{_SCHEMA}"."{_TABLE}"
            WHERE {_UTB_FILTER_SQL}
              AND display_name = :name
        """), {"name": display_name.strip()})
