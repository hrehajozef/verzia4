"""CRUD pre utb_authors_limited (remote DB)."""

from __future__ import annotations

from sqlalchemy import text

from src.config.settings import settings
from src.db.engines import get_remote_engine

# Schéma a tabuľka na remote DB
_SCHEMA = settings.remote_schema
_TABLE  = "utb_authors_limited"


def get_all_authors(engine=None) -> list[dict]:
    """Vráti všetkých autorov z utb_authors_limited."""
    engine = engine or get_remote_engine()
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT display_name
            FROM "{_SCHEMA}"."{_TABLE}"
            ORDER BY display_name
        """)).fetchall()
    result = []
    for row in rows:
        raw = row.display_name or ""
        variants = [v.strip() for v in raw.split("||") if v.strip()]
        result.append({
            "display_name": raw,
            "variants":     variants,
            "primary":      variants[0] if variants else raw,
        })
    return result


def search_authors(query: str, engine=None) -> list[dict]:
    """Fulltextové vyhľadávanie autorov podľa časti mena."""
    engine = engine or get_remote_engine()
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT display_name
            FROM "{_SCHEMA}"."{_TABLE}"
            WHERE display_name ILIKE :q
            ORDER BY display_name
            LIMIT 50
        """), {"q": f"%{query}%"}).fetchall()
    result = []
    for row in rows:
        raw = row.display_name or ""
        variants = [v.strip() for v in raw.split("||") if v.strip()]
        result.append({
            "display_name": raw,
            "variants":     variants,
            "primary":      variants[0] if variants else raw,
        })
    return result


def add_author(display_name: str, engine=None) -> None:
    """Pridá nového autora do utb_authors_limited."""
    engine = engine or get_remote_engine()
    with engine.begin() as conn:
        conn.execute(text(f"""
            INSERT INTO "{_SCHEMA}"."{_TABLE}" (display_name)
            VALUES (:name)
            ON CONFLICT DO NOTHING
        """), {"name": display_name.strip()})


def remove_author(display_name: str, engine=None) -> None:
    """Odstráni autora z utb_authors_limited podľa display_name."""
    engine = engine or get_remote_engine()
    with engine.begin() as conn:
        conn.execute(text(f"""
            DELETE FROM "{_SCHEMA}"."{_TABLE}"
            WHERE display_name = :name
        """), {"name": display_name.strip()})
