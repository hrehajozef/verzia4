"""Zdielane helpery pre detail zaznamu a change-buffer workflow."""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any

from sqlalchemy import text

from src.authors.parsers.scopus import parse_scopus_affiliation_array
from src.authors.parsers.wos import parse_wos_affiliation_array
from src.authors.source_authors import _author_key
from src.authors.source_authors import split_source_author_lists
from src.common.constants import (
    CZECH_DEPARTMENT_MAP_NORM,
    DEPARTMENTS,
    QUEUE_TABLE,
    _norm as _norm_const,
)
from src.config.settings import settings
from src.db.engines import get_local_engine

VALUE_SEPARATOR = "||"
CHANGE_BUFFER_TABLE = "utb_change_buffer"
DETAIL_ROW_ORDER_PATH = Path("data/detail_row_order.json")

PRIORITY_FIELDS = [
    "dc.title",
    "dc.title.alternative",
    "dc.title.translated",
    "dc.title.other",
    "dc.description.title",
    "dc.contributor.author",
    "utb.contributor.internalauthor",
    "utb.identifier.wok",
    "utb.identifier.scopus",
    "dc.identifier.doi",
    "utb.identifier.obdid",
    "dc.identifier.issn",
    "dc.identifier.isbn",
    "dc.date.issued",
    "dc.publisher",
    "dc.relation.ispartof",
    "utb.relation.volume",
    "utb.relation.issue",
    "dc.citation.spage",
    "dc.citation.epage",
    "dc.rights",
    "dc.rights.uri",
    "dc.rights.access",
    "utb.fulltext.affiliation",
    "utb.fulltext.faculty",
    "utb.fulltext.ou",
    "utb.faculty",
    "utb.ou",
    "utb_date_received",
    "utb_date_reviewed",
    "utb_date_accepted",
    "utb_date_published_online",
    "utb_date_published",
    "utb.fulltext.dates",
    "dc.type",
]

_AFFILIATION_KEYS = {"utb.wos.affiliation", "utb.scopus.affiliation", "utb.fulltext.affiliation"}
_AFFILIATION_LABEL_ORDER: tuple[str, ...] = (
    "utb.fulltext.affiliation",
    "utb.wos.affiliation",
    "utb.scopus.affiliation",
)
AFFILIATION_GROUP_LABEL = "\n".join(_AFFILIATION_LABEL_ORDER)

_PROPOSED_FROM_QUEUE: dict[str, str] = {
    "dc.contributor.author": "author_dc_names",
    "utb.contributor.internalauthor": "author_internal_names",
    "dc.publisher": "journal_norm_proposed_publisher",
    "dc.relation.ispartof": "journal_norm_proposed_ispartof",
    "utb.faculty": "author_faculty",
    "utb.ou": "author_ou",
}

_AUTHOR_LLM_FIELDS: dict[str, str] = {
    "utb.contributor.internalauthor": "name",
    "utb.faculty": "faculty",
    "utb.ou": "ou",
}

_DATE_LLM_FIELDS: dict[str, str] = {
    "utb_date_received": "received",
    "utb_date_reviewed": "reviewed",
    "utb_date_accepted": "accepted",
    "utb_date_published_online": "published_online",
    "utb_date_published": "published",
}

_WOS_COL_MAP: dict[str, str] = {
    "utb.fulltext.affiliation": "utb.wos.affiliation",
    "utb.identifier.wok": "utb.identifier.wok",
}

_SCOPUS_COL_MAP: dict[str, str] = {
    "utb.fulltext.affiliation": "utb.scopus.affiliation",
    "utb.identifier.scopus": "utb.identifier.scopus",
}

QUEUE_FIELDS: dict[str, str] = {
    "utb_date_received": "Datum prijatia zaznamu",
    "utb_date_reviewed": "Datum recenzovania",
    "utb_date_accepted": "Datum prijatia clanku",
    "utb_date_published_online": "Datum online vydania",
    "utb_date_published": "Datum vydania tlacou",
}

_HIDDEN_FIELDS: set[str] = {
    "author_dc_names",
    "author_internal_names",
    "journal_norm_proposed_publisher",
    "journal_norm_proposed_ispartof",
    "validation_suggested_fixes",
    "validation_flags",
    "flags",
    "date_flags",
    "heuristic_status",
    "llm_status",
    "date_heuristic_status",
    "date_llm_status",
    "date_needs_llm",
    "needs_llm",
    "validation_status",
    "validation_version",
    "validation_checked_at",
    "date_heuristic_version",
    "date_processed_at",
    "date_llm_processed_at",
    "date_llm_result",
    "heuristic_version",
    "heuristic_processed_at",
    "llm_processed_at",
    "llm_result",
    "updated_at",
    "librarian_checked_at",
    "librarian_modified_at",
    "author_heuristic_status",
    "author_heuristic_version",
    "author_heuristic_processed_at",
    "author_needs_llm",
    "author_llm_status",
    "author_llm_result",
    "author_llm_processed_at",
    "author_flags",
    "author_faculty",
    "author_ou",
}

_VALID_OU_NAMES: frozenset[str] = frozenset(DEPARTMENTS.keys())


def _translate_ou(value: str | None) -> str | None:
    if not value:
        return value
    if value in _VALID_OU_NAMES:
        return value
    translated = CZECH_DEPARTMENT_MAP_NORM.get(_norm_const(value))
    return translated or value


def _to_display(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (list, tuple)):
        non_empty = [str(item).strip() for item in value if item is not None and str(item).strip()]
        return VALUE_SEPARATOR.join(non_empty) if non_empty else None
    if isinstance(value, datetime):
        return value.date().isoformat()
    text_value = str(value).strip()
    return text_value or None


def _split_display_values(value: str | None) -> list[str]:
    if not value:
        return []
    return [item.strip() for item in re.split(r"\s*\|\|\s*", value) if item.strip()]


def get_detail_row_order() -> list[str]:
    try:
        if DETAIL_ROW_ORDER_PATH.exists():
            data = json.loads(DETAIL_ROW_ORDER_PATH.read_text(encoding="utf-8"))
            if isinstance(data, list):
                values = [str(item).strip() for item in data if str(item).strip()]
                if values:
                    return values
    except Exception:
        pass
    return list(PRIORITY_FIELDS)


def save_detail_row_order(fields: list[str]) -> list[str]:
    cleaned = list(dict.fromkeys(str(field).strip() for field in fields if str(field).strip()))
    if not cleaned:
        cleaned = list(PRIORITY_FIELDS)
    DETAIL_ROW_ORDER_PATH.parent.mkdir(parents=True, exist_ok=True)
    DETAIL_ROW_ORDER_PATH.write_text(
        json.dumps(cleaned, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return cleaned


def reset_detail_row_order() -> list[str]:
    if DETAIL_ROW_ORDER_PATH.exists():
        DETAIL_ROW_ORDER_PATH.unlink()
    return list(PRIORITY_FIELDS)


def ensure_change_buffer_table(engine=None) -> None:
    engine = engine or get_local_engine()
    schema = settings.local_schema
    queue = QUEUE_TABLE
    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS "{schema}"."{CHANGE_BUFFER_TABLE}" (
                id BIGSERIAL PRIMARY KEY,
                resource_id BIGINT NOT NULL,
                field_key TEXT NOT NULL,
                target_table TEXT NOT NULL,
                old_value TEXT,
                new_value TEXT,
                created_at TIMESTAMPTZ DEFAULT now(),
                approved_at TIMESTAMPTZ,
                discarded_at TIMESTAMPTZ
            )
        """))
        conn.execute(text(f"""
            CREATE INDEX IF NOT EXISTS idx_{CHANGE_BUFFER_TABLE}_resource_pending
            ON "{schema}"."{CHANGE_BUFFER_TABLE}" (resource_id)
            WHERE approved_at IS NULL AND discarded_at IS NULL
        """))
        conn.execute(text(f"""
            ALTER TABLE "{schema}"."{queue}"
            ADD COLUMN IF NOT EXISTS librarian_modified_at TIMESTAMPTZ
        """))


def get_pending_changes(resource_id: str, engine=None) -> list[dict[str, Any]]:
    engine = engine or get_local_engine()
    schema = settings.local_schema
    ensure_change_buffer_table(engine)
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT id, field_key, target_table, old_value, new_value, created_at
            FROM "{schema}"."{CHANGE_BUFFER_TABLE}"
            WHERE resource_id = :rid
              AND approved_at IS NULL
              AND discarded_at IS NULL
            ORDER BY created_at DESC, id DESC
        """), {"rid": int(resource_id)}).mappings().fetchall()
    return [dict(row) for row in rows]


def _pending_change_map(pending_changes: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    latest: dict[tuple[str, str], dict[str, Any]] = {}
    for item in pending_changes:
        key = (str(item.get("field_key") or ""), str(item.get("target_table") or ""))
        if not key[0] or not key[1]:
            continue
        previous = latest.get(key)
        if previous is None or (item.get("id") or 0) > (previous.get("id") or 0):
            latest[key] = item
    return latest


def _load_table_columns(engine, schema: str, table: str) -> dict[str, str]:
    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT column_name, udt_name
            FROM information_schema.columns
            WHERE table_schema = :s AND table_name = :t
            ORDER BY ordinal_position
        """), {"s": schema, "t": table}).fetchall()
    return {row.column_name: row.udt_name for row in rows}


def _resolve_field_target(
    field_key: str,
    main_columns: dict[str, str],
    queue_columns: dict[str, str],
    main_table: str,
    queue_table: str,
) -> tuple[str, str]:
    if field_key in QUEUE_FIELDS:
        if field_key not in queue_columns:
            raise ValueError(f"Stlpec '{field_key}' neexistuje v tabulke '{queue_table}'")
        return queue_table, queue_columns[field_key]
    if field_key in main_columns:
        return main_table, main_columns[field_key]
    if field_key in queue_columns:
        return queue_table, queue_columns[field_key]
    raise ValueError(f"Stlpec '{field_key}' neexistuje v tabulke '{main_table}' ani '{queue_table}'")


def _display_to_db_value(new_value: str, udt_name: str) -> Any:
    stripped = new_value.strip() if new_value else ""
    if udt_name.startswith("_"):
        parts = _split_display_values(stripped)
        return parts if parts else []
    return stripped or None


def _json_dict(raw: Any) -> dict:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
        except Exception:
            return {}
        return parsed if isinstance(parsed, dict) else {}
    return {}


def _author_llm_proposed(queue_data: dict[str, Any], key: str) -> str | None:
    if queue_data.get("author_llm_status") != "processed":
        return None
    llm_field = _AUTHOR_LLM_FIELDS.get(key)
    if not llm_field:
        return None

    result = _json_dict(queue_data.get("author_llm_result"))
    entries = result.get("internal_authors")
    if not isinstance(entries, list):
        return None

    values: list[str] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        raw_value = entry.get(llm_field)
        if raw_value is None:
            values.append("")
            continue
        value = _to_display(raw_value)
        if llm_field == "ou":
            value = _translate_ou(value) or ""
        values.append(value or "")
    if not any(values):
        return None
    return VALUE_SEPARATOR.join(values)


def _date_llm_proposed(queue_data: dict[str, Any], key: str) -> str | None:
    if queue_data.get("date_llm_status") != "processed":
        return None
    llm_field = _DATE_LLM_FIELDS.get(key)
    if not llm_field:
        return None
    result = _json_dict(queue_data.get("date_llm_result"))
    return _to_display(result.get(llm_field))


def _author_source_values(
    resource_id: str,
    main_data: dict[str, Any],
    engine=None,
) -> dict[str, str | None]:
    engine = engine or get_local_engine()
    schema = settings.local_schema

    history_rows: list[dict[str, Any]] = []
    with engine.connect() as conn:
        try:
            rows = conn.execute(text(f"""
                SELECT
                    "utb.source" AS source_arr,
                    "dc.contributor.author" AS authors_arr
                FROM "{schema}"."dedup_histoire"
                WHERE dedup_kept_resource_id = :rid
            """), {"rid": int(resource_id)}).fetchall()
        except Exception:
            rows = []

    for row in rows:
        history_rows.append({
            "sources": row.source_arr,
            "authors": row.authors_arr,
        })

    split = split_source_author_lists(
        current_authors=main_data.get("dc.contributor.author"),
        current_sources=main_data.get("utb.source"),
        history_rows=history_rows,
    )
    return {
        "wos": _to_display(split.get("wos")),
        "scopus": _to_display(split.get("scopus")),
    }


def _author_modal_data(
    main_data: dict[str, Any],
    internal_display: str | None,
) -> list[dict[str, Any]]:
    author_names = _split_display_values(_to_display(main_data.get("dc.contributor.author")))
    internal_keys = {_author_key(name) for name in _split_display_values(internal_display)}

    scopus_aff_by_author: dict[str, str] = {}
    for parsed in parse_scopus_affiliation_array(main_data.get("utb.scopus.affiliation")):
        for block in parsed.blocks:
            if not block.author_name or not block.affiliation:
                continue
            scopus_aff_by_author.setdefault(_author_key(block.author_name), block.affiliation)

    wos_aff_by_author: dict[str, str] = {}
    for parsed in parse_wos_affiliation_array(main_data.get("utb.wos.affiliation")):
        for block in parsed.blocks:
            for author_name in block.authors:
                if not author_name or not block.affiliation_raw:
                    continue
                wos_aff_by_author.setdefault(_author_key(author_name), block.affiliation_raw)

    return [
        {
            "name": author_name,
            "is_internal": _author_key(author_name) in internal_keys,
            "scopus_aff": scopus_aff_by_author.get(_author_key(author_name), ""),
            "wos_aff": wos_aff_by_author.get(_author_key(author_name), ""),
        }
        for author_name in author_names
    ]


def _merged_source_records(resource_id: str, engine=None) -> list[dict[str, Any]]:
    engine = engine or get_local_engine()
    schema = settings.local_schema

    with engine.connect() as conn:
        try:
            rows = conn.execute(text(f"""
                SELECT
                    ctid::text AS history_row_ref,
                    resource_id,
                    dedup_match_type,
                    dedup_match_score,
                    dedup_match_details,
                    dedup_merged_at,
                    dedup_kept_resource_id,
                    dedup_other_resource_id
                FROM "{schema}"."dedup_histoire"
                WHERE dedup_kept_resource_id = :rid
                ORDER BY dedup_merged_at DESC, resource_id ASC
            """), {"rid": int(resource_id)}).mappings().fetchall()
        except Exception:
            rows = []

    merged: list[dict[str, Any]] = []
    seen_resource_ids: set[str] = set()
    for row in rows:
        row_resource_id = str(row["resource_id"])
        if row_resource_id in seen_resource_ids:
            continue
        seen_resource_ids.add(row_resource_id)
        details = _json_dict(row.get("dedup_match_details"))
        score = row.get("dedup_match_score")
        merged.append({
            "history_row_ref": str(row.get("history_row_ref") or ""),
            "resource_id": row_resource_id,
            "kept_resource_id": str(row.get("dedup_kept_resource_id") or ""),
            "other_resource_id": str(row.get("dedup_other_resource_id") or ""),
            "match_type": str(row.get("dedup_match_type") or "unknown"),
            "match_score": float(score) if isinstance(score, (int, float)) else None,
            "details": details,
            "merged_at": _to_display(row.get("dedup_merged_at")),
            "is_kept_original": str(row.get("resource_id")) == str(row.get("dedup_kept_resource_id")),
        })
    return merged


def _get_history_record_row(history_row_ref: str, engine=None) -> dict[str, Any] | None:
    engine = engine or get_local_engine()
    schema = settings.local_schema
    with engine.connect() as conn:
        row = conn.execute(text(f"""
            SELECT ctid::text AS history_row_ref, *
            FROM "{schema}"."dedup_histoire"
            WHERE ctid::text = :row_ref
        """), {"row_ref": history_row_ref}).mappings().fetchone()
    return dict(row) if row else None
