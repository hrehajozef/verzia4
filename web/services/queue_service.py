"""Načítanie dát záznamu a jeho pipeline výsledkov pre detail stránku."""

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

_VALID_OU_NAMES: frozenset[str] = frozenset(DEPARTMENTS.keys())


def _translate_ou(v: str | None) -> str | None:
    """Preloží český názov OU na anglický. Neznáme hodnoty ponechá (môžu byť validné anglické)."""
    if not v:
        return v
    if v in _VALID_OU_NAMES:
        return v
    translated = CZECH_DEPARTMENT_MAP_NORM.get(_norm_const(v))
    if translated:
        return translated
    return v
from src.config.settings import settings
from src.db.engines import get_local_engine

VALUE_SEPARATOR = "||"
CHANGE_BUFFER_TABLE = "utb_change_buffer"
DETAIL_ROW_ORDER_PATH = Path("data/detail_row_order.json")


# Prioritné polia – zobrazujú sa vždy na začiatku Metadata stĺpca
PRIORITY_FIELDS = [
    # ── Názov ──
    "dc.title",
    # ── Autori a afiliácia ──
    "dc.contributor.author",
    "utb.contributor.internalauthor",
    "utb.fulltext.affiliation",
    "utb.faculty",
    "utb.ou",
    # ── Dátumy ──
    "dc.date.issued",
    "utb_date_received",
    "utb_date_reviewed",
    "utb_date_accepted",
    "utb_date_published_online",
    "utb_date_published",
    "utb.fulltext.dates",
    # ── Publikácia ──
    "dc.relation.ispartof",
    "dc.publisher",
    "utb.relation.volume",
    "utb.relation.issue",
    "dc.citation.spage",
    "dc.citation.epage",
    # ── Typ ──
    "dc.type",
    # ── Identifikátory ──
    "dc.identifier.doi",
    "dc.identifier.issn",
    "dc.identifier.isbn",
    "utb.identifier.wok",
    "utb.identifier.scopus",
]

# Kľúče, ktoré tvoria grouped "Affiliation" riadok
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

# Poradie afiliačných stĺpcov, ktoré sa vykresľujú v zlúčenom riadku
# "Affiliation". Riadok zobrazuje pod sebou skutočné názvy DB stĺpcov, ktoré
# sa zlučujú do jednej bunky.
_AFFILIATION_LABEL_ORDER: tuple[str, ...] = (
    "utb.fulltext.affiliation",
    "utb.wos.affiliation",
    "utb.scopus.affiliation",
)
AFFILIATION_GROUP_LABEL: str = "\n".join(_AFFILIATION_LABEL_ORDER)

# Pre tieto hlavné fieldy sa "proposed" hodnota berie z príslušného queue stĺpca
# (namiesto zobrazovania queue stĺpca ako samostatného riadku)
_PROPOSED_FROM_QUEUE: dict[str, str] = {
    "dc.contributor.author":          "author_dc_names",
    "utb.contributor.internalauthor": "author_internal_names",
    "dc.publisher":                   "journal_norm_proposed_publisher",
    "dc.relation.ispartof":           "journal_norm_proposed_ispartof",
    "utb.faculty":                    "author_faculty",
    "utb.ou":                         "author_ou",
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

# Polia špecifické pre WoS (zobrazujú sa v WOS stĺpci)
_WOS_COL_MAP: dict[str, str] = {
    # row_key -> wos_field_key
    "utb.fulltext.affiliation": "utb.wos.affiliation",
    "utb.identifier.wok":       "utb.identifier.wok",
}

# Polia špecifické pre Scopus (zobrazujú sa v Scopus stĺpci)
_SCOPUS_COL_MAP: dict[str, str] = {
    # row_key -> scopus_field_key
    "utb.fulltext.affiliation":  "utb.scopus.affiliation",
    "utb.identifier.scopus":     "utb.identifier.scopus",
}

# Interne pipeline polia (z queue tabuľky), nie z hlavnej tabuľky
QUEUE_FIELDS: dict[str, str] = {
    "utb_date_received":              "D\u00e1tum prijatia z\u00e1znamu",
    "utb_date_reviewed":              "D\u00e1tum recenzovania",
    "utb_date_accepted":              "D\u00e1tum prijatia \u010dl\u00e1nku",
    "utb_date_published_online":      "D\u00e1tum online vydania",
    "utb_date_published":             "D\u00e1tum vydania tla\u010dou",
    # validation_suggested_fixes je aplikovan\u00fd per-field inline \u2013 nie ako samostatn\u00fd riadok
}

# Stĺpce, ktoré sa nezobrazujú ako samostatné riadky (raw JSONB bloby a interné flagy)
# Detail page zobrazuje skuto\u010dn\u00e9 DB n\u00e1zvy st\u013apcov ako label v st\u013apci Metadata
# (predt\u00fdm tu bola tabu\u013eka FIELD_LABELS so slovensk\u00fdmi popiskami \u2013 odstr\u00e1nen\u00e9,
# label pre ka\u017ed\u00fd riadok je teraz priamo n\u00e1zov st\u013apca v DB).

_HIDDEN_FIELDS: set[str] = {
    # Tieto queue stĺpce sa zobrazujú inline v príslušných hlavných riadkoch
    # (cez _PROPOSED_FROM_QUEUE), nie ako samostatné riadky
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


def _to_display(val: Any) -> str | None:
    """Prevod hodnoty z DB na zobraziteľný reťazec."""
    if val is None:
        return None
    if isinstance(val, (list, tuple)):
        non_empty = [str(v).strip() for v in val if v is not None and str(v).strip()]
        return VALUE_SEPARATOR.join(non_empty) if non_empty else None
    if isinstance(val, datetime):
        return val.date().isoformat()
    s = str(val).strip()
    return s if s else None


def _split_display_values(value: str | None) -> list[str]:
    if not value:
        return []
    return [v.strip() for v in re.split(r"\s*\|\|\s*", value) if v.strip()]


def get_detail_row_order() -> list[str]:
    """Load configurable detail row order, falling back to the project default."""
    try:
        if DETAIL_ROW_ORDER_PATH.exists():
            data = json.loads(DETAIL_ROW_ORDER_PATH.read_text(encoding="utf-8"))
            if isinstance(data, list):
                values = [str(v).strip() for v in data if str(v).strip()]
                if values:
                    return values
    except Exception:
        pass
    return list(PRIORITY_FIELDS)


def save_detail_row_order(fields: list[str]) -> list[str]:
    """Persist detail row order configured from the UI."""
    cleaned = list(dict.fromkeys(str(f).strip() for f in fields if str(f).strip()))
    if not cleaned:
        cleaned = list(PRIORITY_FIELDS)
    DETAIL_ROW_ORDER_PATH.parent.mkdir(parents=True, exist_ok=True)
    DETAIL_ROW_ORDER_PATH.write_text(
        json.dumps(cleaned, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    return cleaned


def reset_detail_row_order() -> list[str]:
    """Reset detail row order to the default order."""
    if DETAIL_ROW_ORDER_PATH.exists():
        DETAIL_ROW_ORDER_PATH.unlink()
    return list(PRIORITY_FIELDS)


def get_pending_changes(resource_id: str, engine=None) -> list[dict[str, Any]]:
    """Return not-yet-approved change-buffer entries for one record."""
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
        prev = latest.get(key)
        if prev is None or (item.get("id") or 0) > (prev.get("id") or 0):
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
            raise ValueError(f"Stĺpec '{field_key}' neexistuje v tabuľke '{queue_table}'")
        return queue_table, queue_columns[field_key]
    if field_key in main_columns:
        return main_table, main_columns[field_key]
    if field_key in queue_columns:
        return queue_table, queue_columns[field_key]
    raise ValueError(f"Stĺpec '{field_key}' neexistuje v tabuľke '{main_table}' ani '{queue_table}'")


def _display_to_db_value(new_value: str, udt_name: str) -> Any:
    val_stripped = new_value.strip() if new_value else ""
    if udt_name.startswith("_"):
        parts = _split_display_values(val_stripped)
        return parts if parts else []
    return val_stripped if val_stripped else None


def _json_dict(raw: Any) -> dict:
    """Best-effort JSONB/TEXT value to dict."""
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
    """Explicit LLM proposal for author-related UI fields, if the LLM returned one."""
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
    """Explicit LLM proposal for one date field, if present."""
    if queue_data.get("date_llm_status") != "processed":
        return None
    llm_field = _DATE_LLM_FIELDS.get(key)
    if not llm_field:
        return None
    result = _json_dict(queue_data.get("date_llm_result"))
    return _to_display(result.get(llm_field))


def _main_display_value(main_data: dict[str, Any], key: str) -> str | None:
    return _to_display(main_data.get(key))


def _author_source_values(
    resource_id: str,
    main_data: dict[str, Any],
    engine=None,
) -> dict[str, str | None]:
    """Resolve source-specific author lists for the detail page author row."""
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
    """Vrati prehlad povodnych zaznamov, ktore vstupili do zl?ceneho zaznamu."""
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
    """Na??ta jeden konkr?tny historick? riadok z dedup_histoire pod?a ctid."""
    engine = engine or get_local_engine()
    schema = settings.local_schema
    with engine.connect() as conn:
        row = conn.execute(text(f"""
            SELECT ctid::text AS history_row_ref, *
            FROM "{schema}"."dedup_histoire"
            WHERE ctid::text = :row_ref
        """), {"row_ref": history_row_ref}).mappings().fetchone()
    return dict(row) if row else None


def get_record_detail(resource_id: str, engine=None) -> dict[str, Any] | None:
    """
    Načíta kompletné dáta záznamu pre detail stránku.

    Vráti dict s:
      - main: všetky stĺpce z utb_metadata_arr (pôvodné dáta)
      - queue: všetky stĺpce z utb_processing_queue (pipeline výsledky)
      - fields: zoradený zoznam field dict-ov pre tabuľku
    """
    engine = engine or get_local_engine()
    schema = settings.local_schema
    table  = settings.local_table
    queue  = QUEUE_TABLE
    ensure_change_buffer_table(engine)

    with engine.connect() as conn:
        rid = int(resource_id)
        main_row = conn.execute(text(f"""
            SELECT * FROM "{schema}"."{table}"
            WHERE resource_id = :rid
        """), {"rid": rid}).mappings().fetchone()

        queue_row = conn.execute(text(f"""
            SELECT * FROM "{schema}"."{queue}"
            WHERE resource_id = :rid
        """), {"rid": rid}).mappings().fetchone()

    if not main_row:
        return None

    main_data  = dict(main_row)
    queue_data = dict(queue_row) if queue_row else {}
    author_source_values = _author_source_values(resource_id, main_data, engine)

    # Parsovanie validation_suggested_fixes pre inline návrhy
    vsf = _json_dict(queue_data.get("validation_suggested_fixes") or {})

    def _get_proposed(key: str) -> str | None:
        """Navrhnutá hodnota: najprv z vsf, potom queue alias, potom priamo z queue_data."""
        # LLM ma prednost pred validacnymi a heuristickymi navrhmi.
        v = _author_llm_proposed(queue_data, key)
        if v is not None:
            return v
        v = _date_llm_proposed(queue_data, key)
        if v is not None:
            return v
        if key in vsf:
            fix = vsf[key]
            v = _to_display(fix.get("suggested"))
            if v is not None:
                return v
        v = _author_llm_proposed(queue_data, key)
        if v is not None:
            return v
        v = _date_llm_proposed(queue_data, key)
        if v is not None:
            return v
        # Pre niektoré hlavné fieldy sa proposed berie z dedikovaného queue stĺpca
        queue_alias = _PROPOSED_FROM_QUEUE.get(key)
        if queue_alias:
            v = _to_display(queue_data.get(queue_alias))
            if v is not None:
                return v
            return None
        return _to_display(queue_data.get(key))

    seen: set[str] = set()
    ordered_fields: list[dict] = []
    _affiliation_added = False

    def _add_affiliation_group() -> None:
        nonlocal _affiliation_added
        if _affiliation_added:
            return
        _affiliation_added = True
        for k in _AFFILIATION_KEYS:
            seen.add(k)

        main_val     = _to_display(main_data.get("utb.fulltext.affiliation"))
        proposed_val = _get_proposed("utb.fulltext.affiliation")

        ordered_fields.append({
            "key":             "utb.fulltext.affiliation",
            "label":           AFFILIATION_GROUP_LABEL,
            "main":            main_val,
            "proposed":        proposed_val,
            "wos_val":         _to_display(main_data.get("utb.wos.affiliation")),
            "wos_field_key":   "utb.wos.affiliation",
            "scopus_val":      _to_display(main_data.get("utb.scopus.affiliation")),
            "scopus_field_key":"utb.scopus.affiliation",
            "is_queue":        False,
        })

    def _add_field(key: str, label: str | None = None) -> None:
        if key in seen:
            return

        # Affiliation kľúče sa zlučujú do jedného grouped riadku
        if key in _AFFILIATION_KEYS:
            _add_affiliation_group()
            return

        seen.add(key)

        if key in _HIDDEN_FIELDS:
            return

        main_val     = _main_display_value(main_data, key)
        proposed_val = _get_proposed(key)

        wos_fk    = _WOS_COL_MAP.get(key)
        scopus_fk = _SCOPUS_COL_MAP.get(key)
        wos_val = _to_display(main_data.get(wos_fk)) if wos_fk else None
        scopus_val = _to_display(main_data.get(scopus_fk)) if scopus_fk else None

        if key == "dc.contributor.author":
            wos_val = author_source_values["wos"]
            scopus_val = author_source_values["scopus"]
            wos_fk = None
            scopus_fk = None

        ordered_fields.append({
            "key":             key,
            "label":           label or key,
            "main":            main_val,
            "proposed":        proposed_val,
            "wos_val":         wos_val,
            "wos_field_key":   wos_fk,
            "scopus_val":      scopus_val,
            "scopus_field_key":scopus_fk,
            "is_queue":        key in QUEUE_FIELDS,
        })

    # 1. Prioritné polia
    for key in get_detail_row_order():
        _add_field(key)

    # 2. Zvyšné polia z main – najprv neprázdne, potom NULL
    all_main_keys = list(main_data.keys())
    non_null = [k for k in all_main_keys if k not in seen and main_data.get(k) is not None]
    null_keys = [k for k in all_main_keys if k not in seen and main_data.get(k) is None]

    for key in non_null:
        _add_field(key)
    for key in null_keys:
        _add_field(key)

    # 3. Queue polia s návrhmi (tie, kde je queue hodnota alebo vsf návrh)
    for key in QUEUE_FIELDS:
        if key not in seen:
            _add_field(key)

    return {
        "resource_id": resource_id,
        "main":        main_data,
        "queue":       queue_data,
        "fields":      ordered_fields,
        "checked_at":  queue_data.get("librarian_checked_at"),
        "pending_changes": get_pending_changes(resource_id, engine),
    }


def ensure_change_buffer_table(engine=None) -> None:
    """Create the pending-change audit table used by the librarian workflow."""
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


def save_record_field(resource_id: str, field_key: str, new_value: str, engine=None) -> None:
    """
    Uloží zmenenú hodnotu pre jeden field záznamu.

    field_key môže byť:
      - bežný kľúč (dc.title) → uloží do main table
      - queue kľúč (author_faculty) → uloží do queue table
    """
    engine = engine or get_local_engine()
    schema = settings.local_schema
    table  = settings.local_table
    queue  = QUEUE_TABLE

    target_table = queue if field_key in QUEUE_FIELDS else table

    # Zistíme typ stĺpca (array vs. scalar)
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT udt_name FROM information_schema.columns
            WHERE table_schema = :s AND table_name = :t AND column_name = :c
        """), {"s": schema, "t": target_table, "c": field_key}).fetchone()

    if row is None:
        raise ValueError(f"Stĺpec '{field_key}' neexistuje v tabuľke '{target_table}'")

    udt_name = row[0]
    is_array = udt_name.startswith("_")

    val_stripped = new_value.strip() if new_value else ""

    if is_array:
        parts = _split_display_values(val_stripped)
        db_value: Any = parts if parts else []
    else:
        db_value = val_stripped if val_stripped else None

    rid_int = int(resource_id)

    with engine.begin() as conn:
        old_value = conn.execute(text(f"""
            SELECT "{field_key}"
            FROM "{schema}"."{target_table}"
            WHERE resource_id = :rid
        """), {"rid": rid_int}).scalar()
        old_display = _to_display(old_value)
        new_display = _to_display(db_value)

        if old_display != new_display:
            conn.execute(text(f"""
                INSERT INTO "{schema}"."{CHANGE_BUFFER_TABLE}"
                    (resource_id, field_key, target_table, old_value, new_value)
                VALUES (:rid, :field_key, :target_table, :old_value, :new_value)
            """), {
                "rid": rid_int,
                "field_key": field_key,
                "target_table": target_table,
                "old_value": old_display,
                "new_value": new_display,
            })

        conn.execute(text(f"""
            UPDATE "{schema}"."{target_table}"
            SET "{field_key}" = :val
            WHERE resource_id = :rid
        """), {"val": db_value, "rid": rid_int})

        # Always stamp the queue row so we can show "pending changes" on home page
        conn.execute(text(f"""
            UPDATE "{schema}"."{queue}"
            SET librarian_modified_at = now()
            WHERE resource_id = :rid
        """), {"rid": rid_int})


def mark_checked(resource_id: str, engine=None) -> None:
    """Označí záznam ako skontrolovaný (pridá timestamp do librarian_checked_at)."""
    engine = engine or get_local_engine()
    schema = settings.local_schema
    queue  = QUEUE_TABLE
    ensure_change_buffer_table(engine)

    with engine.begin() as conn:
        conn.execute(text(f"""
            UPDATE "{schema}"."{queue}"
            SET librarian_checked_at = array_append(
                COALESCE(librarian_checked_at, ARRAY[]::TIMESTAMPTZ[]),
                now()
            ),
            updated_at = now()
            WHERE resource_id = :rid
        """), {"rid": int(resource_id)})
        conn.execute(text(f"""
            UPDATE "{schema}"."{CHANGE_BUFFER_TABLE}"
            SET approved_at = now()
            WHERE resource_id = :rid
              AND approved_at IS NULL
              AND discarded_at IS NULL
        """), {"rid": int(resource_id)})


def _get_record_detail_v2(resource_id: str, engine=None) -> dict[str, Any] | None:
    """Load detail data with active stack changes rendered directly in cells."""
    engine = engine or get_local_engine()
    schema = settings.local_schema
    table = settings.local_table
    queue = QUEUE_TABLE
    ensure_change_buffer_table(engine)

    main_columns = _load_table_columns(engine, schema, table)
    queue_columns = _load_table_columns(engine, schema, queue)

    with engine.connect() as conn:
        rid = int(resource_id)
        main_row = conn.execute(text(f"""
            SELECT * FROM "{schema}"."{table}"
            WHERE resource_id = :rid
        """), {"rid": rid}).mappings().fetchone()
        queue_row = conn.execute(text(f"""
            SELECT * FROM "{schema}"."{queue}"
            WHERE resource_id = :rid
        """), {"rid": rid}).mappings().fetchone()
        pending_rows = conn.execute(text(f"""
            SELECT id, field_key, target_table, old_value, new_value, created_at
            FROM "{schema}"."{CHANGE_BUFFER_TABLE}"
            WHERE resource_id = :rid
              AND approved_at IS NULL
              AND discarded_at IS NULL
            ORDER BY created_at DESC, id DESC
        """), {"rid": rid}).mappings().fetchall()

    if not main_row:
        return None

    main_data = dict(main_row)
    queue_data = dict(queue_row) if queue_row else {}
    pending_changes = [dict(row) for row in pending_rows]
    pending_map = _pending_change_map(pending_changes)
    merged_sources = _merged_source_records(resource_id, engine)

    effective_main = dict(main_data)
    effective_queue = dict(queue_data)
    for (field_key, target_table), change in pending_map.items():
        if target_table == table:
            effective_main[field_key] = change.get("new_value")
        elif target_table == queue:
            effective_queue[field_key] = change.get("new_value")

    author_source_values = _author_source_values(resource_id, effective_main, engine)
    vsf = _json_dict(effective_queue.get("validation_suggested_fixes") or {})

    def _get_proposed(key: str) -> str | None:
        value = _author_llm_proposed(effective_queue, key)
        if value is not None:
            return value
        value = _date_llm_proposed(effective_queue, key)
        if value is not None:
            return value
        if key in vsf:
            fix = vsf[key]
            value = _to_display(fix.get("suggested"))
            if value is not None:
                return value
        queue_alias = _PROPOSED_FROM_QUEUE.get(key)
        if queue_alias:
            return _to_display(effective_queue.get(queue_alias))
        return _to_display(effective_queue.get(key))

    def _field_exists(key: str) -> bool:
        return key in main_columns or key in queue_columns or key in _AFFILIATION_KEYS or key in QUEUE_FIELDS

    seen: set[str] = set()
    ordered_fields: list[dict] = []
    affiliation_added = False

    def _add_affiliation_group() -> None:
        nonlocal affiliation_added
        if affiliation_added:
            return
        affiliation_added = True
        for item in _AFFILIATION_KEYS:
            seen.add(item)
        ordered_fields.append({
            "key": "utb.fulltext.affiliation",
            "label": AFFILIATION_GROUP_LABEL,
            "main": _to_display(effective_main.get("utb.fulltext.affiliation")),
            "proposed": _get_proposed("utb.fulltext.affiliation"),
            "wos_val": _to_display(effective_main.get("utb.wos.affiliation")),
            "wos_field_key": "utb.wos.affiliation",
            "scopus_val": _to_display(effective_main.get("utb.scopus.affiliation")),
            "scopus_field_key": "utb.scopus.affiliation",
            "is_queue": False,
        })

    def _add_field(key: str, label: str | None = None) -> None:
        if key in seen or not _field_exists(key):
            return
        if key in _AFFILIATION_KEYS:
            _add_affiliation_group()
            return
        seen.add(key)
        if key in _HIDDEN_FIELDS:
            return

        wos_fk = _WOS_COL_MAP.get(key)
        scopus_fk = _SCOPUS_COL_MAP.get(key)
        wos_val = _to_display(effective_main.get(wos_fk)) if wos_fk else None
        scopus_val = _to_display(effective_main.get(scopus_fk)) if scopus_fk else None
        if key == "dc.contributor.author":
            wos_val = author_source_values["wos"]
            scopus_val = author_source_values["scopus"]
            wos_fk = None
            scopus_fk = None

        ordered_fields.append({
            "key": key,
            "label": label or key,
            "main": _to_display(effective_queue.get(key)) if key in QUEUE_FIELDS else _main_display_value(effective_main, key),
            "proposed": _get_proposed(key),
            "wos_val": wos_val,
            "wos_field_key": wos_fk,
            "scopus_val": scopus_val,
            "scopus_field_key": scopus_fk,
            "is_queue": key in QUEUE_FIELDS,
        })

    for key in get_detail_row_order():
        _add_field(key)

    all_main_keys = list(main_data.keys())
    non_null = [key for key in all_main_keys if key not in seen and effective_main.get(key) is not None]
    null_keys = [key for key in all_main_keys if key not in seen and effective_main.get(key) is None]
    for key in non_null:
        _add_field(key)
    for key in null_keys:
        _add_field(key)

    for key in QUEUE_FIELDS:
        if key not in seen:
            _add_field(key)

    return {
        "resource_id": resource_id,
        "main": effective_main,
        "queue": effective_queue,
        "fields": ordered_fields,
        "author_modal_data": _author_modal_data(
            effective_main,
            _get_proposed("utb.contributor.internalauthor"),
        ),
        "checked_at": effective_queue.get("librarian_checked_at"),
        "pending_changes": pending_changes,
        "merged_sources": merged_sources,
        "read_only": False,
        "is_history": False,
    }


def get_history_record_detail(history_row_ref: str, engine=None) -> dict[str, Any] | None:
    """Load read-only detail for one archived row in dedup_histoire."""
    engine = engine or get_local_engine()
    schema = settings.local_schema
    history_row = _get_history_record_row(history_row_ref, engine=engine)
    if not history_row:
        return None

    main_columns = _load_table_columns(engine, schema, settings.local_table)
    main_data = {key: history_row.get(key) for key in main_columns.keys() if key in history_row}
    author_source_values = _author_source_values(str(history_row.get("resource_id") or ""), main_data, engine)

    seen: set[str] = set()
    ordered_fields: list[dict] = []
    affiliation_added = False

    def _field_exists(key: str) -> bool:
        return key in main_columns or key in _AFFILIATION_KEYS

    def _add_affiliation_group() -> None:
        nonlocal affiliation_added
        if affiliation_added:
            return
        affiliation_added = True
        for item in _AFFILIATION_KEYS:
            seen.add(item)
        ordered_fields.append({
            "key": "utb.fulltext.affiliation",
            "label": AFFILIATION_GROUP_LABEL,
            "main": _to_display(main_data.get("utb.fulltext.affiliation")),
            "proposed": None,
            "wos_val": _to_display(main_data.get("utb.wos.affiliation")),
            "wos_field_key": None,
            "scopus_val": _to_display(main_data.get("utb.scopus.affiliation")),
            "scopus_field_key": None,
            "is_queue": False,
        })

    def _add_field(key: str, label: str | None = None) -> None:
        if key in seen or not _field_exists(key):
            return
        if key in _AFFILIATION_KEYS:
            _add_affiliation_group()
            return
        seen.add(key)
        if key in _HIDDEN_FIELDS:
            return

        wos_fk = _WOS_COL_MAP.get(key)
        scopus_fk = _SCOPUS_COL_MAP.get(key)
        wos_val = _to_display(main_data.get(wos_fk)) if wos_fk else None
        scopus_val = _to_display(main_data.get(scopus_fk)) if scopus_fk else None
        if key == "dc.contributor.author":
            wos_val = author_source_values["wos"]
            scopus_val = author_source_values["scopus"]

        ordered_fields.append({
            "key": key,
            "label": label or key,
            "main": _main_display_value(main_data, key),
            "proposed": None,
            "wos_val": wos_val,
            "wos_field_key": None,
            "scopus_val": scopus_val,
            "scopus_field_key": None,
            "is_queue": False,
        })

    for key in get_detail_row_order():
        _add_field(key)

    all_main_keys = list(main_data.keys())
    non_null = [key for key in all_main_keys if key not in seen and main_data.get(key) is not None]
    null_keys = [key for key in all_main_keys if key not in seen and main_data.get(key) is None]
    for key in non_null:
        _add_field(key)
    for key in null_keys:
        _add_field(key)

    return {
        "resource_id": str(history_row.get("resource_id") or ""),
        "main": main_data,
        "queue": {},
        "fields": ordered_fields,
        "author_modal_data": _author_modal_data(
            main_data,
            _to_display(main_data.get("utb.contributor.internalauthor")),
        ),
        "checked_at": None,
        "pending_changes": [],
        "merged_sources": [],
        "read_only": True,
        "is_history": True,
        "history_row_ref": str(history_row.get("history_row_ref") or ""),
        "history_info": {
            "kept_resource_id": str(history_row.get("dedup_kept_resource_id") or ""),
            "other_resource_id": str(history_row.get("dedup_other_resource_id") or ""),
            "match_type": str(history_row.get("dedup_match_type") or "unknown"),
            "match_score": float(history_row["dedup_match_score"]) if isinstance(history_row.get("dedup_match_score"), (int, float)) else None,
            "merged_at": _to_display(history_row.get("dedup_merged_at")),
        },
    }


def _save_record_field_v2(resource_id: str, field_key: str, new_value: str, engine=None) -> None:
    """Save a change only into the approval stack; do not write the target table yet."""
    engine = engine or get_local_engine()
    schema = settings.local_schema
    table = settings.local_table
    queue = QUEUE_TABLE
    ensure_change_buffer_table(engine)

    main_columns = _load_table_columns(engine, schema, table)
    queue_columns = _load_table_columns(engine, schema, queue)
    target_table, udt_name = _resolve_field_target(field_key, main_columns, queue_columns, table, queue)
    db_value = _display_to_db_value(new_value, udt_name)
    new_display = _to_display(db_value)
    rid_int = int(resource_id)

    with engine.begin() as conn:
        current_value = conn.execute(text(f"""
            SELECT "{field_key}"
            FROM "{schema}"."{target_table}"
            WHERE resource_id = :rid
        """), {"rid": rid_int}).scalar()
        old_display = _to_display(current_value)

        existing_pending = conn.execute(text(f"""
            SELECT id, old_value, new_value
            FROM "{schema}"."{CHANGE_BUFFER_TABLE}"
            WHERE resource_id = :rid
              AND field_key = :field_key
              AND target_table = :target_table
              AND approved_at IS NULL
              AND discarded_at IS NULL
            ORDER BY id DESC
            LIMIT 1
        """), {
            "rid": rid_int,
            "field_key": field_key,
            "target_table": target_table,
        }).mappings().fetchone()

        baseline_old = (existing_pending.get("old_value") if existing_pending else old_display)
        if baseline_old == new_display:
            if existing_pending:
                conn.execute(text(f"""
                    UPDATE "{schema}"."{CHANGE_BUFFER_TABLE}"
                    SET discarded_at = now()
                    WHERE id = :id
                """), {"id": int(existing_pending["id"])})
        elif existing_pending:
            conn.execute(text(f"""
                UPDATE "{schema}"."{CHANGE_BUFFER_TABLE}"
                SET new_value = :new_value,
                    created_at = now()
                WHERE id = :id
            """), {
                "new_value": new_display,
                "id": int(existing_pending["id"]),
            })
        else:
            conn.execute(text(f"""
                INSERT INTO "{schema}"."{CHANGE_BUFFER_TABLE}"
                    (resource_id, field_key, target_table, old_value, new_value)
                VALUES (:rid, :field_key, :target_table, :old_value, :new_value)
            """), {
                "rid": rid_int,
                "field_key": field_key,
                "target_table": target_table,
                "old_value": old_display,
                "new_value": new_display,
            })

        conn.execute(text(f"""
            UPDATE "{schema}"."{queue}"
            SET librarian_modified_at = now()
            WHERE resource_id = :rid
        """), {"rid": rid_int})


def _mark_checked_v2(resource_id: str, engine=None) -> None:
    """Apply stacked changes to the real tables and then mark the record as approved."""
    engine = engine or get_local_engine()
    schema = settings.local_schema
    table = settings.local_table
    queue = QUEUE_TABLE
    ensure_change_buffer_table(engine)

    main_columns = _load_table_columns(engine, schema, table)
    queue_columns = _load_table_columns(engine, schema, queue)
    rid_int = int(resource_id)

    with engine.begin() as conn:
        pending_rows = conn.execute(text(f"""
            SELECT id, field_key, target_table, new_value
            FROM "{schema}"."{CHANGE_BUFFER_TABLE}"
            WHERE resource_id = :rid
              AND approved_at IS NULL
              AND discarded_at IS NULL
            ORDER BY id ASC
        """), {"rid": rid_int}).mappings().fetchall()

        latest_pending = _pending_change_map([dict(row) for row in pending_rows])
        for (field_key, target_table), change in latest_pending.items():
            _, udt_name = _resolve_field_target(field_key, main_columns, queue_columns, table, queue)
            db_value = _display_to_db_value(str(change.get("new_value") or ""), udt_name)
            conn.execute(text(f"""
                UPDATE "{schema}"."{target_table}"
                SET "{field_key}" = :val
                WHERE resource_id = :rid
            """), {"val": db_value, "rid": rid_int})

        conn.execute(text(f"""
            UPDATE "{schema}"."{queue}"
            SET librarian_checked_at = array_append(
                COALESCE(librarian_checked_at, ARRAY[]::TIMESTAMPTZ[]),
                now()
            ),
            updated_at = now(),
            librarian_modified_at = now()
            WHERE resource_id = :rid
        """), {"rid": rid_int})
        conn.execute(text(f"""
            UPDATE "{schema}"."{CHANGE_BUFFER_TABLE}"
            SET approved_at = now()
            WHERE resource_id = :rid
              AND approved_at IS NULL
              AND discarded_at IS NULL
        """), {"rid": rid_int})


get_record_detail = _get_record_detail_v2
save_record_field = _save_record_field_v2
mark_checked = _mark_checked_v2
