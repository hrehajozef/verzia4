"""Načítanie dát záznamu a jeho pipeline výsledkov pre detail stránku."""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

from sqlalchemy import text

from src.common.constants import QUEUE_TABLE
from src.config.settings import settings
from src.db.engines import get_local_engine


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
_AFFILIATION_KEYS = {"utb.wos.affiliation", "utb.scopus.affiliation", "utb.fulltext.affiliation"}

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
    "utb_date_received":              "Received",
    "utb_date_reviewed":              "Reviewed",
    "utb_date_accepted":              "Accepted",
    "utb_date_published_online":      "Published online",
    "utb_date_published":             "Published",
    # validation_suggested_fixes je aplikovaný per-field inline – nie ako samostatný riadok
}

# Stĺpce, ktoré sa nezobrazujú ako samostatné riadky (raw JSONB bloby a interné flagy)
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
        return " || ".join(non_empty) if non_empty else None
    if isinstance(val, datetime):
        return val.date().isoformat()
    s = str(val).strip()
    return s if s else None


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
        value = _to_display(entry.get(llm_field))
        if value:
            values.append(value)
    deduped = list(dict.fromkeys(values))
    return " || ".join(deduped) if deduped else None


def _date_llm_proposed(queue_data: dict[str, Any], key: str) -> str | None:
    """Explicit LLM proposal for one date field, if present."""
    if queue_data.get("date_llm_status") != "processed":
        return None
    llm_field = _DATE_LLM_FIELDS.get(key)
    if not llm_field:
        return None
    result = _json_dict(queue_data.get("date_llm_result"))
    return _to_display(result.get(llm_field))


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
            "label":           "Affiliation",
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

        main_val     = _to_display(main_data.get(key))
        proposed_val = _get_proposed(key)

        wos_fk    = _WOS_COL_MAP.get(key)
        scopus_fk = _SCOPUS_COL_MAP.get(key)

        ordered_fields.append({
            "key":             key,
            "label":           label or key,
            "main":            main_val,
            "proposed":        proposed_val,
            "wos_val":         _to_display(main_data.get(wos_fk)) if wos_fk else None,
            "wos_field_key":   wos_fk,
            "scopus_val":      _to_display(main_data.get(scopus_fk)) if scopus_fk else None,
            "scopus_field_key":scopus_fk,
            "is_queue":        key in QUEUE_FIELDS,
        })

    # 1. Prioritné polia
    for key in PRIORITY_FIELDS:
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
    for key, label in QUEUE_FIELDS.items():
        if key not in seen:
            _add_field(key, label)

    return {
        "resource_id": resource_id,
        "main":        main_data,
        "queue":       queue_data,
        "fields":      ordered_fields,
        "checked_at":  queue_data.get("librarian_checked_at"),
    }


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
        parts = [v.strip() for v in val_stripped.split(" || ") if v.strip()]
        db_value: Any = parts if parts else []
    else:
        db_value = val_stripped if val_stripped else None

    rid_int = int(resource_id)

    with engine.begin() as conn:
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
