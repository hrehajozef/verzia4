"""Nacitanie dat zaznamu a jeho pipeline vysledkov pre detail stranku."""

from __future__ import annotations

from typing import Any

from sqlalchemy import text

from src.config.settings import settings
from src.db.engines import get_local_engine
from web.services.queue_service_support import (
    AFFILIATION_GROUP_LABEL,
    CHANGE_BUFFER_TABLE,
    QUEUE_FIELDS,
    _AFFILIATION_KEYS,
    _HIDDEN_FIELDS,
    _PROPOSED_FROM_QUEUE,
    _SCOPUS_COL_MAP,
    _WOS_COL_MAP,
    _author_llm_proposed,
    _author_modal_data,
    _author_source_values,
    _date_llm_proposed,
    _display_to_db_value,
    _get_history_record_row,
    _json_dict,
    _load_table_columns,
    _merged_source_records,
    _pending_change_map,
    _resolve_field_target,
    _to_display,
    ensure_change_buffer_table,
    get_detail_row_order,
    get_pending_changes,
    reset_detail_row_order,
    save_detail_row_order,
    PRIORITY_FIELDS,
)
from src.common.constants import QUEUE_TABLE


def _main_display_value(main_data: dict[str, Any], key: str) -> str | None:
    return _to_display(main_data.get(key))


def _build_detail_context(
    *,
    main_data: dict[str, Any],
    queue_data: dict[str, Any],
    main_columns: dict[str, str],
    queue_columns: dict[str, str],
    author_source_values: dict[str, str | None],
    include_queue_fields: bool,
) -> dict[str, Any]:
    return {
        "main_data": main_data,
        "main_keys": list(main_data.keys()),
        "queue_data": queue_data,
        "main_columns": main_columns,
        "queue_columns": queue_columns,
        "author_source_values": author_source_values,
        "validation_suggested_fixes": _json_dict(queue_data.get("validation_suggested_fixes") or {}),
        "include_queue_fields": include_queue_fields,
    }


def _detail_proposed_value(context: dict[str, Any], key: str) -> str | None:
    if not context["include_queue_fields"]:
        return None

    queue_data = context["queue_data"]
    value = _author_llm_proposed(queue_data, key)
    if value is not None:
        return value

    value = _date_llm_proposed(queue_data, key)
    if value is not None:
        return value

    fixes = context["validation_suggested_fixes"]
    if key in fixes:
        value = _to_display(fixes[key].get("suggested"))
        if value is not None:
            return value

    queue_alias = _PROPOSED_FROM_QUEUE.get(key)
    if queue_alias:
        return _to_display(queue_data.get(queue_alias))
    return _to_display(queue_data.get(key))


def _detail_field_exists(context: dict[str, Any], key: str) -> bool:
    if key in context["main_columns"] or key in _AFFILIATION_KEYS:
        return True
    if not context["include_queue_fields"]:
        return False
    return key in context["queue_columns"] or key in QUEUE_FIELDS


def _add_affiliation_group(
    ordered_fields: list[dict[str, Any]],
    seen: set[str],
    context: dict[str, Any],
) -> None:
    for key in _AFFILIATION_KEYS:
        seen.add(key)

    main_data = context["main_data"]
    editable = context["include_queue_fields"]
    ordered_fields.append({
        "key": "utb.fulltext.affiliation",
        "label": AFFILIATION_GROUP_LABEL,
        "main": _to_display(main_data.get("utb.fulltext.affiliation")),
        "proposed": _detail_proposed_value(context, "utb.fulltext.affiliation"),
        "wos_val": _to_display(main_data.get("utb.wos.affiliation")),
        "wos_field_key": "utb.wos.affiliation" if editable else None,
        "scopus_val": _to_display(main_data.get("utb.scopus.affiliation")),
        "scopus_field_key": "utb.scopus.affiliation" if editable else None,
        "is_queue": False,
    })


def _add_detail_field(
    ordered_fields: list[dict[str, Any]],
    seen: set[str],
    key: str,
    context: dict[str, Any],
    *,
    label: str | None = None,
) -> None:
    if key in seen or not _detail_field_exists(context, key):
        return
    if key in _AFFILIATION_KEYS:
        _add_affiliation_group(ordered_fields, seen, context)
        return

    seen.add(key)
    if key in _HIDDEN_FIELDS:
        return

    main_data = context["main_data"]
    editable = context["include_queue_fields"]
    wos_field_key = _WOS_COL_MAP.get(key) if editable else None
    scopus_field_key = _SCOPUS_COL_MAP.get(key) if editable else None
    wos_val = _to_display(main_data.get(wos_field_key)) if wos_field_key else None
    scopus_val = _to_display(main_data.get(scopus_field_key)) if scopus_field_key else None
    if key == "dc.contributor.author":
        wos_val = context["author_source_values"]["wos"]
        scopus_val = context["author_source_values"]["scopus"]
        wos_field_key = None
        scopus_field_key = None

    ordered_fields.append({
        "key": key,
        "label": label or key,
        "main": (
            _to_display(context["queue_data"].get(key))
            if editable and key in QUEUE_FIELDS
            else _main_display_value(main_data, key)
        ),
        "proposed": _detail_proposed_value(context, key),
        "wos_val": wos_val,
        "wos_field_key": wos_field_key,
        "scopus_val": scopus_val,
        "scopus_field_key": scopus_field_key,
        "is_queue": editable and key in QUEUE_FIELDS,
    })


def _build_detail_fields(context: dict[str, Any]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    ordered_fields: list[dict[str, Any]] = []

    for key in get_detail_row_order():
        _add_detail_field(ordered_fields, seen, key, context)

    main_data = context["main_data"]
    main_keys = context["main_keys"]
    non_null = [key for key in main_keys if key not in seen and main_data.get(key) is not None]
    null_keys = [key for key in main_keys if key not in seen and main_data.get(key) is None]
    for key in non_null:
        _add_detail_field(ordered_fields, seen, key, context)
    for key in null_keys:
        _add_detail_field(ordered_fields, seen, key, context)

    if context["include_queue_fields"]:
        for key in QUEUE_FIELDS:
            if key not in seen:
                _add_detail_field(ordered_fields, seen, key, context)

    return ordered_fields


def get_record_detail(resource_id: str, engine=None) -> dict[str, Any] | None:
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

    context = _build_detail_context(
        main_data=effective_main,
        queue_data=effective_queue,
        main_columns=main_columns,
        queue_columns=queue_columns,
        author_source_values=_author_source_values(resource_id, effective_main, engine),
        include_queue_fields=True,
    )

    return {
        "resource_id": resource_id,
        "main": effective_main,
        "queue": effective_queue,
        "fields": _build_detail_fields(context),
        "author_modal_data": _author_modal_data(
            effective_main,
            _detail_proposed_value(context, "utb.contributor.internalauthor"),
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
    main_data = {key: history_row.get(key) for key in main_columns if key in history_row}
    context = _build_detail_context(
        main_data=main_data,
        queue_data={},
        main_columns=main_columns,
        queue_columns={},
        author_source_values=_author_source_values(str(history_row.get("resource_id") or ""), main_data, engine),
        include_queue_fields=False,
    )

    score = history_row.get("dedup_match_score")
    return {
        "resource_id": str(history_row.get("resource_id") or ""),
        "main": main_data,
        "queue": {},
        "fields": _build_detail_fields(context),
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
            "match_score": float(score) if isinstance(score, (int, float)) else None,
            "merged_at": _to_display(history_row.get("dedup_merged_at")),
        },
    }


def save_record_field(resource_id: str, field_key: str, new_value: str, engine=None) -> None:
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

        baseline_old = existing_pending.get("old_value") if existing_pending else old_display
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


def mark_checked(resource_id: str, engine=None) -> None:
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
