"""Podporne helpery pre batch a reporting heuristik autorov."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.common.constants import QUEUE_TABLE
from src.config.settings import settings
from src.db.engines import get_local_engine
from src.authors.source_authors import split_source_author_lists


def build_source_author_map(
    engine: Engine,
    rows: list,
) -> dict[int, dict[str, list[str]]]:
    if not rows:
        return {}

    schema = settings.local_schema
    record_ids = [int(row.resource_id) for row in rows]
    current_rows = {
        int(row.resource_id): {
            "authors": list(row.dc_authors) if row.dc_authors else [],
            "sources": list(row.source_arr) if getattr(row, "source_arr", None) else [],
        }
        for row in rows
    }

    history_map: dict[int, list[dict[str, object]]] = {rid: [] for rid in record_ids}
    with engine.connect() as conn:
        try:
            history_rows = conn.execute(text(f"""
                SELECT
                    dedup_kept_resource_id,
                    "utb.source" AS source_arr,
                    "dc.contributor.author" AS authors_arr
                FROM "{schema}"."dedup_histoire"
                WHERE dedup_kept_resource_id = ANY(:ids)
            """), {"ids": record_ids}).fetchall()
        except Exception:
            history_rows = []

    for row in history_rows:
        kept_id = int(row.dedup_kept_resource_id)
        history_map.setdefault(kept_id, []).append({
            "sources": row.source_arr,
            "authors": row.authors_arr,
        })

    return {
        rid: split_source_author_lists(
            current_authors=current_rows[rid]["authors"],
            current_sources=current_rows[rid]["sources"],
            history_rows=history_map.get(rid, []),
        )
        for rid in record_ids
    }


def _norm_name_set(names: list[str] | None) -> set[str]:
    from src.authors.registry import _normalize_name

    if not names:
        return set()
    return {_normalize_name(name) for name in names if name and name.strip()}


def compare_with_librarian(engine: Engine | None = None) -> None:
    """Porovna author_internal_names vs utb.contributor.internalauthor."""
    engine = engine or get_local_engine()
    schema = settings.local_schema
    table = settings.local_table
    queue = QUEUE_TABLE

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT q.resource_id,
                   q.author_internal_names AS prog,
                   m."utb.contributor.internalauthor" AS lib
            FROM "{schema}"."{queue}" q
            JOIN "{schema}"."{table}" m ON q.resource_id = m.resource_id
            WHERE q.author_heuristic_status = 'processed'
        """)).fetchall()

    cats: dict[str, int] = {
        "exact": 0,
        "partial": 0,
        "no_overlap": 0,
        "only_prog": 0,
        "only_lib": 0,
        "both_empty": 0,
    }
    total = len(rows)

    for row in rows:
        prog = _norm_name_set(row.prog)
        lib = _norm_name_set(row.lib)

        if not prog and not lib:
            cats["both_empty"] += 1
        elif prog and not lib:
            cats["only_prog"] += 1
        elif lib and not prog:
            cats["only_lib"] += 1
        elif prog == lib:
            cats["exact"] += 1
        elif prog & lib:
            cats["partial"] += 1
        else:
            cats["no_overlap"] += 1

    matched = cats["exact"] + cats["partial"]
    print(f"Spracovanych zaznamov (heuristic_status=processed): {total}")
    print()
    print(f"  Presna zhoda (exact):          {cats['exact']:>6}  ({100*cats['exact']/total:.1f}%)" if total else "")
    print(f"  Ciastocna zhoda (partial):     {cats['partial']:>6}  ({100*cats['partial']/total:.1f}%)" if total else "")
    print(f"  Bez prieniku (no_overlap):     {cats['no_overlap']:>6}  ({100*cats['no_overlap']/total:.1f}%)" if total else "")
    print(f"  Len program (only_prog):       {cats['only_prog']:>6}  ({100*cats['only_prog']/total:.1f}%)" if total else "")
    print(f"  Len knihovnik (only_lib):      {cats['only_lib']:>6}  ({100*cats['only_lib']/total:.1f}%)" if total else "")
    print(f"  Oba prazdne (both_empty):      {cats['both_empty']:>6}  ({100*cats['both_empty']/total:.1f}%)" if total else "")
    print()
    if total:
        print(f"  Celkom zhodnych (exact+partial): {matched} / {total}  ({100*matched/total:.1f}%)")
