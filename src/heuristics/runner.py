"""Heuristický runner – spracuje záznamy z lokálnej DB tabuľky.

Výstupné stĺpce (TEXT[] PostgreSQL polia):
  dc_contributor_author          – všetci autori z dc.contributor.author
  utb_contributor_internalauthor – interní UTB autori
  utb_faculty                    – fakulty interných autorov
  utb_ou                         – oddelenia/ústavy interných autorov
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.authors.internal import InternalAuthor, get_author_registry, match_author
from src.common.constants import (
    DEPT_KEYWORD_MAP,
    FACULTIES,
    FACULTY_KEYWORD_RULES,
    FlagKey,
    HeuristicStatus,
)
from src.config.settings import settings
from src.db.engines import get_local_engine
from src.parsers.wos_affiliation import (
    extract_ou_candidates,
    normalize_text,
    parse_wos_affiliation,
)

HEURISTIC_VERSION = "3.0.0"


# -----------------------------------------------------------------------
# Faculty / OU matching
# -----------------------------------------------------------------------

def resolve_faculty_and_ou(affiliation_text: str) -> tuple[str, str]:
    """
    Z textu afiliácie vráti (faculty_id, plný_názov_oddelenia).

    Postup:
    1. Hľadá zhodu oddelenia v DEPT_KEYWORD_MAP (normalizovaný substring match).
    2. Ak nenájde, použije FACULTY_KEYWORD_RULES (heuristický fallback).
    """
    norm = normalize_text(affiliation_text)

    # Krok 1: presná zhoda oddelenia podľa kľúčového slova
    best_dept: str = ""
    best_fid:  str = ""
    best_len:  int = 0

    for keyword, (dept_name, fid) in DEPT_KEYWORD_MAP.items():
        if keyword in norm and len(keyword) > best_len:
            best_len  = len(keyword)
            best_dept = dept_name
            best_fid  = fid

    if best_fid:
        return FACULTIES.get(best_fid, best_fid), best_dept

    # Krok 2: fallback cez FACULTY_KEYWORD_RULES
    for keywords, fid in FACULTY_KEYWORD_RULES:
        if any(kw in norm for kw in keywords):
            return FACULTIES.get(fid, fid), ""

    return "", ""


# -----------------------------------------------------------------------
# Spracovanie jedného záznamu
# -----------------------------------------------------------------------

def process_record(
    resource_id:     int,
    wos_aff_arr:     list[str] | None,
    dc_authors_arr:  list[str] | None,
    registry:        list[InternalAuthor],
) -> dict:
    """
    Spracuje jeden záznam a vráti slovník pripravený na DB UPDATE.

    Parametre:
      resource_id    – PK záznamu
      wos_aff_arr    – pole z utb.wos.affiliation
      dc_authors_arr – pole z dc.contributor.author (kopírujeme do dc_contributor_author)
      registry       – register interných autorov
    """
    result: dict = {
        "resource_id":                     resource_id,
        "heuristic_status":                HeuristicStatus.ERROR,
        "heuristic_version":               HEURISTIC_VERSION,
        "heuristic_processed_at":          datetime.now(timezone.utc),
        "needs_llm":                       False,
        "dc_contributor_author":           list(dc_authors_arr) if dc_authors_arr else None,
        "utb_contributor_internalauthor":  None,
        "utb_faculty":                     None,
        "utb_ou":                          None,
        "flags":                           {},
    }

    try:
        if not wos_aff_arr:
            result["heuristic_status"] = HeuristicStatus.PROCESSED
            result["flags"] = {FlagKey.NO_WOS_DATA: True}
            return result

        matched_authors:   list[str] = []
        matched_faculties: list[str] = []
        matched_ous:       list[str] = []
        unmatched_utb:     list[str] = []
        warnings:          list[str] = []
        needs_llm:         bool      = False
        seen_authors:      set[str]  = set()

        for raw_aff in wos_aff_arr:
            if not raw_aff:
                continue
            parsed = parse_wos_affiliation(str(raw_aff), resource_id=resource_id)
            warnings.extend(parsed.warnings)
            if not parsed.ok:
                needs_llm = True

            for block in parsed.utb_blocks:
                faculty, ou = resolve_faculty_and_ou(block.affiliation_raw)

                # Ak oddelenie nie je nájdené, skús z extract_ou_candidates
                if not ou:
                    candidates = extract_ou_candidates(block.affiliation_raw)
                    ou = candidates[0] if candidates else ""

                for author in block.authors:
                    norm_author = normalize_text(author)
                    if norm_author in seen_authors:
                        continue
                    seen_authors.add(norm_author)

                    match = match_author(author, registry, settings.author_match_threshold)
                    if match.matched and match.author:
                        # Používame plné meno s diakritikou z registra
                        matched_authors.append(match.author.full_name)
                        matched_faculties.append(faculty)
                        matched_ous.append(ou)
                    else:
                        unmatched_utb.append(author)
                        needs_llm = True

        flags: dict = {FlagKey.MATCHED_UTB_AUTHORS: len(matched_authors)}
        if unmatched_utb:
            flags[FlagKey.UNMATCHED_UTB_AUTHORS] = unmatched_utb
        if warnings:
            flags[FlagKey.PARSE_WARNINGS] = warnings
        if any("Viac UTB blokov" in w for w in warnings):
            flags[FlagKey.MULTIPLE_UTB_BLOCKS] = True

        result.update({
            "heuristic_status":               HeuristicStatus.PROCESSED,
            "needs_llm":                      needs_llm,
            # TEXT[] – Python list, psycopg3 ho automaticky preloží na PostgreSQL array
            "utb_contributor_internalauthor": matched_authors or None,
            "utb_faculty":                    list(dict.fromkeys(filter(None, matched_faculties))) or None,
            "utb_ou":                         list(dict.fromkeys(filter(None, matched_ous)))      or None,
            "flags":                          flags,
        })

    except Exception as exc:
        result["needs_llm"] = True
        result["flags"]     = {FlagKey.ERROR: str(exc)}

    return result


# -----------------------------------------------------------------------
# Dávkové spracovanie
# -----------------------------------------------------------------------

def process_batch(rows: list, registry: list[InternalAuthor]) -> list[dict]:
    return [
        process_record(
            resource_id    = row.resource_id,
            wos_aff_arr    = row.wos_aff,
            dc_authors_arr = row.dc_authors,
            registry       = registry,
        )
        for row in rows
    ]


def run_heuristics(
    engine:            Engine | None = None,
    batch_size:        int | None    = None,
    limit:             int           = 0,
    reprocess_errors:  bool          = False,
) -> None:
    engine     = engine or get_local_engine()
    batch_size = batch_size or settings.heuristics_batch_size
    schema     = settings.local_schema
    table      = settings.local_table
    statuses   = [HeuristicStatus.NOT_PROCESSED]
    if reprocess_errors:
        statuses.append(HeuristicStatus.ERROR)

    registry = get_author_registry(engine)
    print(f"[INFO] Načítaných interných autorov: {len(registry)}")

    with engine.connect() as conn:
        total = conn.execute(
            text(
                f'SELECT COUNT(*) FROM "{schema}"."{table}"'
                " WHERE heuristic_status = ANY(:s)"
            ),
            {"s": statuses},
        ).scalar_one()

    if limit > 0:
        total = min(total, limit)

    if total == 0:
        print("[INFO] Žiadne záznamy na spracovanie.")
        return

    print(f"[INFO] Záznamov na spracovanie: {total}")
    print("[UTB-WOS] Výpis každého UTB bloku z utb.wos.affiliation:")

    processed = 0
    while processed < total:
        batch = min(batch_size, total - processed)

        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    f"""
                    SELECT resource_id,
                           "utb.wos.affiliation"    AS wos_aff,
                           "dc.contributor.author"  AS dc_authors
                    FROM "{schema}"."{table}"
                    WHERE heuristic_status = ANY(:s)
                    ORDER BY resource_id
                    LIMIT :lim
                    """
                ),
                {"s": statuses, "lim": batch},
            ).fetchall()

        if not rows:
            break

        updates = process_batch(rows, registry)

        update_sql = f"""
            UPDATE "{schema}"."{table}"
            SET
                flags                          = %s::jsonb,
                heuristic_status               = %s,
                heuristic_version              = %s,
                heuristic_processed_at         = %s,
                needs_llm                      = %s,
                dc_contributor_author          = %s,
                utb_contributor_internalauthor = %s,
                utb_faculty                    = %s,
                utb_ou                         = %s
            WHERE resource_id = %s
        """
        # psycopg3 preloží Python list na TEXT[] automaticky
        params = [
            (
                json.dumps(u["flags"], ensure_ascii=False),
                u["heuristic_status"],
                u["heuristic_version"],
                u["heuristic_processed_at"],
                u["needs_llm"],
                u["dc_contributor_author"],
                u["utb_contributor_internalauthor"],
                u["utb_faculty"],
                u["utb_ou"],
                u["resource_id"],
            )
            for u in updates
        ]

        raw = engine.raw_connection()
        try:
            with raw.cursor() as cur:
                cur.executemany(update_sql, params)
            raw.commit()
        finally:
            raw.close()

        processed += len(rows)
        print(f"  Spracované: {processed}/{total}")

    print(f"[OK] Heuristiky hotové. Spracovaných: {processed}")