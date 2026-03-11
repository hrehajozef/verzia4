"""Heuristický runner – spracuje záznamy z lokálnej DB tabuľky.

Stratégie:
  A) Má WoS afiliáciu → parsuj WoS bloky, matchuj autorov z UTB blokov.
     Fakultu/OU urči z WoS textu; ak nenájdeš, použi pracovisko z registra.
  B) Nemá WoS afiliáciu → matchuj dc.contributor.author priamo proti registru.
     Fakultu/OU vezmi priamo z registra (parent_name / workplace_name).
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
    WOS_ABBREV_NORM,
    FlagKey,
    HeuristicStatus,
    _norm,
)
from src.config.settings import settings
from src.db.engines import get_local_engine
from src.parsers.wos_affiliation import (
    extract_ou_candidates,
    normalize_text,
    parse_wos_affiliation,
)

HEURISTIC_VERSION = "3.3.0"

_DEPT_PREFIXES = ("dept", "ctr ", "inst ", "lab ", "centre", "center", "language", "res ctr")


def _keyword_score(keyword: str) -> int:
    base = len(keyword)
    if any(keyword.startswith(p) for p in _DEPT_PREFIXES):
        base += 20
    return base


def resolve_faculty_and_ou(affiliation_text: str) -> tuple[str, str]:
    """
    Z textu WoS afiliácie vráti (plný_názov_fakulty, plný_názov_oddelenia).
    Oddelenie má bonus +20 voči len-fakultovým zápisom.
    """
    norm = _norm(affiliation_text)
    best_dept, best_fid, best_score = "", "", 0

    for keyword, (dept_name, fid) in {**WOS_ABBREV_NORM, **DEPT_KEYWORD_MAP}.items():
        if keyword in norm:
            sc = _keyword_score(keyword)
            if sc > best_score:
                best_score, best_dept, best_fid = sc, dept_name, fid

    if best_fid:
        return FACULTIES.get(best_fid, best_fid), best_dept

    for keywords, fid in FACULTY_KEYWORD_RULES:
        if any(kw in norm for kw in keywords):
            return FACULTIES.get(fid, fid), ""

    return "", ""


def _faculty_from_registry(author: InternalAuthor) -> str:
    """
    Vráti názov fakulty z registra.
    parent_name = nadradené pracovisko (zvyčajne fakulta alebo UTB celá).
    Ak parent_name nie je fakulta (napr. "Rektorát"), vráti workplace_name.
    """
    # Preferuj parent_name ak obsahuje slovo "fakulta" / "faculty" / "ústav" / "institute"
    faculty_keywords = ("fakult", "faculty", "ustav", "institute", "logist", "humanit", "multimedia")
    if author.parent_name and any(kw in author.parent_name.lower() for kw in faculty_keywords):
        return author.parent_name
    if author.workplace_name and any(kw in author.workplace_name.lower() for kw in faculty_keywords):
        return author.workplace_name
    # Fallback: vráť parent ak existuje, inak workplace
    return author.parent_name or author.workplace_name or ""


def _ou_from_registry(author: InternalAuthor) -> str:
    """
    Vráti názov oddelenia/pracoviska z registra.
    workplace_name je konkrétnejšie oddelenie (nie fakulta).
    """
    faculty_keywords = ("fakult", "faculty", "univers", "rektora")
    # Ak workplace je samotná fakulta (nie oddelenie), nevraciaj ho ako OU
    if author.workplace_name and not any(kw in author.workplace_name.lower() for kw in faculty_keywords):
        return author.workplace_name
    return ""


# -----------------------------------------------------------------------
# Spracovanie jedného záznamu
# -----------------------------------------------------------------------

def process_record(
    resource_id:     int,
    wos_aff_arr:     list[str] | None,
    dc_authors_arr:  list[str] | None,
    registry:        list[InternalAuthor],
) -> dict:

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
        has_wos = bool(wos_aff_arr and any(x for x in wos_aff_arr if x))

        if has_wos:
            # ------------------------------------------------------------
            # STRATÉGIA A: WoS afiliácia dostupná
            # Parsuj UTB bloky → matchuj autorov → urči fakultu/OU z WoS.
            # Ak WoS neurčí fakultu/OU, použi údaj z registra (DB pracovísk).
            # ------------------------------------------------------------
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
                    # Pokus 1: fakulta/OU z WoS textu
                    wos_faculty, wos_ou = resolve_faculty_and_ou(block.affiliation_raw)
                    if not wos_ou:
                        candidates = extract_ou_candidates(block.affiliation_raw)
                        wos_ou = candidates[0] if candidates else ""

                    for author_str in block.authors:
                        norm_author = normalize_text(author_str)
                        if norm_author in seen_authors:
                            continue
                        seen_authors.add(norm_author)

                        m = match_author(author_str, registry, settings.author_match_threshold)
                        if m.matched and m.author:
                            matched_authors.append(m.author.full_name)

                            # Pokus 2: ak WoS nedal fakultu/OU, vezmi z registra
                            faculty = wos_faculty or _faculty_from_registry(m.author)
                            ou      = wos_ou      or _ou_from_registry(m.author)

                            matched_faculties.append(faculty)
                            matched_ous.append(ou)
                        else:
                            unmatched_utb.append(author_str)
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
                "utb_contributor_internalauthor": matched_authors or None,
                "utb_faculty": list(dict.fromkeys(filter(None, matched_faculties))) or None,
                "utb_ou":      list(dict.fromkeys(filter(None, matched_ous)))       or None,
                "flags":       flags,
            })

        else:
            # ------------------------------------------------------------
            # STRATÉGIA B: Chýba WoS afiliácia
            # Matchuj dc.contributor.author priamo proti registru.
            # Fakultu/OU vezmi z registra (parent_name / workplace_name).
            # ------------------------------------------------------------
            if not dc_authors_arr:
                result["heuristic_status"] = HeuristicStatus.PROCESSED
                result["flags"] = {FlagKey.NO_WOS_DATA: True}
                return result

            matched_authors:   list[str] = []
            matched_faculties: list[str] = []
            matched_ous:       list[str] = []
            seen_authors:      set[str]  = set()

            for author_str in dc_authors_arr:
                if not author_str:
                    continue
                norm_author = normalize_text(author_str)
                if norm_author in seen_authors:
                    continue
                seen_authors.add(norm_author)

                m = match_author(author_str, registry, settings.author_match_threshold)
                if m.matched and m.author:
                    matched_authors.append(m.author.full_name)
                    matched_faculties.append(_faculty_from_registry(m.author))
                    matched_ous.append(_ou_from_registry(m.author))

            result.update({
                "heuristic_status":               HeuristicStatus.PROCESSED,
                # needs_llm=True len ak sme niečo našli ale nemáme fakultu
                "needs_llm": any(
                    not f for f in matched_faculties
                ) if matched_authors else False,
                "utb_contributor_internalauthor": matched_authors or None,
                "utb_faculty": list(dict.fromkeys(filter(None, matched_faculties))) or None,
                "utb_ou":      list(dict.fromkeys(filter(None, matched_ous)))       or None,
                "flags": {
                    FlagKey.NO_WOS_DATA:         True,
                    FlagKey.MATCHED_UTB_AUTHORS: len(matched_authors),
                },
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
    engine:           Engine | None = None,
    batch_size:       int | None    = None,
    limit:            int           = 0,
    reprocess_errors: bool          = False,
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
            text(f'SELECT COUNT(*) FROM "{schema}"."{table}" WHERE heuristic_status = ANY(:s)'),
            {"s": statuses},
        ).scalar_one()

    if limit > 0:
        total = min(total, limit)
    if total == 0:
        print("[INFO] Žiadne záznamy na spracovanie.")
        return

    print(f"[INFO] Záznamov na spracovanie: {total}")

    processed = 0
    while processed < total:
        batch = min(batch_size, total - processed)

        with engine.connect() as conn:
            rows = conn.execute(
                text(f"""
                    SELECT resource_id,
                           "utb.wos.affiliation"   AS wos_aff,
                           "dc.contributor.author" AS dc_authors
                    FROM "{schema}"."{table}"
                    WHERE heuristic_status = ANY(:s)
                    ORDER BY resource_id
                    LIMIT :lim
                """),
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