"""Heuristický runner pre mená autorov a afiliácie.

Stratégie:
  A) Má WoS afiliáciu → parsuj WoS bloky, matchuj autorov z UTB blokov voči
     lokálnej tabuľke utb_internal_authors. Pre každého nájdeného autora sa
     WoS fakulta má prednosť; ak nezodpovedá remote DB, zapíše sa flag.
     Ak WoS neobsahuje OU, doplní sa z remote DB.
  B) Nemá WoS afiliáciu → matchuj dc.contributor.author priamo proti registru.
     Fakultu/OU vezmi z remote DB. Ak má autor viac fakúlt a WoS nepomôže,
     zapíše sa flag pre manuálne doriešenie knihovníkom.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.authors.registry import (
    InternalAuthor,
    get_author_registry,
    lookup_author_affiliations,
    match_author,
)
from src.common.constants import (
    CZECH_FACULTY_MAP_NORM,
    DEPT_KEYWORD_MAP,
    FACULTIES,
    FACULTY_ENGLISH_TO_ID,
    FACULTY_KEYWORD_RULES,
    WOS_ABBREV_NORM,
    FlagKey,
    HeuristicStatus,
    _norm,
)
from src.config.settings import settings
from src.db.engines import get_local_engine, get_remote_engine
from src.authors.parsers.wos import (
    extract_ou_candidates,
    normalize_text,
    parse_wos_affiliation,
)

HEURISTIC_VERSION = "4.0.0"

_DEPT_PREFIXES = ("dept", "ctr ", "inst ", "lab ", "centre", "center", "language", "res ctr")


def _keyword_score(keyword: str) -> int:
    base = len(keyword)
    if any(keyword.startswith(p) for p in _DEPT_PREFIXES):
        base += 20
    return base


def resolve_faculty_and_ou(affiliation_text: str) -> tuple[str, str]:
    """
    Z textu WoS afiliácie vráti (plný_anglický_názov_fakulty, plný_názov_oddelenia).
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


def _registry_faculty_to_english(czech_faculty: str) -> str:
    """Prevedie český názov fakulty na anglický cez faculty_id. Ak nenájde, vráti pôvodný."""
    fid = CZECH_FACULTY_MAP_NORM.get(_norm(czech_faculty))
    return FACULTIES.get(fid, czech_faculty) if fid else czech_faculty


# -----------------------------------------------------------------------
# Spracovanie jedného záznamu
# -----------------------------------------------------------------------

def process_record(
    resource_id:     int,
    wos_aff_arr:     list[str] | None,
    dc_authors_arr:  list[str] | None,
    registry:        list[InternalAuthor],
    normalize:       bool          = False,
    remote_engine:   Engine | None = None,
) -> dict:

    result: dict = {
        "resource_id":                     resource_id,
        "author_heuristic_status":         HeuristicStatus.ERROR,
        "author_heuristic_version":        HEURISTIC_VERSION,
        "author_heuristic_processed_at":   datetime.now(timezone.utc),
        "author_needs_llm":                False,
        "author_dc_names":                 list(dc_authors_arr) if dc_authors_arr else None,
        "author_internal_names":           None,
        "author_faculty":                  None,
        "author_ou":                       None,
        "author_flags":                    {},
    }

    try:
        has_wos = bool(wos_aff_arr and any(x for x in wos_aff_arr if x))

        if has_wos:
            # ------------------------------------------------------------------
            # Path A: WoS afiliácia
            # ------------------------------------------------------------------
            matched_authors:    list[str]  = []
            matched_faculties:  list[str]  = []
            matched_ous:        list[str]  = []
            unmatched_utb:      list[str]  = []
            warnings:           list[str]  = []
            faculty_mismatches: list[dict] = []
            needs_llm:          bool       = False
            seen_authors:       set[str]   = set()

            for raw_aff in wos_aff_arr:
                if not raw_aff:
                    continue
                parsed = parse_wos_affiliation(str(raw_aff), resource_id=resource_id)
                warnings.extend(parsed.warnings)
                if not parsed.ok:
                    needs_llm = True

                for block in parsed.utb_blocks:
                    wos_faculty, wos_ou = resolve_faculty_and_ou(block.affiliation_raw)
                    if not wos_ou:
                        candidates = extract_ou_candidates(block.affiliation_raw)
                        wos_ou = candidates[0] if candidates else ""

                    for author_str in block.authors:
                        norm_author = normalize_text(author_str)
                        if norm_author in seen_authors:
                            continue
                        seen_authors.add(norm_author)

                        m = match_author(author_str, registry, settings.author_match_threshold, normalize=normalize)
                        if m.matched and m.author:
                            matched_authors.append(m.author.full_name)

                            # Afiliácia z remote DB pre tohto autora
                            db_faculties, db_ou = lookup_author_affiliations(
                                m.author.surname, m.author.firstname, remote_engine
                            )

                            faculty = wos_faculty

                            # Validuj WoS fakultu voči remote DB
                            if wos_faculty and db_faculties:
                                wos_fid     = FACULTY_ENGLISH_TO_ID.get(wos_faculty)
                                author_fids = {
                                    CZECH_FACULTY_MAP_NORM.get(_norm(f))
                                    for f in db_faculties
                                } - {None}
                                if wos_fid and author_fids and wos_fid not in author_fids:
                                    faculty_mismatches.append({
                                        "author":             m.author.full_name,
                                        "wos_faculty":        wos_faculty,
                                        "registry_faculties": list(db_faculties),
                                    })

                            # OU: WoS má prednosť, fallback z remote DB
                            ou = wos_ou or db_ou

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
            if faculty_mismatches:
                flags[FlagKey.WOS_FACULTY_NOT_IN_REGISTRY] = faculty_mismatches

            result.update({
                "author_heuristic_status": HeuristicStatus.PROCESSED,
                "author_needs_llm":        needs_llm,
                "author_internal_names":   matched_authors or None,
                "author_faculty": list(dict.fromkeys(filter(None, matched_faculties))) or None,
                "author_ou":      list(dict.fromkeys(filter(None, matched_ous)))       or None,
                "author_flags":   flags,
            })

        else:
            # ------------------------------------------------------------------
            # Path B: Žiadna WoS afiliácia – priamy matching dc.contributor.author
            # ------------------------------------------------------------------
            if not dc_authors_arr:
                result["author_heuristic_status"] = HeuristicStatus.PROCESSED
                result["author_flags"] = {FlagKey.NO_WOS_DATA: True}
                return result

            matched_authors:           list[str]  = []
            matched_faculties:         list[str]  = []
            matched_ous:               list[str]  = []
            ambiguous_faculty_authors: list[dict] = []
            seen_authors:              set[str]   = set()

            low_confidence_matches: list[dict] = []

            for author_str in dc_authors_arr:
                if not author_str:
                    continue
                norm_author = normalize_text(author_str)
                if norm_author in seen_authors:
                    continue
                seen_authors.add(norm_author)

                m = match_author(
                    author_str, registry, settings.author_match_threshold,
                    normalize=normalize, require_surname_match=True,
                )
                if m.matched and m.author:
                    matched_authors.append(m.author.full_name)

                    if m.match_type == "fuzzy":
                        low_confidence_matches.append({
                            "input":   author_str,
                            "matched": m.author.full_name,
                            "score":   round(m.score, 4),
                        })

                    db_faculties, db_ou = lookup_author_affiliations(
                        m.author.surname, m.author.firstname, remote_engine
                    )

                    if len(db_faculties) == 1:
                        faculty = _registry_faculty_to_english(db_faculties[0])
                    elif len(db_faculties) > 1:
                        faculty = ""
                        ambiguous_faculty_authors.append({
                            "author":    m.author.full_name,
                            "faculties": list(db_faculties),
                        })
                    else:
                        faculty = ""

                    matched_faculties.append(faculty)
                    matched_ous.append(db_ou)

            flags_b: dict = {
                FlagKey.NO_WOS_DATA:         True,
                FlagKey.MATCHED_UTB_AUTHORS: len(matched_authors),
            }
            if ambiguous_faculty_authors:
                flags_b[FlagKey.MULTIPLE_FACULTIES_AMBIGUOUS] = ambiguous_faculty_authors
            if low_confidence_matches:
                flags_b[FlagKey.PATH_B_LOW_CONFIDENCE] = low_confidence_matches

            result.update({
                "author_heuristic_status": HeuristicStatus.PROCESSED,
                "author_needs_llm":        False,
                "author_internal_names":   matched_authors or None,
                "author_faculty": list(dict.fromkeys(filter(None, matched_faculties))) or None,
                "author_ou":      list(dict.fromkeys(filter(None, matched_ous)))       or None,
                "author_flags":   flags_b,
            })

    except Exception as exc:
        result["author_needs_llm"] = True
        result["author_flags"]     = {FlagKey.ERROR: str(exc)}

    return result


# -----------------------------------------------------------------------
# Dávkové spracovanie
# -----------------------------------------------------------------------

def process_batch(
    rows:          list,
    registry:      list[InternalAuthor],
    normalize:     bool          = False,
    remote_engine: Engine | None = None,
) -> list[dict]:
    return [
        process_record(
            resource_id    = row.resource_id,
            wos_aff_arr    = row.wos_aff,
            dc_authors_arr = row.dc_authors,
            registry       = registry,
            normalize      = normalize,
            remote_engine  = remote_engine,
        )
        for row in rows
    ]


def run_heuristics(
    engine:           Engine | None = None,
    remote_engine:    Engine | None = None,
    batch_size:       int | None    = None,
    limit:            int           = 0,
    reprocess_errors: bool          = False,
    reprocess:        bool          = False,
    normalize:        bool          = False,
) -> None:
    engine        = engine        or get_local_engine()
    remote_engine = remote_engine or get_remote_engine()
    batch_size    = batch_size    or settings.heuristics_batch_size
    schema        = settings.local_schema
    table         = settings.local_table
    statuses      = [HeuristicStatus.NOT_PROCESSED]
    if reprocess_errors:
        statuses.append(HeuristicStatus.ERROR)
    if reprocess:
        statuses.append(HeuristicStatus.PROCESSED)

    registry = get_author_registry(engine)
    print(f"[INFO] Načítaných interných autorov z lokálnej DB: {len(registry)}")

    with engine.connect() as conn:
        total = conn.execute(
            text(f'SELECT COUNT(*) FROM "{schema}"."{table}" WHERE author_heuristic_status = ANY(:s)'),
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
                    WHERE author_heuristic_status = ANY(:s)
                    ORDER BY resource_id
                    LIMIT :lim
                """),
                {"s": statuses, "lim": batch},
            ).fetchall()

        if not rows:
            break

        updates = process_batch(rows, registry, normalize=normalize, remote_engine=remote_engine)

        update_sql = f"""
            UPDATE "{schema}"."{table}"
            SET
                author_flags                   = %s::jsonb,
                author_heuristic_status        = %s,
                author_heuristic_version       = %s,
                author_heuristic_processed_at  = %s,
                author_needs_llm               = %s,
                author_dc_names                = %s,
                author_internal_names          = %s,
                author_faculty                 = %s,
                author_ou                      = %s
            WHERE resource_id = %s
        """
        params = [
            (
                json.dumps(u["author_flags"], ensure_ascii=False),
                u["author_heuristic_status"],
                u["author_heuristic_version"],
                u["author_heuristic_processed_at"],
                u["author_needs_llm"],
                u["author_dc_names"],
                u["author_internal_names"],
                u["author_faculty"],
                u["author_ou"],
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

    print(f"[OK] Heuristiky autorov hotové. Spracovaných: {processed}")


# -----------------------------------------------------------------------
# Porovnanie s hodnotami knihovníka
# -----------------------------------------------------------------------

def _norm_name_set(names: list[str] | None) -> set[str]:
    """Normalizuje zoznam mien na porovnanie: lowercase, bez diakritiky, komprimované medzery."""
    from src.authors.registry import _normalize_name
    if not names:
        return set()
    return {_normalize_name(n) for n in names if n and n.strip()}


def compare_with_librarian(engine: "Engine | None" = None) -> None:
    """
    Porovná author_internal_names (program) vs utb.contributor.internalauthor (knihovník).

    Kategórie:
      exact      – normalizované množiny sú totožné
      partial    – prienik neprázdny, ale nie totožný
      no_overlap – obe neprázdne, prienik prázdny
      only_prog  – program našiel autorov, knihovník nemá
      only_lib   – knihovník má autorov, program nenašiel
      both_empty – oba stĺpce prázdne / null
    """
    engine = engine or get_local_engine()
    schema = settings.local_schema
    table  = settings.local_table

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT resource_id,
                   author_internal_names                  AS prog,
                   "utb.contributor.internalauthor"       AS lib
            FROM "{schema}"."{table}"
            WHERE author_heuristic_status = 'processed'
        """)).fetchall()

    cats: dict[str, int] = {
        "exact":      0,
        "partial":    0,
        "no_overlap": 0,
        "only_prog":  0,
        "only_lib":   0,
        "both_empty": 0,
    }
    total = len(rows)

    for row in rows:
        prog = _norm_name_set(row.prog)
        lib  = _norm_name_set(row.lib)

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
    print(f"Spracovaných záznamov (heuristic_status=processed): {total}")
    print()
    print(f"  Presná zhoda (exact):          {cats['exact']:>6}  ({100*cats['exact']/total:.1f}%)" if total else "")
    print(f"  Čiastočná zhoda (partial):     {cats['partial']:>6}  ({100*cats['partial']/total:.1f}%)" if total else "")
    print(f"  Bez prieniku (no_overlap):     {cats['no_overlap']:>6}  ({100*cats['no_overlap']/total:.1f}%)" if total else "")
    print(f"  Len program (only_prog):       {cats['only_prog']:>6}  ({100*cats['only_prog']/total:.1f}%)" if total else "")
    print(f"  Len knihovník (only_lib):      {cats['only_lib']:>6}  ({100*cats['only_lib']/total:.1f}%)" if total else "")
    print(f"  Oba prázdne (both_empty):      {cats['both_empty']:>6}  ({100*cats['both_empty']/total:.1f}%)" if total else "")
    print()
    if total:
        print(f"  Celkom zhodných (exact+partial): {matched} / {total}  ({100*matched/total:.1f}%)")
