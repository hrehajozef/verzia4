"""Heuristicky runner pre mena autorov a afiliacie."""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.authors.parsers.scopus import parse_scopus_affiliation_array
from src.authors.parsers.wos import (
    detect_utb_affiliation,
    extract_ou_candidates,
    normalize_text,
    parse_wos_affiliation,
)
from src.authors.registry import (
    InternalAuthor,
    get_author_registry,
    lookup_author_affiliations,
    match_author,
)
from src.authors.source_authors import merge_author_lists, split_source_author_lists
from src.common.constants import (
    DEPT_KEYWORD_MAP,
    FACULTIES,
    FACULTY_ENGLISH_TO_ID,
    FACULTY_KEYWORD_RULES,
    FlagKey,
    HeuristicStatus,
    QUEUE_TABLE,
    WOS_ABBREV_NORM,
    _norm,
)
from src.config.settings import settings
from src.db.engines import get_local_engine, get_remote_engine

HEURISTIC_VERSION = "4.2.0"

_DEPT_PREFIXES = ("dept", "ctr ", "inst ", "lab ", "centre", "center", "language", "res ctr")
_MIN_STRONG_FUZZY_SCORE = 0.90


def _keyword_score(keyword: str) -> int:
    base = len(keyword)
    if any(keyword.startswith(prefix) for prefix in _DEPT_PREFIXES):
        base += 20
    return base


def resolve_faculty_and_ou(affiliation_text: str) -> tuple[str, str]:
    """
    Return (english_faculty_name, english_department_name) from affiliation text.

    Department matches are preferred over faculty-only matches.
    """
    norm = _norm(affiliation_text)
    best_dept, best_fid, best_score = "", "", 0

    for keyword, (dept_name, fid) in {**WOS_ABBREV_NORM, **DEPT_KEYWORD_MAP}.items():
        if keyword in norm:
            score = _keyword_score(keyword)
            if score > best_score:
                best_score, best_dept, best_fid = score, dept_name, fid

    if best_fid:
        return FACULTIES.get(best_fid, best_fid), best_dept

    for keywords, fid in FACULTY_KEYWORD_RULES:
        if any(keyword in norm for keyword in keywords):
            return FACULTIES.get(fid, fid), ""

    return "", ""


def _collect_affiliation_hints(
    scopus_aff_arr: list[str] | None,
    fulltext_aff_arr: list[str] | None,
) -> list[tuple[str, str, str]]:
    """Collect unique (faculty, ou, source) hints from Scopus/fulltext affiliations."""
    hints: list[tuple[str, str, str]] = []

    for parsed in parse_scopus_affiliation_array(scopus_aff_arr):
        for block in parsed.utb_blocks:
            faculty, ou = resolve_faculty_and_ou(block.raw)
            if faculty or ou:
                hints.append((faculty, ou, "scopus"))

    for raw_text in fulltext_aff_arr or []:
        if not raw_text:
            continue
        parts = [part.strip() for part in re.split(r"[\r\n;]+", str(raw_text)) if part.strip()]
        for part in parts:
            is_utb, _ = detect_utb_affiliation(part)
            if not is_utb:
                continue
            faculty, ou = resolve_faculty_and_ou(part)
            if faculty or ou:
                hints.append((faculty, ou, "fulltext"))

    return list(dict.fromkeys(hints))


def _choose_affiliation_from_hints(
    db_faculties: tuple[str, ...],
    db_ou: str,
    hints: list[tuple[str, str, str]],
) -> tuple[str, str, bool]:
    """Choose the best faculty/OU for one author using DB data and affiliation context."""
    hint_faculties = [faculty for faculty, _, _ in hints if faculty]
    hint_ous = [ou for _, ou, _ in hints if ou]
    unique_hint_faculties = list(dict.fromkeys(hint_faculties))
    unique_hint_ous = list(dict.fromkeys(hint_ous))

    used_hint = False
    faculty = ""
    ou = db_ou or ""

    if len(db_faculties) == 1:
        faculty = db_faculties[0]
    elif len(db_faculties) > 1:
        matching = [value for value in unique_hint_faculties if value in db_faculties]
        if len(matching) == 1:
            faculty = matching[0]
            used_hint = True
        elif len(unique_hint_faculties) == 1:
            faculty = unique_hint_faculties[0]
            used_hint = True
    elif len(unique_hint_faculties) == 1:
        faculty = unique_hint_faculties[0]
        used_hint = True

    if len(unique_hint_ous) == 1:
        ou = unique_hint_ous[0]
        used_hint = True

    if not faculty and ou:
        inferred_faculty_id = DEPT_KEYWORD_MAP.get(_norm(ou), ("", ""))[1]
        if inferred_faculty_id:
            faculty = FACULTIES.get(inferred_faculty_id, "")

    return faculty, ou, used_hint


def _author_output(values: list[str]) -> list[str] | None:
    return values or None


def _registry_identity(author: InternalAuthor) -> str:
    if author.limited_author_id is not None:
        return f"id:{author.limited_author_id}"
    return f"name:{normalize_text(author.canonical_name)}"


def _preferred_repo_author_names(
    repo_authors: list[str] | None,
    registry: list[InternalAuthor],
    *,
    normalize: bool,
) -> dict[str, str]:
    preferred: dict[str, str] = {}
    for author_name in repo_authors or []:
        if not author_name:
            continue
        match = match_author(
            author_name,
            registry,
            settings.author_match_threshold,
            normalize=normalize,
            require_surname_match=True,
        )
        if not (match.matched and match.author):
            continue
        identity = _registry_identity(match.author)
        preferred.setdefault(identity, author_name.strip())
    return preferred


def process_record(
    resource_id: int,
    wos_aff_arr: list[str] | None,
    dc_authors_arr: list[str] | None,
    registry: list[InternalAuthor],
    normalize: bool = False,
    remote_engine: Engine | None = None,
    scopus_aff_arr: list[str] | None = None,
    fulltext_aff_arr: list[str] | None = None,
    wos_author_arr: list[str] | None = None,
    scopus_author_arr: list[str] | None = None,
) -> dict:
    combined_authors = merge_author_lists(dc_authors_arr, wos_author_arr, scopus_author_arr)
    preferred_repo_names = _preferred_repo_author_names(dc_authors_arr, registry, normalize=normalize)
    result: dict = {
        "resource_id": resource_id,
        "author_heuristic_status": HeuristicStatus.ERROR,
        "author_heuristic_version": HEURISTIC_VERSION,
        "author_heuristic_processed_at": datetime.now(timezone.utc),
        "author_needs_llm": False,
        "author_dc_names": combined_authors or None,
        "author_internal_names": None,
        "author_faculty": None,
        "author_ou": None,
        "author_flags": {},
    }

    try:
        has_wos = bool(wos_aff_arr and any(value for value in wos_aff_arr if value))

        matched_authors: list[str] = []
        matched_faculties: list[str] = []
        matched_ous: list[str] = []
        seen_internal_authors: set[str] = set()
        ambiguous_faculty_authors: list[dict] = []
        low_confidence_matches: list[dict] = []
        context_overrides: list[dict] = []
        affiliation_hints = _collect_affiliation_hints(scopus_aff_arr, fulltext_aff_arr)
        unique_hint_faculties = list(dict.fromkeys(
            faculty for faculty, _, _ in affiliation_hints if faculty
        ))

        warnings: list[str] = []
        unmatched_utb: list[str] = []
        faculty_mismatches: list[dict] = []
        needs_llm = False
        wos_matches: dict[str, tuple[str, str]] = {}
        wos_display_names: dict[str, str] = {}

        if has_wos:
            seen_wos_authors: set[str] = set()
            for raw_aff in wos_aff_arr or []:
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
                        if norm_author in seen_wos_authors:
                            continue
                        seen_wos_authors.add(norm_author)

                        match = match_author(
                            author_str,
                            registry,
                            settings.author_match_threshold,
                            normalize=normalize,
                        )
                        if not (match.matched and match.author):
                            unmatched_utb.append(author_str)
                            needs_llm = True
                            continue

                        db_faculties, db_ou = lookup_author_affiliations(
                            match.author.surname,
                            match.author.firstname,
                            remote_engine,
                        )

                        faculty = wos_faculty or (db_faculties[0] if len(db_faculties) == 1 else "")
                        if wos_faculty and db_faculties:
                            wos_fid = FACULTY_ENGLISH_TO_ID.get(wos_faculty)
                            author_fids = {FACULTY_ENGLISH_TO_ID.get(value) for value in db_faculties} - {None}
                            if wos_fid and author_fids and wos_fid not in author_fids:
                                faculty_mismatches.append({
                                    "author": author_str,
                                    "wos_faculty": wos_faculty,
                                    "registry_faculties": list(db_faculties),
                                })

                        ou = wos_ou or db_ou
                        identity = _registry_identity(match.author)
                        wos_matches[identity] = (faculty, ou)
                        wos_display_names.setdefault(identity, author_str.strip())

        author_candidates = combined_authors or []
        if not author_candidates and wos_matches:
            author_candidates = list(wos_display_names.values())
            result["author_dc_names"] = author_candidates
        if not author_candidates:
            result["author_heuristic_status"] = HeuristicStatus.PROCESSED
            flags_empty: dict = {FlagKey.NO_WOS_DATA: True}
            if unmatched_utb:
                flags_empty[FlagKey.UNMATCHED_UTB_AUTHORS] = unmatched_utb
            if warnings:
                flags_empty[FlagKey.PARSE_WARNINGS] = warnings
            if any("Viac UTB blokov" in warning for warning in warnings):
                flags_empty[FlagKey.MULTIPLE_UTB_BLOCKS] = True
            if faculty_mismatches:
                flags_empty[FlagKey.WOS_FACULTY_NOT_IN_REGISTRY] = faculty_mismatches
            result["author_flags"] = flags_empty
            return result

        seen_source_authors: set[str] = set()
        for author_str in author_candidates:
            if not author_str:
                continue
            norm_author = normalize_text(author_str)
            if norm_author in seen_source_authors:
                continue
            seen_source_authors.add(norm_author)

            match = match_author(
                author_str,
                registry,
                settings.author_match_threshold,
                normalize=normalize,
                require_surname_match=True,
            )
            if not (match.matched and match.author):
                continue

            if match.match_type == "fuzzy" and match.score < _MIN_STRONG_FUZZY_SCORE:
                low_confidence_matches.append({
                    "input": author_str,
                    "matched": match.author.full_name,
                    "score": round(match.score, 4),
                    "accepted": False,
                })
                continue

            db_faculties, db_ou = lookup_author_affiliations(
                match.author.surname,
                match.author.firstname,
                remote_engine,
            )

            if (
                match.match_type == "fuzzy"
                and len(unique_hint_faculties) == 1
                and db_faculties
                and unique_hint_faculties[0] not in db_faculties
            ):
                low_confidence_matches.append({
                    "input": author_str,
                    "matched": match.author.full_name,
                    "score": round(match.score, 4),
                    "accepted": False,
                    "reason": "affiliation_conflict",
                })
                continue

            if match.match_type == "fuzzy":
                low_confidence_matches.append({
                    "input": author_str,
                    "matched": match.author.full_name,
                    "score": round(match.score, 4),
                    "accepted": True,
                })

            identity = _registry_identity(match.author)
            output_author = preferred_repo_names.get(identity, author_str.strip())
            if identity in seen_internal_authors:
                continue
            seen_internal_authors.add(identity)

            wos_match = wos_matches.get(identity)
            if wos_match:
                faculty, ou = wos_match
                used_hint = False
            else:
                faculty, ou, used_hint = _choose_affiliation_from_hints(db_faculties, db_ou, affiliation_hints)
                if len(db_faculties) > 1 and not faculty:
                    ambiguous_faculty_authors.append({
                        "author": output_author,
                        "faculties": list(db_faculties),
                    })
                if used_hint:
                    context_overrides.append({
                        "author": output_author,
                        "faculty": faculty,
                        "ou": ou,
                    })

            matched_authors.append(output_author)
            matched_faculties.append(faculty)
            matched_ous.append(ou)

        flags_b: dict = {
            FlagKey.MATCHED_UTB_AUTHORS: len(matched_authors),
        }
        inferred_from_cohort: list[dict] = []
        unique_matched_faculties = list(dict.fromkeys(value for value in matched_faculties if value))
        unique_matched_ous = list(dict.fromkeys(value for value in matched_ous if value))
        if len(unique_matched_faculties) == 1 or len(unique_matched_ous) == 1:
            for idx, author_name in enumerate(matched_authors):
                inferred = False
                if not matched_faculties[idx] and len(unique_matched_faculties) == 1:
                    matched_faculties[idx] = unique_matched_faculties[0]
                    inferred = True
                if not matched_ous[idx] and len(unique_matched_ous) == 1:
                    matched_ous[idx] = unique_matched_ous[0]
                    inferred = True
                if inferred:
                    inferred_from_cohort.append({
                        "author": author_name,
                        "faculty": matched_faculties[idx],
                        "ou": matched_ous[idx],
                    })
        if not has_wos:
            flags_b[FlagKey.NO_WOS_DATA] = True
        if unmatched_utb:
            flags_b[FlagKey.UNMATCHED_UTB_AUTHORS] = unmatched_utb
        if warnings:
            flags_b[FlagKey.PARSE_WARNINGS] = warnings
        if any("Viac UTB blokov" in warning for warning in warnings):
            flags_b[FlagKey.MULTIPLE_UTB_BLOCKS] = True
        if faculty_mismatches:
            flags_b[FlagKey.WOS_FACULTY_NOT_IN_REGISTRY] = faculty_mismatches
        if ambiguous_faculty_authors:
            flags_b[FlagKey.MULTIPLE_FACULTIES_AMBIGUOUS] = ambiguous_faculty_authors
        if low_confidence_matches:
            flags_b[FlagKey.PATH_B_LOW_CONFIDENCE] = low_confidence_matches
        if affiliation_hints:
            flags_b["affiliation_context_hints"] = [
                {"faculty": faculty, "ou": ou, "source": source}
                for faculty, ou, source in affiliation_hints
            ]
        if context_overrides:
            flags_b["affiliation_context_overrides"] = context_overrides
        if inferred_from_cohort:
            flags_b["affiliation_inferred_from_cohort"] = inferred_from_cohort

        result.update({
            "author_heuristic_status": HeuristicStatus.PROCESSED,
            "author_needs_llm": False,
            "author_internal_names": _author_output(matched_authors),
            "author_faculty": _author_output(matched_faculties),
            "author_ou": _author_output(matched_ous),
            "author_flags": flags_b,
        })

    except Exception as exc:
        result["author_needs_llm"] = True
        result["author_flags"] = {FlagKey.ERROR: str(exc)}

    return result


def process_batch(
    rows: list,
    registry: list[InternalAuthor],
    normalize: bool = False,
    remote_engine: Engine | None = None,
    source_author_map: dict[int, dict[str, list[str]]] | None = None,
) -> list[dict]:
    return [
        process_record(
            resource_id=row.resource_id,
            wos_aff_arr=row.wos_aff,
            dc_authors_arr=row.dc_authors,
            registry=registry,
            normalize=normalize,
            remote_engine=remote_engine,
            scopus_aff_arr=row.scopus_aff,
            fulltext_aff_arr=row.fulltext_aff,
            wos_author_arr=(source_author_map or {}).get(row.resource_id, {}).get("wos"),
            scopus_author_arr=(source_author_map or {}).get(row.resource_id, {}).get("scopus"),
        )
        for row in rows
    ]


def _build_source_author_map(
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

    history_map: dict[int, list[dict[str, Any]]] = {rid: [] for rid in record_ids}
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


def run_heuristics(
    engine: Engine | None = None,
    remote_engine: Engine | None = None,
    batch_size: int | None = None,
    limit: int = 0,
    reprocess_errors: bool = False,
    reprocess: bool = False,
    normalize: bool = False,
) -> None:
    engine = engine or get_local_engine()
    remote_engine = remote_engine or get_remote_engine()
    batch_size = batch_size or settings.heuristics_batch_size
    schema = settings.local_schema
    table = settings.local_table
    queue = QUEUE_TABLE
    statuses = [HeuristicStatus.NOT_PROCESSED]
    if reprocess_errors:
        statuses.append(HeuristicStatus.ERROR)
    if reprocess:
        statuses.append(HeuristicStatus.PROCESSED)

    registry = get_author_registry(remote_engine=remote_engine)
    print(f"[INFO] Nacitanych internych autorov z remote DB: {len(registry)}")

    with engine.connect() as conn:
        id_rows = conn.execute(
            text(f"""
                SELECT q.resource_id
                FROM "{schema}"."{queue}" q
                JOIN "{schema}"."{table}" m ON q.resource_id = m.resource_id
                WHERE q.author_heuristic_status = ANY(:s)
                ORDER BY q.resource_id
            """),
            {"s": statuses},
        ).fetchall()

    all_ids: list[int] = [int(row[0]) for row in id_rows]
    total = len(all_ids)

    if limit > 0:
        all_ids = all_ids[:limit]
        total = len(all_ids)
    if total == 0:
        print("[INFO] Ziadne zaznamy na spracovanie.")
        return

    print(f"[INFO] Zaznamov na spracovanie: {total}")

    processed = 0
    while processed < total:
        batch_ids = all_ids[processed: processed + batch_size]

        with engine.connect() as conn:
            rows = conn.execute(
                text(f"""
                    SELECT m.resource_id,
                           m."utb.wos.affiliation" AS wos_aff,
                           m."utb.scopus.affiliation" AS scopus_aff,
                           m."utb.fulltext.affiliation" AS fulltext_aff,
                           m."dc.contributor.author" AS dc_authors,
                           m."utb.source" AS source_arr
                    FROM "{schema}"."{table}" m
                    JOIN "{schema}"."{queue}" q ON m.resource_id = q.resource_id
                    WHERE q.resource_id = ANY(:ids)
                    ORDER BY m.resource_id
                """),
                {"ids": batch_ids},
            ).fetchall()

        if not rows:
            processed += len(batch_ids)
            print(f"  [WARN] Davka bez platnych zaznamov: {batch_ids}")
            continue

        source_author_map = _build_source_author_map(engine, rows)
        updates = process_batch(
            rows,
            registry,
            normalize=normalize,
            remote_engine=remote_engine,
            source_author_map=source_author_map,
        )

        update_sql = f"""
            UPDATE "{schema}"."{queue}"
            SET
                author_flags = %s::jsonb ||
                    CASE
                        WHEN author_flags ? 'duplicates'
                        THEN jsonb_build_object('duplicates', author_flags->'duplicates')
                        ELSE '{{}}'::jsonb
                    END,
                author_heuristic_status = %s,
                author_heuristic_version = %s,
                author_heuristic_processed_at = %s,
                author_needs_llm = %s,
                author_dc_names = %s,
                author_internal_names = %s,
                author_faculty = %s,
                author_ou = %s
            WHERE resource_id = %s
        """
        params = [
            (
                json.dumps(update["author_flags"], ensure_ascii=False),
                update["author_heuristic_status"],
                update["author_heuristic_version"],
                update["author_heuristic_processed_at"],
                update["author_needs_llm"],
                update["author_dc_names"],
                update["author_internal_names"],
                update["author_faculty"],
                update["author_ou"],
                update["resource_id"],
            )
            for update in updates
        ]

        raw = engine.raw_connection()
        try:
            with raw.cursor() as cur:
                cur.executemany(update_sql, params)
            raw.commit()
        finally:
            raw.close()

        processed += len(batch_ids)
        print(f"  Spracovane: {processed}/{total}")

    print(f"[OK] Heuristiky autorov hotove. Spracovanych: {processed}")


def _norm_name_set(names: list[str] | None) -> set[str]:
    from src.authors.registry import _normalize_name

    if not names:
        return set()
    return {_normalize_name(name) for name in names if name and name.strip()}


def compare_with_librarian(engine: Engine | None = None) -> None:
    """
    Compare author_internal_names (program) vs utb.contributor.internalauthor (librarian).
    """
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
