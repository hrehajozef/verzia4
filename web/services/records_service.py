"""Načítanie a zoskupovanie záznamov pre hlavnú stránku."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

from sqlalchemy import text

from src.common.constants import QUEUE_TABLE
from src.config.settings import settings
from src.db.engines import get_local_engine

SORT_OPTIONS = {
    "oldest":  ('m."dc.date.issued"[1]',  "ASC NULLS LAST"),
    "newest":  ('m."dc.date.issued"[1]',  "DESC NULLS LAST"),
    "journal": ('m."dc.relation.ispartof"[1]', "ASC NULLS LAST"),
    "volume":  ('m."utb.relation.volume"', "ASC NULLS LAST"),
}

GROUP_EXISTING  = "existing"   # záznam zodpovedá existujúcemu záznamu v repozitári (RIV/OBD)
GROUP_DUPLICATE = "duplicate"  # záznam je importovaný z WoS aj Scopus
GROUP_SINGLE    = "single"     # záznam len z WoS alebo len zo Scopus


@dataclass
class RecordRow:
    resource_id:  str
    title:        str | None
    authors:      list[str]
    year:         str | None
    journal:      str | None
    volume:       str | None
    issue:        str | None
    source:       list[str]
    has_wos:      bool
    has_scopus:   bool
    has_riv:      bool
    doi:          str | None
    issn:         list[str]
    isbn:         list[str]
    group:        str = field(default="")
    duplicate_ids: list[str] = field(default_factory=list)
    duplicate_group_key: str | None = None
    duplicate_matches: list["DuplicateMatch"] = field(default_factory=list)
    merged_children: list["MergedHistoryRow"] = field(default_factory=list)
    merged_summary: str | None = None


@dataclass
class DuplicateMatch:
    resource_id: str
    match_type: str
    score: float | None = None
    details: dict[str, Any] = field(default_factory=dict)
    summary: str = ""


@dataclass
class MergedHistoryRow:
    resource_id: str
    title: str | None
    authors: list[str]
    year: str | None
    journal: str | None
    volume: str | None
    issue: str | None
    doi: str | None
    match_type: str
    match_score: float | None
    details: dict[str, Any] = field(default_factory=dict)
    summary: str = ""


def _to_list(val: Any) -> list[str]:
    if val is None:
        return []
    if isinstance(val, (list, tuple)):
        return [str(v).strip() for v in val if v and str(v).strip()]
    s = str(val).strip()
    return [s] if s else []


def _first(val: Any) -> str | None:
    lst = _to_list(val)
    return lst[0] if lst else None


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


def _match_summary(match_type: str, score: float | None, details: dict[str, Any]) -> str:
    basis = details.get("basis")
    if basis == "exact_column":
        column = details.get("column") or match_type.replace("exact:", "", 1)
        value = details.get("matched_value")
        return f"Presná zhoda: {column}" + (f" = {value}" if value else "")
    if match_type == "early_access":
        title_score = details.get("title_similarity")
        return "Early access / final version" + (f" (názov {title_score:.3f})" if isinstance(title_score, (int, float)) else "")
    if match_type == "merged_type":
        return "Rovnaký obsah, iný typ záznamu"
    if match_type == "exact:content":
        title_score = details.get("title_similarity")
        return "Obsahová zhoda" + (f" (názov {title_score:.3f})" if isinstance(title_score, (int, float)) else "")
    if match_type == "autoplagiat":
        return "Rovnaký obsah v inom časopise"
    if match_type.startswith("fuzzy_title"):
        if score is not None:
            return f"Podobný názov ({score:.3f})"
        return "Podobný názov"
    return match_type


def _duplicate_matches(*flag_values: Any) -> list[DuplicateMatch]:
    matches: list[DuplicateMatch] = []
    seen: set[tuple[str, str, str]] = set()
    for flags_raw in flag_values:
        flags = _json_dict(flags_raw)
        duplicates = flags.get("duplicates")
        if not isinstance(duplicates, list):
            continue
        for item in duplicates:
            if not isinstance(item, dict):
                continue
            rid = item.get("resource_id")
            if rid is None:
                continue
            match_type = str(item.get("match_type") or "unknown")
            details = item.get("details")
            if not isinstance(details, dict):
                details = {}
            score = item.get("score")
            score_val = float(score) if isinstance(score, (int, float)) else None
            key = (str(rid), match_type, json.dumps(details, sort_keys=True, ensure_ascii=False))
            if key in seen:
                continue
            seen.add(key)
            matches.append(DuplicateMatch(
                resource_id=str(rid),
                match_type=match_type,
                score=score_val,
                details=details,
                summary=_match_summary(match_type, score_val, details),
            ))
    return matches


def _duplicate_ids(matches: list[DuplicateMatch]) -> list[str]:
    return list(dict.fromkeys(match.resource_id for match in matches))


def _duplicate_group_key(resource_id: str, duplicate_ids: list[str]) -> str | None:
    if not duplicate_ids:
        return None
    return "|".join(sorted([resource_id, *duplicate_ids], key=lambda x: int(x) if x.isdigit() else x))


def _attach_duplicate_metadata(records: list[RecordRow], engine) -> None:
    for rec in records:
        rec.merged_children = []
        rec.merged_summary = None

    if not records:
        return

    record_ids = [int(rec.resource_id) for rec in records if rec.resource_id.isdigit()]
    schema = settings.local_schema

    merged_map: dict[str, list[MergedHistoryRow]] = {}
    if record_ids:
        try:
            with engine.connect() as conn:
                history_rows = conn.execute(text(f"""
                    SELECT
                        resource_id,
                        "dc.title"              AS title_arr,
                        "dc.contributor.author" AS authors_arr,
                        "dc.date.issued"        AS issued_arr,
                        "dc.relation.ispartof"  AS journal_arr,
                        "utb.relation.volume"   AS volume,
                        "utb.relation.issue"    AS issue,
                        "dc.identifier.doi"     AS doi_arr,
                        dedup_kept_resource_id,
                        dedup_match_type,
                        dedup_match_score,
                        dedup_match_details
                    FROM "{schema}"."dedup_histoire"
                    WHERE dedup_kept_resource_id = ANY(:ids)
                      AND resource_id <> dedup_kept_resource_id
                    ORDER BY dedup_merged_at DESC, resource_id ASC
                """), {"ids": record_ids}).fetchall()
        except Exception:
            history_rows = []

        for row in history_rows:
            details = _json_dict(row.dedup_match_details)
            score = float(row.dedup_match_score) if row.dedup_match_score is not None else None
            child = MergedHistoryRow(
                resource_id=str(row.resource_id),
                title=_first(row.title_arr),
                authors=_to_list(row.authors_arr),
                year=_first(row.issued_arr),
                journal=_first(row.journal_arr),
                volume=str(row.volume).strip() if row.volume else None,
                issue=str(row.issue).strip() if row.issue else None,
                doi=_first(row.doi_arr),
                match_type=row.dedup_match_type or "unknown",
                match_score=score,
                details=details,
                summary=_match_summary(row.dedup_match_type or "unknown", score, details),
            )
            merged_map.setdefault(str(row.dedup_kept_resource_id), []).append(child)

    for rec in records:
        rec.merged_children = merged_map.get(rec.resource_id, [])
        if rec.merged_children:
            summaries = list(dict.fromkeys(child.summary for child in rec.merged_children if child.summary))
            rec.merged_summary = "; ".join(summaries)


def _source_flags(source_arr: list[str]) -> tuple[bool, bool, bool]:
    """Vráti (has_wos, has_scopus, has_riv) pre pole utb.source."""
    has_wos    = any("wok"    in s.lower() for s in source_arr)
    has_scopus = any("scopus" in s.lower() for s in source_arr)
    has_riv    = any(
        part in s.lower() for s in source_arr
        for part in ("riv", "obd", "orig")
    )
    return has_wos, has_scopus, has_riv


def _assign_group(
    row:           RecordRow,
    doi_riv_set:   set[str],
    issn_riv_set:  set[str],
    isbn_riv_set:  set[str],
) -> str:
    """
    Priradí skupinu pre záznam z WoS/Scopus:
      GROUP_EXISTING  – zodpovedá existujúcemu záznamu v RIV/OBD
      GROUP_DUPLICATE – importovaný z oboch zdrojov (WoS + Scopus)
      GROUP_SINGLE    – len jeden zdroj
    """
    # Zodpovedá záznamu v repozitári?
    doi = (row.doi or "").strip().lower()
    if doi and doi in doi_riv_set:
        return GROUP_EXISTING
    for issn in row.issn:
        if issn.lower() in issn_riv_set:
            return GROUP_EXISTING
    for isbn in row.isbn:
        if isbn.lower() in isbn_riv_set:
            return GROUP_EXISTING

    # WoS aj Scopus?
    if row.has_wos and row.has_scopus:
        return GROUP_DUPLICATE

    return GROUP_SINGLE


SEARCH_FIELD_CONDITIONS: dict[str, str] = {
    "id":      'm.resource_id::text ILIKE :q',
    "title":   'array_to_string(m."dc.title", \' \') ILIKE :q',
    "authors": 'array_to_string(m."dc.contributor.author", \' \') ILIKE :q',
    "year":    'COALESCE(m."dc.date.issued"[1], \'\') ILIKE :q',
    "journal": 'array_to_string(m."dc.relation.ispartof", \' \') ILIKE :q',
    "voliss":  'CONCAT(COALESCE(m."utb.relation.volume",\'\'), \'/\', COALESCE(m."utb.relation.issue",\'\')) ILIKE :q',
    "doi":     'array_to_string(m."dc.identifier.doi", \' \') ILIKE :q',
}


def search_records(
    q:                str,
    fields:           list[str],
    include_processed: bool = False,
    limit:            int = 10,
    engine=None,
) -> list[dict]:
    """Fulltextové vyhľadávanie v záznamoch. Vracia zoznam dict pre JSON."""
    engine = engine or get_local_engine()
    schema = settings.local_schema
    table  = settings.local_table
    queue  = QUEUE_TABLE

    valid_fields = [f for f in fields if f in SEARCH_FIELD_CONDITIONS]
    if not valid_fields or not q.strip():
        return []

    field_sql       = " OR ".join(SEARCH_FIELD_CONDITIONS[f] for f in valid_fields)
    processed_filter = "" if include_processed else "AND q.librarian_checked_at IS NULL"

    sql = f"""
        SELECT
            m.resource_id,
            m."dc.title"               AS title_arr,
            m."dc.contributor.author"  AS authors_arr,
            m."dc.date.issued"         AS issued_arr,
            m."dc.relation.ispartof"   AS journal_arr,
            m."utb.relation.volume"    AS volume,
            m."utb.relation.issue"     AS issue,
            m."dc.identifier.doi"      AS doi_arr
        FROM "{schema}"."{table}" m
        JOIN "{schema}"."{queue}" q ON m.resource_id = q.resource_id
        WHERE m.withdrawn = FALSE
          AND ({field_sql})
          {processed_filter}
        ORDER BY m."dc.date.issued"[1] ASC NULLS LAST
        LIMIT :limit
    """

    with engine.connect() as conn:
        rows = conn.execute(text(sql), {"q": f"%{q}%", "limit": limit}).fetchall()

    results = []
    for row in rows:
        vol   = str(row.volume).strip() if row.volume else ""
        iss   = str(row.issue).strip()  if row.issue  else ""
        voliss = f"{vol}/{iss}" if vol and iss else (vol or iss)
        results.append({
            "resource_id": str(row.resource_id),
            "title":       _first(row.title_arr)   or "",
            "authors":     _to_list(row.authors_arr)[:3],
            "year":        _first(row.issued_arr)  or "",
            "journal":     _first(row.journal_arr) or "",
            "voliss":      voliss,
            "doi":         _first(row.doi_arr)     or "",
        })
    return results


def fetch_pending_records(engine=None) -> list[RecordRow]:
    """
    Načíta záznamy s uloženými zmenami čakajúcimi na schválenie.
    Kritérium: librarian_modified_at IS NOT NULL AND librarian_checked_at IS NULL.
    """
    engine = engine or get_local_engine()
    schema = settings.local_schema
    table  = settings.local_table
    queue  = QUEUE_TABLE

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT
                m.resource_id,
                m."dc.title"               AS title_arr,
                m."dc.contributor.author"  AS authors_arr,
                m."dc.date.issued"         AS issued_arr,
                m."dc.relation.ispartof"   AS journal_arr,
                m."utb.relation.volume"    AS volume,
                m."utb.relation.issue"     AS issue,
                m."utb.source"             AS source_arr,
                m."dc.identifier.doi"      AS doi_arr,
                m."dc.identifier.issn"     AS issn_arr,
                m."dc.identifier.isbn"     AS isbn_arr,
                q.librarian_modified_at,
                q.author_flags AS queue_author_flags,
                m.author_flags AS main_author_flags
            FROM "{schema}"."{table}" m
            JOIN "{schema}"."{queue}" q ON m.resource_id = q.resource_id
            WHERE q.librarian_modified_at IS NOT NULL
              AND q.librarian_checked_at IS NULL
              AND m.withdrawn = FALSE
            ORDER BY q.librarian_modified_at DESC
        """)).fetchall()

    result = []
    for row in rows:
        source_arr = _to_list(row.source_arr)
        has_wos, has_scopus, has_riv = _source_flags(source_arr)
        rec = RecordRow(
            resource_id = str(row.resource_id),
            title       = _first(row.title_arr),
            authors     = _to_list(row.authors_arr),
            year        = _first(row.issued_arr),
            journal     = _first(row.journal_arr),
            volume      = str(row.volume).strip() if row.volume else None,
            issue       = str(row.issue).strip() if row.issue else None,
            source      = source_arr,
            has_wos     = has_wos,
            has_scopus  = has_scopus,
            has_riv     = has_riv,
            doi         = _first(row.doi_arr),
            issn        = _to_list(row.issn_arr),
            isbn        = _to_list(row.isbn_arr),
        )
        rec.duplicate_matches = _duplicate_matches(row.queue_author_flags, row.main_author_flags)
        rec.duplicate_ids = _duplicate_ids(rec.duplicate_matches)
        rec.duplicate_group_key = _duplicate_group_key(rec.resource_id, rec.duplicate_ids)
        result.append(rec)
    _attach_duplicate_metadata(result, engine)
    return sorted(result, key=lambda rec: (rec.duplicate_group_key or f"~{rec.resource_id}", rec.resource_id))


def fetch_unchecked_records(
    sort:   str = "oldest",
    engine = None,
) -> dict[str, list[RecordRow]]:
    """
    Načíta záznamy, ktoré knihovník ešte neskontroloval (librarian_checked_at IS NULL).
    Vráti slovník {group_key: [RecordRow, ...]}.
    """
    engine = engine or get_local_engine()
    schema = settings.local_schema
    table  = settings.local_table
    queue  = QUEUE_TABLE

    sort_col, sort_dir = SORT_OPTIONS.get(sort, SORT_OPTIONS["oldest"])

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT
                m.resource_id,
                m."dc.title"               AS title_arr,
                m."dc.contributor.author"  AS authors_arr,
                m."dc.date.issued"         AS issued_arr,
                m."dc.relation.ispartof"   AS journal_arr,
                m."utb.relation.volume"    AS volume,
                m."utb.relation.issue"     AS issue,
                m."utb.source"             AS source_arr,
                m."dc.identifier.doi"      AS doi_arr,
                m."dc.identifier.issn"     AS issn_arr,
                m."dc.identifier.isbn"     AS isbn_arr,
                q.author_flags             AS queue_author_flags,
                m.author_flags             AS main_author_flags
            FROM "{schema}"."{table}" m
            JOIN "{schema}"."{queue}" q ON m.resource_id = q.resource_id
            WHERE q.librarian_checked_at IS NULL
              AND m.withdrawn = FALSE
            ORDER BY {sort_col} {sort_dir}, m.resource_id ASC
        """)).fetchall()

    if not rows:
        return {GROUP_EXISTING: [], GROUP_DUPLICATE: [], GROUP_SINGLE: []}

    # Zostavíme zoznamy DOI/ISSN/ISBN pre záznamy z RIV/OBD (slúžia na porovnanie)
    riv_rows = conn = None
    with engine.connect() as conn:
        riv_rows = conn.execute(text(f"""
            SELECT
                m."dc.identifier.doi"  AS doi_arr,
                m."dc.identifier.issn" AS issn_arr,
                m."dc.identifier.isbn" AS isbn_arr
            FROM "{schema}"."{table}" m
            WHERE EXISTS (
                SELECT 1 FROM unnest(m."utb.source") s
                WHERE s ILIKE '%-riv' OR s ILIKE '%-obd' OR s ILIKE '%-orig'
            )
        """)).fetchall()

    doi_riv_set:  set[str] = set()
    issn_riv_set: set[str] = set()
    isbn_riv_set: set[str] = set()
    for r in (riv_rows or []):
        for d in _to_list(r.doi_arr):
            doi_riv_set.add(d.strip().lower())
        for i in _to_list(r.issn_arr):
            issn_riv_set.add(i.strip().lower())
        for i in _to_list(r.isbn_arr):
            isbn_riv_set.add(i.strip().lower())

    groups: dict[str, list[RecordRow]] = {
        GROUP_EXISTING:  [],
        GROUP_DUPLICATE: [],
        GROUP_SINGLE:    [],
    }

    for row in rows:
        source_arr = _to_list(row.source_arr)
        has_wos, has_scopus, has_riv = _source_flags(source_arr)

        # Záznamy, ktoré SÚ z RIV/OBD (nie nové importy), preskočíme
        if has_riv and not has_wos and not has_scopus:
            continue

        rec = RecordRow(
            resource_id = str(row.resource_id),
            title       = _first(row.title_arr),
            authors     = _to_list(row.authors_arr),
            year        = _first(row.issued_arr),
            journal     = _first(row.journal_arr),
            volume      = str(row.volume).strip() if row.volume else None,
            issue       = str(row.issue).strip() if row.issue else None,
            source      = source_arr,
            has_wos     = has_wos,
            has_scopus  = has_scopus,
            has_riv     = has_riv,
            doi         = _first(row.doi_arr),
            issn        = _to_list(row.issn_arr),
            isbn        = _to_list(row.isbn_arr),
        )
        rec.duplicate_matches = _duplicate_matches(row.queue_author_flags, row.main_author_flags)
        rec.duplicate_ids = _duplicate_ids(rec.duplicate_matches)
        rec.duplicate_group_key = _duplicate_group_key(rec.resource_id, rec.duplicate_ids)
        rec.group = _assign_group(rec, doi_riv_set, issn_riv_set, isbn_riv_set)
        groups[rec.group].append(rec)

    for key in groups:
        _attach_duplicate_metadata(groups[key], engine)
        groups[key].sort(key=lambda rec: (rec.duplicate_group_key or f"~{rec.resource_id}", rec.resource_id))

    return groups
