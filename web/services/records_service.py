"""Načítanie a zoskupovanie záznamov pre hlavnú stránku."""

from __future__ import annotations

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
                q.librarian_modified_at
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
        result.append(rec)
    return result


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
                m."dc.identifier.isbn"     AS isbn_arr
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
        rec.group = _assign_group(rec, doi_riv_set, issn_riv_set, isbn_riv_set)
        groups[rec.group].append(rec)

    return groups
