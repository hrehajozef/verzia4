"""Validácia kvality metadát.

Spúšťa sa PRED heuristickým spracovaním ako vstupná kontrola.

Kontroly:
  1. Trailing spaces   – vedúce/koncové biele znaky v textových poliach
  2. Mojibake          – znaky poškodené nesprávnym enkódovaním
  3. Formát DOI        – stĺpec dc.identifier.doi (bez prefixu http)
  4. Interní autori ⊆ všetci autori  (len ak heuristika prebehla)
  5. Interní autori existujú v registri (len ak heuristika prebehla)

Výsledky sa ukladajú do stĺpcov validation_status a validation_flags.
"""

from __future__ import annotations

import json
import re
import unicodedata
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.config.settings import settings
from src.db.engines import get_local_engine

VALIDATION_VERSION = "1.0.0"

# Textové stĺpce kontrolované na trailing spaces a mojibake
_TEXT_COLUMNS: list[str] = [
    "dc.title",
    "dc.contributor.author",
    "dc.description.abstract",
    "dc.identifier.doi",
    "utb.wos.affiliation",
    "utb.scopus.affiliation",
]

# DOI formát: začína 10. + registrant + suffix, bez http prefixu
_DOI_RE = re.compile(r"^10\.\d{4,9}/[-._;()\/:A-Za-z0-9]+$")

# Mojibake patterny:
# - U+FFFD (replacement char)
# - \xc3 + \x80-\xbf  (UTF-8 2-byte seq čítaný ako Latin-1 → napr. "Ã©" = é)
# - \xe2\x80 sequences (UTF-8 3-byte seq čítaný ako Latin-1 → napr. "â€™" = ')
_MOJIBAKE_RE = re.compile(
    "\ufffd"                                 # Unicode replacement character
    "|\u00c3[\u0080-\u00bf\u00c0-\u00ff]"    # Ã + 0x80-0xFF (UTF-8 C3+xx ako Latin-1)
    "|\u00e2\u0080[^\\s]"                    # â€ + char (UTF-8 E2 80 xx ako Latin-1)
)


# -----------------------------------------------------------------------
# Funkcie jednotlivých kontrol
# -----------------------------------------------------------------------

def check_trailing_spaces(value: str) -> bool:
    """True ak hodnota má vedúce alebo koncové biele znaky."""
    return bool(value) and value != value.strip()


def check_mojibake(value: str) -> bool:
    """True ak hodnota obsahuje znaky typické pre poškodené enkódovanie."""
    return bool(value) and bool(_MOJIBAKE_RE.search(value))


def check_doi_format(doi: str) -> bool:
    """True ak DOI zodpovedá formátu 10.XXXX/suffix (bez http prefixu)."""
    if not doi:
        return True   # chýbajúce DOI nie je chyba formátu
    return bool(_DOI_RE.match(doi.strip()))


def _normalize_name(name: str) -> str:
    nfd    = unicodedata.normalize("NFD", name)
    no_acc = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    return re.sub(r"\s+", " ", no_acc.lower()).strip()


# -----------------------------------------------------------------------
# Validácia jedného záznamu
# -----------------------------------------------------------------------

def validate_record(
    resource_id:    int,
    row_data:       dict[str, Any],
    registry_names: set[str],
) -> tuple[str, dict]:
    """
    Zvaliduje jeden záznam. Vráti (status, issues_dict).

    status: 'ok' | 'has_issues'
    issues_dict: slovník s nájdenými problémami (prázdny ak ok)
    """
    issues: dict[str, Any] = {}

    # --- 1. Trailing spaces ---
    trailing: list[str] = []
    for col in _TEXT_COLUMNS:
        value = row_data.get(col)
        if isinstance(value, str):
            if check_trailing_spaces(value):
                trailing.append(col)
        elif isinstance(value, list):
            if any(isinstance(item, str) and check_trailing_spaces(item) for item in value):
                trailing.append(col)
    if trailing:
        issues["trailing_spaces"] = trailing

    # --- 2. Mojibake ---
    mojibake: list[str] = []
    for col in _TEXT_COLUMNS:
        value = row_data.get(col)
        if isinstance(value, str):
            if check_mojibake(value):
                mojibake.append(col)
        elif isinstance(value, list):
            if any(isinstance(item, str) and check_mojibake(item) for item in value):
                mojibake.append(col)
    if mojibake:
        issues["mojibake"] = mojibake

    # --- 3. DOI formát ---
    doi_raw = row_data.get("dc.identifier.doi")
    doi_str: str | None = None
    if isinstance(doi_raw, list) and doi_raw:
        doi_str = str(doi_raw[0]) if doi_raw[0] else None
    elif isinstance(doi_raw, str):
        doi_str = doi_raw

    if doi_str and not check_doi_format(doi_str):
        issues["invalid_doi"] = doi_str

    # --- 4. Interní autori ⊆ všetci autori (len ak heuristika prebehla) ---
    internal_authors: list[str] = row_data.get("utb_contributor_internalauthor") or []
    all_authors:      list[str] = row_data.get("dc_contributor_author") or []

    if internal_authors and all_authors:
        all_norms = {_normalize_name(a) for a in all_authors if a}
        not_found: list[str] = []
        for ia in internal_authors:
            norm_ia = _normalize_name(ia)
            # Priezvisko prefix match (toleruje rozdiel v diakritike/formáte)
            surname_prefix = norm_ia.split(",")[0].strip()[:6] if "," in norm_ia else norm_ia[:6]
            if surname_prefix and not any(surname_prefix in na for na in all_norms):
                not_found.append(ia)
        if not_found:
            issues["internal_not_in_authors"] = not_found

    # --- 5. Interní autori existujú v registri ---
    if internal_authors and registry_names:
        not_in_registry = [ia for ia in internal_authors if ia not in registry_names]
        if not_in_registry:
            issues["authors_not_in_registry"] = not_in_registry

    status = "ok" if not issues else "has_issues"
    return status, issues


# -----------------------------------------------------------------------
# Migrácia DB stĺpcov
# -----------------------------------------------------------------------

def setup_validation_columns(engine: Engine | None = None) -> None:
    """
    Pridá validation stĺpce do lokálnej tabuľky.
    Bezpečné spustiť opakovane (ADD COLUMN IF NOT EXISTS).
    """
    engine = engine or get_local_engine()
    schema = settings.local_schema
    table  = settings.local_table

    cols = [
        ("validation_status",     "TEXT",        "'not_checked'"),
        ("validation_flags",      "JSONB",       "'{}'::jsonb"),
        ("validation_version",    "TEXT",        None),
        ("validation_checked_at", "TIMESTAMPTZ", None),
    ]

    print(f"[SETUP] Pridávam validation stĺpce do {schema}.{table}...")
    with engine.begin() as conn:
        for col_name, col_type, col_default in cols:
            default_sql = f" DEFAULT {col_default}" if col_default else ""
            conn.execute(text(f"""
                ALTER TABLE "{schema}"."{table}"
                ADD COLUMN IF NOT EXISTS "{col_name}" {col_type}{default_sql}
            """))
            print(f"  + {col_name} ({col_type})")

    print("[OK] Validation stĺpce pripravené.")


# -----------------------------------------------------------------------
# Hlavný runner
# -----------------------------------------------------------------------

def run_validation(
    engine:     Engine | None = None,
    batch_size: int           = 500,
    limit:      int           = 0,
    revalidate: bool          = False,
) -> None:
    """
    Spustí validačné kontroly pre všetky záznamy.

    Args:
        engine:     SQLAlchemy engine (použije lokálny ak None).
        batch_size: Veľkosť dávky.
        limit:      Max počet záznamov (0 = všetky).
        revalidate: Ak True, znovu validuje aj záznamy s existujúcim výsledkom.
    """
    engine = engine or get_local_engine()
    schema = settings.local_schema
    table  = settings.local_table

    # Načítaj registre interných autorov (raz)
    with engine.connect() as conn:
        reg_rows = conn.execute(text("""
            SELECT surname || ', ' || firstname AS full_name
            FROM utb_internal_authors
        """)).fetchall()
    registry_names: set[str] = {r.full_name for r in reg_rows}

    # Počet záznamov na spracovanie
    where_clause = "" if revalidate else "WHERE validation_status = 'not_checked'"
    with engine.connect() as conn:
        total = conn.execute(
            text(f'SELECT COUNT(*) FROM "{schema}"."{table}" {where_clause}')
        ).scalar_one()

    if limit > 0:
        total = min(total, limit)

    if total == 0:
        print("[INFO] Žiadne záznamy na validáciu.")
        return

    print(f"[INFO] Záznamov na validáciu: {total}")

    # SQL pre výber stĺpcov
    select_sql = f"""
        SELECT
            resource_id,
            "dc.title"                AS "dc.title",
            "dc.contributor.author"   AS "dc.contributor.author",
            "dc.description.abstract" AS "dc.description.abstract",
            "dc.identifier.doi"       AS "dc.identifier.doi",
            "utb.wos.affiliation"     AS "utb.wos.affiliation",
            "utb.scopus.affiliation"  AS "utb.scopus.affiliation",
            dc_contributor_author,
            utb_contributor_internalauthor
        FROM "{schema}"."{table}"
        {where_clause}
        ORDER BY resource_id
        LIMIT :lim
    """

    update_sql = f"""
        UPDATE "{schema}"."{table}"
        SET
            validation_status     = %s,
            validation_flags      = %s::jsonb,
            validation_version    = %s,
            validation_checked_at = %s
        WHERE resource_id = %s
    """

    processed    = 0
    issues_count = 0

    while processed < total:
        batch = min(batch_size, total - processed)

        with engine.connect() as conn:
            rows = conn.execute(text(select_sql), {"lim": batch}).fetchall()

        if not rows:
            break

        params = []
        for row in rows:
            row_data = {
                "dc.title":                    row[1],
                "dc.contributor.author":       row[2],
                "dc.description.abstract":     row[3],
                "dc.identifier.doi":           row[4],
                "utb.wos.affiliation":         row[5],
                "utb.scopus.affiliation":      row[6],
                "dc_contributor_author":       row[7],
                "utb_contributor_internalauthor": row[8],
            }
            status, issues = validate_record(row.resource_id, row_data, registry_names)
            if status == "has_issues":
                issues_count += 1

            params.append((
                status,
                json.dumps(issues, ensure_ascii=False),
                VALIDATION_VERSION,
                datetime.now(timezone.utc),
                row.resource_id,
            ))

        raw = engine.raw_connection()
        try:
            with raw.cursor() as cur:
                cur.executemany(update_sql, params)
            raw.commit()
        finally:
            raw.close()

        processed += len(rows)
        print(f"  Spracované: {processed}/{total} | s problémami: {issues_count}")

    print(f"[OK] Validácia hotová. Spracovaných: {processed}, s problémami: {issues_count}")


# -----------------------------------------------------------------------
# Štatistiky
# -----------------------------------------------------------------------

def print_validation_status(engine: Engine | None = None) -> None:
    engine = engine or get_local_engine()
    schema = settings.local_schema
    table  = settings.local_table

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT
                validation_status,
                COUNT(*) AS cnt
            FROM "{schema}"."{table}"
            GROUP BY validation_status
            ORDER BY cnt DESC
        """)).fetchall()

    print("\n=== Štatistiky validácie ===")
    for r in rows:
        print(f"  {(r.validation_status or 'NULL'):20s} | {r.cnt:6d}")

    # Rozpad problémov
    with engine.connect() as conn:
        issue_rows = conn.execute(text(f"""
            SELECT validation_flags
            FROM "{schema}"."{table}"
            WHERE validation_status = 'has_issues'
        """)).fetchall()

    if issue_rows:
        counters: dict[str, int] = {}
        for row in issue_rows:
            flags = row[0] or {}
            for key in flags:
                counters[key] = counters.get(key, 0) + 1

        print("\n  Rozpad typov problémov:")
        for key, count in sorted(counters.items(), key=lambda x: -x[1]):
            print(f"    {key:35s}: {count:6d}")
