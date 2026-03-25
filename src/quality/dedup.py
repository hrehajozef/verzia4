"""Deduplikácia záznamov.

Stratégie:
  1. Presná zhoda podľa zvoleného stĺpca (default: dc.identifier.doi, case-insensitive)
  2. Fuzzy zhoda: podobnosť titulu (Jaro-Winkler ≥ threshold) + rok vydania ±1 + ISSN/ISBN

Výsledky:
  Každý duplikát dostane do flags záznam:
    {"duplicates": [{"resource_id": X, "match_type": "exact:doi", "score": 1.0, "matched_value": "10.xxx"}]}
  Oba záznamy v páre dostanú odkaz na druhý.

Spúšťa sa CLI príkazom: python -m src.cli deduplicate --by <column>
"""

from __future__ import annotations

import json
import re
import unicodedata
from collections import defaultdict
from typing import Any

import jellyfish
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.config.settings import settings
from src.db.engines import get_local_engine

DEDUP_VERSION = "1.0.0"


# -----------------------------------------------------------------------
# Pomocné funkcie
# -----------------------------------------------------------------------

def _normalize_text(s: str | None) -> str:
    if not s:
        return ""
    nfd    = unicodedata.normalize("NFD", s.lower())
    no_acc = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    return re.sub(r"\s+", " ", no_acc).strip()


def _norm_column_value(value: Any) -> str:
    """Normalizuje hodnotu stĺpca (string alebo prvý prvok poľa) na lowercase string."""
    if isinstance(value, list):
        value = value[0] if value else None
    if not value:
        return ""
    return str(value).strip().lower()


def _extract_year(dc_issued: Any) -> int | None:
    """Extrahuje rok z dc.date.issued hodnoty."""
    text_val = _norm_column_value(dc_issued)
    if not text_val:
        return None
    m = re.search(r"\b(19|20)\d{2}\b", text_val)
    return int(m.group()) if m else None


def _normalize_issn(issn: Any) -> str:
    """Normalizuje ISSN/ISBN – len číslice a X."""
    raw = _norm_column_value(issn)
    return re.sub(r"[^0-9xX]", "", raw).lower()


# -----------------------------------------------------------------------
# Fáza 1: Presná zhoda podľa stĺpca
# -----------------------------------------------------------------------

def find_duplicates_by_column(
    engine:     Engine,
    by_column:  str = "dc.identifier.doi",
) -> list[tuple[list[int], str, str, float]]:
    """
    Nájde skupiny záznamov s rovnakou hodnotou v danom stĺpci (case-insensitive).

    Vracia zoznam (resource_id_group, column_name, normalized_value, score).
    """
    schema = settings.local_schema
    table  = settings.local_table

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT resource_id, "{by_column}"
            FROM "{schema}"."{table}"
            WHERE "{by_column}" IS NOT NULL
            ORDER BY resource_id
        """)).fetchall()

    groups: dict[str, list[int]] = defaultdict(list)
    for row in rows:
        norm_val = _norm_column_value(row[1])
        if norm_val:
            groups[norm_val].append(row.resource_id)

    return [
        (ids, by_column, norm_val, 1.0)
        for norm_val, ids in groups.items()
        if len(ids) > 1
    ]


# -----------------------------------------------------------------------
# Fáza 2: Fuzzy zhoda
# -----------------------------------------------------------------------

def find_duplicates_fuzzy(
    engine:          Engine,
    title_threshold: float = 0.85,
) -> list[tuple[int, int, str, float, str]]:
    """
    Nájde pravdepodobné duplikáty pomocou fuzzy porovnania.

    Kritériá:
      - Podobnosť titulu (Jaro-Winkler) ≥ title_threshold
      - Rok vydania ±1 (blocking)
      - Bonus: zhoda ISSN alebo ISBN

    Vracia zoznam (id_a, id_b, match_type, score, details).
    """
    schema = settings.local_schema
    table  = settings.local_table

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT
                resource_id,
                "dc.title"[1]             AS title,
                "dc.date.issued"[1]       AS issued,
                "dc.identifier.issn"[1]   AS issn,
                "dc.identifier.isbn"[1]   AS isbn
            FROM "{schema}"."{table}"
            WHERE "dc.title" IS NOT NULL
              AND array_length("dc.title", 1) > 0
            ORDER BY resource_id
        """)).fetchall()

    # Príprava záznamov
    records: list[dict] = []
    for row in rows:
        records.append({
            "id":    row.resource_id,
            "title": _normalize_text(row.title),
            "year":  _extract_year(row.issued),
            "issn":  _normalize_issn(row.issn),
            "isbn":  _normalize_issn(row.isbn),
        })

    # Blocking podľa roku: porovnávaj záznamy s rovnakým alebo susedným rokom
    year_index: dict[int | None, list[dict]] = defaultdict(list)
    for rec in records:
        year_index[rec["year"]].append(rec)

    duplicates: list[tuple[int, int, str, float, str]] = []
    seen_pairs: set[tuple[int, int]] = set()

    for base_year, block in year_index.items():
        # Porovnávame len záznamy z rovnakého alebo susedného roku
        if base_year is not None:
            comparison_pool = (
                block
                + year_index.get(base_year - 1, [])
                + year_index.get(base_year + 1, [])
            )
        else:
            comparison_pool = block

        for rec_a in block:
            for rec_b in comparison_pool:
                if rec_a["id"] == rec_b["id"]:
                    continue

                pair = (min(rec_a["id"], rec_b["id"]), max(rec_a["id"], rec_b["id"]))
                if pair in seen_pairs:
                    continue

                if not rec_a["title"] or not rec_b["title"]:
                    continue

                title_score = jellyfish.jaro_winkler_similarity(rec_a["title"], rec_b["title"])
                if title_score < title_threshold:
                    continue

                seen_pairs.add(pair)

                issn_match = bool(rec_a["issn"] and rec_b["issn"] and rec_a["issn"] == rec_b["issn"])
                isbn_match = bool(rec_a["isbn"] and rec_b["isbn"] and rec_a["isbn"] == rec_b["isbn"])

                if issn_match:
                    match_type = "title_fuzzy+issn"
                elif isbn_match:
                    match_type = "title_fuzzy+isbn"
                else:
                    match_type = "title_fuzzy"

                details = f"title_score={title_score:.4f}"
                duplicates.append((rec_a["id"], rec_b["id"], match_type, round(title_score, 4), details))

    return duplicates


# -----------------------------------------------------------------------
# Zápis výsledkov do DB
# -----------------------------------------------------------------------

def _write_duplicates_to_flags(
    engine:           Engine,
    id_to_duplicates: dict[int, list[dict]],
) -> None:
    """
    Pre každý dotknutý záznam aktualizuje kľúč 'duplicates' v stĺpci flags.
    Existujúci obsah flags zostáva zachovaný (merge pomocou ||).
    """
    if not id_to_duplicates:
        return

    schema = settings.local_schema
    table  = settings.local_table

    raw = engine.raw_connection()
    try:
        with raw.cursor() as cur:
            for resource_id, dup_list in id_to_duplicates.items():
                dup_json = json.dumps(dup_list, ensure_ascii=False)
                cur.execute(
                    f"""
                    UPDATE "{schema}"."{table}"
                    SET flags = (flags - 'duplicates')
                                || jsonb_build_object('duplicates', %s::jsonb)
                    WHERE resource_id = %s
                    """,
                    (dup_json, resource_id),
                )
        raw.commit()
    finally:
        raw.close()


# -----------------------------------------------------------------------
# Hlavný runner
# -----------------------------------------------------------------------

def run_deduplication(
    engine:          Engine | None = None,
    by_column:       str           = "dc.identifier.doi",
    fuzzy_fallback:  bool          = True,
    title_threshold: float         = 0.85,
    dry_run:         bool          = False,
) -> None:
    """
    Spustí deduplikáciu záznamov.

    Args:
        engine:          SQLAlchemy engine (použije lokálny ak None).
        by_column:       Stĺpec pre presnú zhodu (default: dc.identifier.doi).
        fuzzy_fallback:  Ak True, spustí aj fuzzy porovnanie po presnej zhode.
        title_threshold: Prah Jaro-Winkler pre fuzzy porovnanie titulu (default 0.85).
        dry_run:         Iba vypíše výsledky, nezapíše do DB.
    """
    engine = engine or get_local_engine()

    id_to_duplicates: dict[int, list[dict]] = defaultdict(list)

    # --- Fáza 1: Presná zhoda ---
    print(f"[INFO] Deduplication – presná zhoda podľa '{by_column}'...")
    exact_groups = find_duplicates_by_column(engine, by_column=by_column)
    exact_pairs  = 0

    for ids, col, matched_val, score in exact_groups:
        pair_count = len(ids) * (len(ids) - 1) // 2
        exact_pairs += pair_count
        for id_a in ids:
            for id_b in ids:
                if id_a != id_b:
                    id_to_duplicates[id_a].append({
                        "resource_id":   id_b,
                        "match_type":    f"exact:{col}",
                        "score":         score,
                        "matched_value": matched_val,
                    })

    print(f"  Skupiny: {len(exact_groups):4d} | Páry: {exact_pairs:6d}")

    # --- Fáza 2: Fuzzy zhoda ---
    fuzzy_pairs = 0
    if fuzzy_fallback:
        print(f"[INFO] Deduplication – fuzzy porovnanie (threshold={title_threshold:.2f})...")
        fuzzy_results = find_duplicates_fuzzy(engine, title_threshold=title_threshold)
        fuzzy_pairs   = len(fuzzy_results)

        for id_a, id_b, match_type, score, details in fuzzy_results:
            id_to_duplicates[id_a].append({
                "resource_id": id_b,
                "match_type":  match_type,
                "score":       score,
                "details":     details,
            })
            id_to_duplicates[id_b].append({
                "resource_id": id_a,
                "match_type":  match_type,
                "score":       score,
                "details":     details,
            })

        print(f"  Fuzzy páry: {fuzzy_pairs:6d}")

    total_affected = len(id_to_duplicates)
    print(f"[INFO] Celkom dotknutých záznamov: {total_affected}")

    if dry_run:
        print("[DRY RUN] Žiadne zmeny v DB.")
        print("  Ukážka (prvých 5):")
        for rid, dups in list(id_to_duplicates.items())[:5]:
            dup_ids = [d["resource_id"] for d in dups]
            print(f"    resource_id={rid} → duplicates={dup_ids}")
        return

    if total_affected > 0:
        print("[INFO] Zapisujem do flags v DB...")
        _write_duplicates_to_flags(engine, dict(id_to_duplicates))
        print(f"[OK] Deduplication hotová. Označených: {total_affected} záznamov.")
    else:
        print("[OK] Žiadne duplikáty nenájdené.")


# -----------------------------------------------------------------------
# Štatistiky
# -----------------------------------------------------------------------

def print_dedup_status(engine: Engine | None = None) -> None:
    """Vypíše štatistiky deduplikácie."""
    engine = engine or get_local_engine()
    schema = settings.local_schema
    table  = settings.local_table

    with engine.connect() as conn:
        total = conn.execute(text(f'SELECT COUNT(*) FROM "{schema}"."{table}"')).scalar_one()
        with_dups = conn.execute(text(f"""
            SELECT COUNT(*) FROM "{schema}"."{table}"
            WHERE flags ? 'duplicates'
              AND jsonb_array_length(flags->'duplicates') > 0
        """)).scalar_one()

    print(f"\n=== Štatistiky deduplikácie ===")
    print(f"  Celkom záznamov:        {total:6d}")
    print(f"  Záznamy s duplikátom:   {with_dups:6d}")
    print(f"  Záznamy bez duplikátu:  {total - with_dups:6d}")
