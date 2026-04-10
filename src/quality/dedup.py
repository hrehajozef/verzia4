"""Deduplikácia záznamov.

Stratégie:
  1. Presná zhoda podľa DOI (alebo zvoleného stĺpca)
  2. Obsahová zhoda 100% (title + autori + abstrakt) → rozlíšenie kategórií
  3. Fuzzy zhoda titulu (Jaro-Winkler ≥ threshold) + rok ±1 + ISSN/ISBN

Kategórie duplikátov:
  exact:<column>      – presná zhoda hodnoty stĺpca
  early_access        – rovnaký obsah+časopis, jeden záznam nemá pagination → zlúčiť
  merged_type         – rovnaký obsah, iný časopis, article vs conferenceObject → zlúčiť
  autoplagiat         – rovnaký obsah, iný časopis, rovnaký typ → len flag, neslúčiť
  fuzzy_title         – podobnosť titulu ≥ threshold → len flag

Spracovanie:
  - exact / early_access / merged_type → oba záznamy nakopíruje do dedup_histoire
                                          → UPDATE + DELETE
  - autoplagiat / fuzzy_title          → zapíše flag do flags['duplicates'], bez zmeny

Príkazy:
  python -m src.cli dedup-setup      # vytvorí tabuľku dedup_histoire (raz)
  python -m src.cli deduplicate      # spustí deduplikáciu
"""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

import jellyfish
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.config.settings import settings
from src.db.engines import get_local_engine

DEDUP_VERSION = "2.0.0"

_PAGINATION_COLS = [
    "utb.relation.volume",
    "utb.relation.issue",
]


# -----------------------------------------------------------------------
# Normalizačné funkcie
# -----------------------------------------------------------------------

def _normalize_text(s: str | None) -> str:
    """Základná normalizácia – accenty + lowercase. Použitie: abstract, autori."""
    if not s:
        return ""
    nfd    = unicodedata.normalize("NFD", s.lower())
    no_acc = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    return re.sub(r"\s+", " ", no_acc).strip()


# Znaky, ktoré sa odstraňujú pri normalizácii titulu pre dedup
_DEDUP_PUNCT = re.compile(
    r"[—–\-\u2010-\u2015"  # em/en/rôzne pomlčky
    r",;:\.!?\""            # interpunkcia
    r"''‛'\u2018\u2019"    # apostrofy
    r'""„‟«»‹›'            # úvodzovky
    r"()\[\]{}]"
)


def _normalize_title_for_dedup(s: str | None) -> str:
    """
    Agresívna normalizácia titulu pre deduplikáciu.
    Odstraňuje pomlčky, interpunkciu, rôzne apostrofy a úvodzovky,
    potom normalizuje accenty a whitespace.
    """
    if not s:
        return ""
    t = s.lower()
    t = unicodedata.normalize("NFD", t)
    t = "".join(c for c in t if unicodedata.category(c) != "Mn")  # bez akcentov
    t = _DEDUP_PUNCT.sub(" ", t)
    return re.sub(r"\s+", " ", t).strip()


def _norm_column_value(value: Any) -> str:
    if isinstance(value, list):
        value = value[0] if value else None
    if not value:
        return ""
    return str(value).strip().lower()


def _extract_year(dc_issued: Any) -> int | None:
    text_val = _norm_column_value(dc_issued)
    if not text_val:
        return None
    m = re.search(r"\b(19|20)\d{2}\b", text_val)
    return int(m.group()) if m else None


def _normalize_issn(issn: Any) -> str:
    raw = _norm_column_value(issn)
    return re.sub(r"[^0-9xX]", "", raw).lower()


def _normalize_authors(authors: Any) -> str:
    """Normalizuje zoznam autorov na porovnateľný reťazec (sort + join)."""
    if not authors:
        return ""
    lst = authors if isinstance(authors, list) else [authors]
    normalized = sorted(_normalize_text(a) for a in lst if a)
    return "|".join(normalized)


def _normalize_abstract(abstract: Any) -> str:
    if isinstance(abstract, list):
        abstract = abstract[0] if abstract else None
    return _normalize_text(abstract or "")


def _pagination_present(rec: dict) -> bool:
    """True ak záznam má aspoň jeden pagination stĺpec s hodnotou."""
    return any(bool(_norm_column_value(rec.get(col))) for col in _PAGINATION_COLS)


# -----------------------------------------------------------------------
# Fáza 1: Presná zhoda podľa stĺpca (DOI)
# -----------------------------------------------------------------------

def find_duplicates_by_column(
    engine:    Engine,
    by_column: str = "dc.identifier.doi",
) -> list[tuple[list[int], str, str, float]]:
    """Nájde skupiny záznamov s rovnakou hodnotou stĺpca (case-insensitive)."""
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
# Fáza 2: Obsahová zhoda 100% – kategorizácia
# -----------------------------------------------------------------------

def find_content_duplicates(
    engine: Engine,
) -> list[tuple[int, int, str, float, str]]:
    """
    Nájde záznamy s identickým normalizovaným obsahom (title + autori + abstrakt).

    Vracia (id_a, id_b, match_type, score, details) kde match_type je:
      early_access  – rovnaký obsah+ISSN, jeden nemá pagination
      merged_type   – rovnaký obsah, rozdielny ISSN, article vs conferenceObject
      autoplagiat   – rovnaký obsah, rozdielny ISSN, rovnaký typ dokumentu
    """
    schema = settings.local_schema
    table  = settings.local_table

    pagination_select = ", ".join(
        f'"{col}"' for col in _PAGINATION_COLS
    )

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT
                resource_id,
                "dc.title"[1]                AS title,
                "dc.contributor.author"      AS authors,
                "dc.description.abstract"[1] AS abstract,
                "dc.identifier.issn"[1]      AS issn,
                "dc.type"                    AS doctype,
                {pagination_select}
            FROM "{schema}"."{table}"
            WHERE "dc.title" IS NOT NULL
              AND array_length("dc.title", 1) > 0
            ORDER BY resource_id
        """)).fetchall()

    records: list[dict] = []
    for row in rows:
        rec: dict = {
            "id":           row.resource_id,
            "norm_title":   _normalize_title_for_dedup(row.title),
            "norm_authors": _normalize_authors(row.authors),
            "norm_abstract":_normalize_abstract(row.abstract),
            "issn":         _normalize_issn(row.issn),
            "doctype":      _norm_column_value(row.doctype),
        }
        for col in _PAGINATION_COLS:
            rec[col] = row._mapping.get(col)
        records.append(rec)

    # Bucket podľa title hash – porovnávame len záznamy so zhodným titulom
    title_buckets: dict[str, list[dict]] = defaultdict(list)
    for rec in records:
        if rec["norm_title"]:
            key = hashlib.md5(rec["norm_title"].encode()).hexdigest()
            title_buckets[key].append(rec)

    results:    list[tuple[int, int, str, float, str]] = []
    seen_pairs: set[tuple[int, int]] = set()

    for bucket in title_buckets.values():
        if len(bucket) < 2:
            continue

        for i, rec_a in enumerate(bucket):
            for rec_b in bucket[i + 1:]:
                pair = (min(rec_a["id"], rec_b["id"]), max(rec_a["id"], rec_b["id"]))
                if pair in seen_pairs:
                    continue

                # Kontrola zhody titulu (100%)
                if rec_a["norm_title"] != rec_b["norm_title"]:
                    continue

                # Kontrola zhody autorov
                if rec_a["norm_authors"] and rec_b["norm_authors"]:
                    if rec_a["norm_authors"] != rec_b["norm_authors"]:
                        continue

                # Kontrola zhody abstraktu (ak obidva majú)
                if rec_a["norm_abstract"] and rec_b["norm_abstract"]:
                    if rec_a["norm_abstract"] != rec_b["norm_abstract"]:
                        continue

                seen_pairs.add(pair)

                same_issn = (
                    rec_a["issn"] and rec_b["issn"]
                    and rec_a["issn"] == rec_b["issn"]
                )
                a_has_pages = _pagination_present(rec_a)
                b_has_pages = _pagination_present(rec_b)
                a_type = rec_a["doctype"]
                b_type = rec_b["doctype"]
                type_combo = frozenset({a_type, b_type})

                if same_issn and (a_has_pages != b_has_pages):
                    match_type = "early_access"
                    details    = f"issn={rec_a['issn']}, pagination_a={a_has_pages}, pagination_b={b_has_pages}"
                elif not same_issn and type_combo == {"article", "conferenceobject"}:
                    match_type = "merged_type"
                    details    = f"issn_a={rec_a['issn']}, issn_b={rec_b['issn']}, types={a_type}+{b_type}"
                elif not same_issn:
                    match_type = "autoplagiat"
                    details    = f"issn_a={rec_a['issn']}, issn_b={rec_b['issn']}"
                else:
                    # Rovnaký ISSN, obaja majú pagination (alebo obaja nemajú) – presný duplikát
                    match_type = "exact:content"
                    details    = f"issn={rec_a['issn']}"

                results.append((rec_a["id"], rec_b["id"], match_type, 1.0, details))

    return results


# -----------------------------------------------------------------------
# Fáza 3: Fuzzy zhoda titulu
# -----------------------------------------------------------------------

def find_duplicates_fuzzy(
    engine:          Engine,
    title_threshold: float = 0.85,
) -> list[tuple[int, int, str, float, str]]:
    """
    Fuzzy porovnanie normalizovaných titulov (Jaro-Winkler ≥ threshold).
    Blocking: rok vydania ±1.
    Vracia (id_a, id_b, match_type, score, details).
    """
    schema = settings.local_schema
    table  = settings.local_table

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT
                resource_id,
                "dc.title"[1]           AS title,
                "dc.date.issued"[1]     AS issued,
                "dc.identifier.issn"[1] AS issn,
                "dc.identifier.isbn"[1] AS isbn
            FROM "{schema}"."{table}"
            WHERE "dc.title" IS NOT NULL
              AND array_length("dc.title", 1) > 0
            ORDER BY resource_id
        """)).fetchall()

    records: list[dict] = [
        {
            "id":    row.resource_id,
            "title": _normalize_title_for_dedup(row.title),
            "year":  _extract_year(row.issued),
            "issn":  _normalize_issn(row.issn),
            "isbn":  _normalize_issn(row.isbn),
        }
        for row in rows
    ]

    year_index: dict[int | None, list[dict]] = defaultdict(list)
    for rec in records:
        year_index[rec["year"]].append(rec)

    duplicates: list[tuple[int, int, str, float, str]] = []
    seen_pairs: set[tuple[int, int]] = set()

    for base_year, block in year_index.items():
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

                score = jellyfish.jaro_winkler_similarity(rec_a["title"], rec_b["title"])
                if score < title_threshold:
                    continue

                seen_pairs.add(pair)

                issn_match = bool(rec_a["issn"] and rec_b["issn"] and rec_a["issn"] == rec_b["issn"])
                isbn_match = bool(rec_a["isbn"] and rec_b["isbn"] and rec_a["isbn"] == rec_b["isbn"])

                if issn_match:
                    match_type = "fuzzy_title+issn"
                elif isbn_match:
                    match_type = "fuzzy_title+isbn"
                else:
                    match_type = "fuzzy_title"

                details = f"score={score:.4f}"
                duplicates.append((rec_a["id"], rec_b["id"], match_type, round(score, 4), details))

    return duplicates


# -----------------------------------------------------------------------
# História deduplikácie
# -----------------------------------------------------------------------

def setup_dedup_table(engine: Engine | None = None) -> None:
    """
    Vytvorí tabuľku dedup_histoire (ak neexistuje) s rovnakou štruktúrou
    ako zdrojová tabuľka + stĺpce pre metadata deduplikácie.
    Bezpečné spustiť opakovane.
    """
    engine = engine or get_local_engine()
    schema = settings.local_schema
    table  = settings.local_table
    hist   = "dedup_histoire"

    with engine.begin() as conn:
        exists = conn.execute(text("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_schema = :schema AND table_name = :tbl
            )
        """), {"schema": schema, "tbl": hist}).scalar()

        if not exists:
            print(f"[SETUP] Vytváram tabuľku {schema}.{hist}...")
            conn.execute(text(f"""
                CREATE TABLE "{schema}"."{hist}" AS
                SELECT * FROM "{schema}"."{table}" WHERE FALSE
            """))
            print(f"  + tabuľka {hist} vytvorená (štruktúra z {table})")

        # Pridaj dedup stĺpce (idempotentné)
        for col, typ in [
            ("dedup_merged_at",         "TIMESTAMPTZ"),
            ("dedup_match_type",        "TEXT"),
            ("dedup_kept_resource_id",  "INTEGER"),
            ("dedup_other_resource_id", "INTEGER"),
        ]:
            conn.execute(text(f"""
                ALTER TABLE "{schema}"."{hist}"
                ADD COLUMN IF NOT EXISTS "{col}" {typ}
            """))

    print(f"[OK] Tabuľka {schema}.{hist} je pripravená.")


def _copy_to_history(
    raw_conn,
    schema:        str,
    table:         str,
    resource_id:   int,
    match_type:    str,
    kept_id:       int,
    other_id:      int,
    col_names:     list[str],
) -> None:
    """Nakopíruje jeden záznam do dedup_histoire pred zlúčením."""
    src_cols  = ", ".join(f'"{c}"' for c in col_names)
    dedup_ext = ", dedup_merged_at, dedup_match_type, dedup_kept_resource_id, dedup_other_resource_id"

    with raw_conn.cursor() as cur:
        cur.execute(
            f"""
            INSERT INTO "{schema}"."dedup_histoire" ({src_cols}{dedup_ext})
            SELECT {src_cols}, %s, %s, %s, %s
            FROM "{schema}"."{table}"
            WHERE resource_id = %s
            """,
            (datetime.now(timezone.utc), match_type, kept_id, other_id, resource_id),
        )


def _get_column_names(engine: Engine, schema: str, table: str) -> list[str]:
    with engine.connect() as conn:
        return list(conn.execute(text("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = :schema AND table_name = :table
            ORDER BY ordinal_position
        """), {"schema": schema, "table": table}).scalars())


# -----------------------------------------------------------------------
# Zlúčenie záznamov
# -----------------------------------------------------------------------

def _merge_pair(
    raw_conn,
    schema:     str,
    table:      str,
    kept_id:    int,
    deleted_id: int,
    match_type: str,
    col_names:  list[str],
    extra_updates: dict | None = None,
) -> None:
    """
    Nakopíruje oba záznamy do histórie, potom UPDATE kept + DELETE deleted.
    extra_updates: {col_name: value} aplikované na kept_id po zlúčení.
    """
    _copy_to_history(raw_conn, schema, table, kept_id,    match_type, kept_id, deleted_id, col_names)
    _copy_to_history(raw_conn, schema, table, deleted_id, match_type, kept_id, deleted_id, col_names)

    if extra_updates:
        set_parts = ", ".join(f'"{c}" = %s' for c in extra_updates)
        with raw_conn.cursor() as cur:
            cur.execute(
                f'UPDATE "{schema}"."{table}" SET {set_parts} WHERE resource_id = %s',
                (*extra_updates.values(), kept_id),
            )

    with raw_conn.cursor() as cur:
        cur.execute(
            f'DELETE FROM "{schema}"."{table}" WHERE resource_id = %s',
            (deleted_id,),
        )


def _fetch_record(engine: Engine, schema: str, table: str, resource_id: int) -> dict:
    """Načíta celý záznam ako dict."""
    with engine.connect() as conn:
        row = conn.execute(text(f"""
            SELECT * FROM "{schema}"."{table}" WHERE resource_id = :rid
        """), {"rid": resource_id}).mappings().fetchone()
    return dict(row) if row else {}


# -----------------------------------------------------------------------
# Zápis flagov (bez fyzického zlúčenia)
# -----------------------------------------------------------------------

def _write_duplicates_to_flags(
    engine:           Engine,
    id_to_duplicates: dict[int, list[dict]],
) -> None:
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
                    SET author_flags = (author_flags - 'duplicates')
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
    engine = engine or get_local_engine()
    schema = settings.local_schema
    table  = settings.local_table

    col_names = _get_column_names(engine, schema, table)

    flag_only:    dict[int, list[dict]] = defaultdict(list)  # autoplagiat, fuzzy
    merge_pairs:  list[tuple[int, int, str, dict | None]] = []  # (kept, deleted, type, extra)
    already_deleted: set[int] = set()

    # --- Fáza 1: Presná zhoda podľa DOI ---
    print(f"[INFO] Fáza 1 – presná zhoda podľa '{by_column}'...")
    exact_groups = find_duplicates_by_column(engine, by_column=by_column)
    exact_pairs  = sum(len(ids) * (len(ids) - 1) // 2 for ids, *_ in exact_groups)

    for ids, col, matched_val, score in exact_groups:
        kept_id = min(ids)  # najnižší resource_id je kanonický
        for deleted_id in ids:
            if deleted_id == kept_id:
                continue
            merge_pairs.append((kept_id, deleted_id, f"exact:{col}", None))

    print(f"  Skupiny: {len(exact_groups):4d} | Páry: {exact_pairs:6d}")

    # --- Fáza 2: Obsahová zhoda 100% ---
    print("[INFO] Fáza 2 – obsahová zhoda (title + autori + abstrakt)...")
    content_results = find_content_duplicates(engine)

    content_counts: dict[str, int] = defaultdict(int)
    for id_a, id_b, match_type, score, details in content_results:
        content_counts[match_type] += 1

        if match_type == "autoplagiat":
            # Len flag, bez zlúčenia
            flag_only[id_a].append({"resource_id": id_b, "match_type": match_type, "score": score, "details": details})
            flag_only[id_b].append({"resource_id": id_a, "match_type": match_type, "score": score, "details": details})

        elif match_type == "early_access":
            # Zachovaj záznam s pagination, zmaž bez pagination
            rec_a = _fetch_record(engine, schema, table, id_a)
            rec_b = _fetch_record(engine, schema, table, id_b)
            a_has = _pagination_present({col: rec_a.get(col) for col in _PAGINATION_COLS})
            kept_id    = id_a if a_has else id_b
            deleted_id = id_b if a_has else id_a
            rec_kept    = rec_a if a_has else rec_b
            rec_deleted = rec_b if a_has else rec_a

            # Predvyplň pagination z kompletného záznamu (mal by ich mať kept)
            extra: dict = {}
            for col in _PAGINATION_COLS:
                val = rec_kept.get(col)
                if val:
                    extra[col] = val

            merge_pairs.append((kept_id, deleted_id, match_type, extra if extra else None))

        elif match_type in ("merged_type", "exact:content"):
            # Pre merged_type: uprednostni "article" typ
            rec_a = _fetch_record(engine, schema, table, id_a)
            rec_b = _fetch_record(engine, schema, table, id_b)
            dtype_a = _norm_column_value(rec_a.get("dc.type"))
            dtype_b = _norm_column_value(rec_b.get("dc.type"))
            if match_type == "merged_type":
                kept_id    = id_a if dtype_a == "article" else id_b
                deleted_id = id_b if dtype_a == "article" else id_a
                # Nastav kombinovaný typ
                extra = {"dc.type": ["article", "conferenceObject"]}
            else:
                kept_id    = min(id_a, id_b)
                deleted_id = max(id_a, id_b)
                extra = None
            merge_pairs.append((kept_id, deleted_id, match_type, extra))

    for mtype, cnt in sorted(content_counts.items()):
        print(f"  {mtype:20s}: {cnt:4d}")

    # --- Fáza 3: Fuzzy zhoda ---
    fuzzy_count = 0
    if fuzzy_fallback:
        print(f"[INFO] Fáza 3 – fuzzy porovnanie (threshold={title_threshold:.2f})...")
        fuzzy_results = find_duplicates_fuzzy(engine, title_threshold=title_threshold)
        fuzzy_count   = len(fuzzy_results)

        for id_a, id_b, match_type, score, details in fuzzy_results:
            flag_only[id_a].append({"resource_id": id_b, "match_type": match_type, "score": score, "details": details})
            flag_only[id_b].append({"resource_id": id_a, "match_type": match_type, "score": score, "details": details})

        print(f"  Fuzzy páry: {fuzzy_count:6d}")

    total_merge = len(merge_pairs)
    total_flag  = len(flag_only)
    print(f"[INFO] Na zlúčenie: {total_merge} | Len flag: {total_flag} záznamov")

    if dry_run:
        print("[DRY RUN] Žiadne zmeny v DB.")
        print("  Ukážka zlúčení (prvých 5):")
        for kept, deleted, mtype, extra in merge_pairs[:5]:
            print(f"    KEEP {kept} / DELETE {deleted} [{mtype}]" + (f" extra={list(extra)}" if extra else ""))
        print("  Ukážka flagov (prvých 5):")
        for rid, dups in list(flag_only.items())[:5]:
            print(f"    resource_id={rid} → {[d['match_type'] for d in dups]}")
        return

    # --- Zápis ---
    if merge_pairs:
        print("[INFO] Zlučujem záznamy (+ kopírujem do dedup_histoire)...")
        raw = engine.raw_connection()
        skipped = 0
        executed = 0
        try:
            for kept_id, deleted_id, match_type, extra in merge_pairs:
                if deleted_id in already_deleted:
                    skipped += 1
                    continue
                _merge_pair(raw, schema, table, kept_id, deleted_id, match_type, col_names, extra)
                already_deleted.add(deleted_id)
                executed += 1
            raw.commit()
        finally:
            raw.close()
        print(f"  Zlúčených párov: {executed} (preskočených: {skipped})")

    if flag_only:
        print("[INFO] Zapisujem flags pre autoplagiát / fuzzy...")
        _write_duplicates_to_flags(engine, dict(flag_only))

    print(f"[OK] Deduplikácia hotová. Zlúčené: {total_merge}, Flagované: {total_flag}.")


# -----------------------------------------------------------------------
# Štatistiky
# -----------------------------------------------------------------------

def print_dedup_status(engine: Engine | None = None) -> None:
    engine = engine or get_local_engine()
    schema = settings.local_schema
    table  = settings.local_table

    with engine.connect() as conn:
        total = conn.execute(text(f'SELECT COUNT(*) FROM "{schema}"."{table}"')).scalar_one()
        with_dups = conn.execute(text(f"""
            SELECT COUNT(*) FROM "{schema}"."{table}"
            WHERE author_flags ? 'duplicates'
              AND jsonb_array_length(author_flags->'duplicates') > 0
        """)).scalar_one()

    # História
    hist_count = 0
    try:
        with engine.connect() as conn:
            hist_count = conn.execute(text(
                f'SELECT COUNT(*) FROM "{schema}"."dedup_histoire"'
            )).scalar_one()
    except Exception:
        pass

    print("\n=== Štatistiky deduplikácie ===")
    print(f"  Celkom záznamov:          {total:6d}")
    print(f"  Záznamy s flag duplikát:  {with_dups:6d}")
    print(f"  Záznamy bez duplikátu:    {total - with_dups:6d}")
    print(f"  Záznamy v histórii:       {hist_count:6d}")

    if with_dups:
        with engine.connect() as conn:
            rows = conn.execute(text(f"""
                SELECT elem->>'match_type' AS mtype, COUNT(*) AS cnt
                FROM "{schema}"."{table}",
                     jsonb_array_elements(author_flags->'duplicates') AS elem
                GROUP BY mtype
                ORDER BY cnt DESC
            """)).fetchall()
        if rows:
            print("\n  Typy flagov:")
            for r in rows:
                print(f"    {(r.mtype or 'unknown'):25s}: {r.cnt:6d}")
