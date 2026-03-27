"""
Normalizácia dc.publisher a dc.relation.ispartof podľa ISSN/ISBN.

Logika:
  1. Záznamy sa zoskupia podľa ISSN (alebo ISBN).
  2. Pre každú skupinu sa lookupuje kanonická hodnota cez API:
       ISSN → Crossref → OpenAlex
       ISBN → Google Books → OpenLibrary
  3. Ak API nenájde nič, fallback z existujúcich záznamov:
       preferujeme hodnoty zo záznamov s Scopus afiliáciou → WoS → najpočetnejšia.
  4. Záznamy, ktorých hodnota sa líši od kanonickej, dostanú status 'has_proposal'.
  5. run_journal_apply zobrazí diff a aplikuje po schválení knihovníkom.
"""

from __future__ import annotations

import time
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine

from src.config.settings import settings
from src.db.engines import get_local_engine
from src.journals.lookup import LookupResult, lookup_by_isbn, lookup_by_issn

JOURNAL_NORM_VERSION = "1.0.0"

_RED   = "\033[91m"
_GREEN = "\033[92m"
_BOLD  = "\033[1m"
_DIM   = "\033[2m"
_RST   = "\033[0m"


class JournalNormStatus:
    NOT_PROCESSED = "not_processed"
    NO_CHANGE     = "no_change"
    HAS_PROPOSAL  = "has_proposal"
    APPLIED       = "applied"
    ERROR         = "error"


_SETUP_COLS: list[tuple[str, str, str | None]] = [
    ("journal_norm_status",             "TEXT",        f"'{JournalNormStatus.NOT_PROCESSED}'"),
    ("journal_norm_proposed_publisher", "TEXT",        None),
    ("journal_norm_proposed_ispartof",  "TEXT",        None),
    ("journal_norm_api_source",         "TEXT",        None),
    ("journal_norm_issn_key",           "TEXT",        None),
    ("journal_norm_version",            "TEXT",        None),
    ("journal_norm_processed_at",       "TIMESTAMPTZ", None),
]


# ═══════════════════════════════════════════════════════════════════════
# Pomocné funkcie
# ═══════════════════════════════════════════════════════════════════════

def _to_list(val: Any) -> list[str]:
    """PostgreSQL TEXT[] (alebo string) → Python list neprázdnych stringov."""
    if val is None:
        return []
    if isinstance(val, (list, tuple)):
        return [str(v).strip() for v in val if v and str(v).strip()]
    s = str(val).strip()
    return [s] if s else []


def _get_text(val: Any) -> str:
    """Prvý neprázdny string z pg array hodnoty."""
    for v in _to_list(val):
        return v
    return ""


def _parse_issns(val: Any) -> list[str]:
    """Extrahuje individuálne ISSN reťazce (formát NNNN-NNNN) z TEXT[] hodnoty."""
    issns: list[str] = []
    for item in _to_list(val):
        for part in item.split(","):
            clean = part.strip()
            if len(clean) == 9 and clean[4] == "-":
                issns.append(clean.lower())
    return list(dict.fromkeys(issns))   # deduplikácia, zachovanie poradia


def _parse_isbns(val: Any) -> list[str]:
    """Extrahuje ISBN-10 / ISBN-13 reťazce (len cifry) z TEXT[] hodnoty."""
    isbns: list[str] = []
    for item in _to_list(val):
        for part in item.split(","):
            clean = part.strip().replace("-", "").replace(" ", "")
            if len(clean) in (10, 13) and (clean[:-1].isdigit()):
                isbns.append(clean)
    return list(dict.fromkeys(isbns))


def _matches(val: Any, canonical: str) -> bool:
    """
    True ak val (pg array) predstavuje presne jeden kanonický string.
    Teda: po deduplikácii = [canonical] → žiadna zmena potrebná.
    """
    unique = list(dict.fromkeys(v for v in _to_list(val) if v))
    return len(unique) == 1 and unique[0] == canonical


# ═══════════════════════════════════════════════════════════════════════
# Setup
# ═══════════════════════════════════════════════════════════════════════

def setup_journal_columns(engine: Engine | None = None) -> None:
    engine = engine or get_local_engine()
    schema = settings.local_schema
    table  = settings.local_table
    existing = {col["name"] for col in inspect(engine).get_columns(table, schema=schema)}

    with engine.begin() as conn:
        for col_name, col_type, col_default in _SETUP_COLS:
            if col_name in existing:
                continue
            default_sql = f" DEFAULT {col_default}" if col_default else ""
            conn.execute(text(f"""
                ALTER TABLE "{schema}"."{table}"
                ADD COLUMN IF NOT EXISTS "{col_name}" {col_type}{default_sql}
            """))
            print(f"  + {col_name} ({col_type})")

    print("[OK] Journal norm stĺpce pripravené.")


# ═══════════════════════════════════════════════════════════════════════
# Fallback – kanonická hodnota z existujúcich záznamov
# ═══════════════════════════════════════════════════════════════════════

def _pick_canonical_from_existing(
    rows: list,
) -> tuple[str | None, str | None, str]:
    """
    Keď API zlyhá: vyber kanonickú (publisher, ispartof) z existujúcich záznamov.
    Preferujeme záznamy so Scopus afiliáciou → WoS → najpočetnejšia hodnota celkovo.
    Vracia (publisher, ispartof, source_label).
    """
    def most_common(recs: list, attr: str) -> str | None:
        vals = [_get_text(getattr(r, attr, None)) for r in recs]
        vals = [v for v in vals if v]
        return Counter(vals).most_common(1)[0][0] if vals else None

    scopus = [r for r in rows if _get_text(getattr(r, "scopus_aff", None))]
    if scopus:
        return most_common(scopus, "publisher"), most_common(scopus, "ispartof"), "existing_scopus"

    wos = [r for r in rows if _get_text(getattr(r, "wos_aff", None))]
    if wos:
        return most_common(wos, "publisher"), most_common(wos, "ispartof"), "existing_wos"

    return most_common(rows, "publisher"), most_common(rows, "ispartof"), "existing_common"


# ═══════════════════════════════════════════════════════════════════════
# Lookup runner
# ═══════════════════════════════════════════════════════════════════════

def run_journal_lookup(
    engine:    Engine | None = None,
    limit:     int           = 0,
    reprocess: bool          = False,
) -> None:
    """
    Pre každú ISSN/ISBN skupinu lookupuje API a ukladá návrhy normalizácie.
    limit = max počet ISSN/ISBN skupín (0 = všetky).
    """
    engine = engine or get_local_engine()
    schema = settings.local_schema
    table  = settings.local_table

    statuses = [JournalNormStatus.NOT_PROCESSED]
    if reprocess:
        statuses += [
            JournalNormStatus.NO_CHANGE,
            JournalNormStatus.HAS_PROPOSAL,
            JournalNormStatus.ERROR,
        ]

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT resource_id,
                   "dc.identifier.issn"     AS issn_arr,
                   "dc.identifier.isbn"     AS isbn_arr,
                   "dc.publisher"           AS publisher,
                   "dc.relation.ispartof"   AS ispartof,
                   "utb.scopus.affiliation" AS scopus_aff,
                   "utb.wos.affiliation"    AS wos_aff
            FROM "{schema}"."{table}"
            WHERE journal_norm_status = ANY(:s)
            ORDER BY resource_id
        """), {"s": statuses}).fetchall()

    if not rows:
        print("[INFO] Žiadne záznamy na spracovanie.")
        return

    print(f"[INFO] Načítaných záznamov: {len(rows)}")

    # ── Zoskupenie podľa ISSN / ISBN ──────────────────────────────────
    issn_groups: dict[str, list] = defaultdict(list)
    isbn_groups: dict[str, list] = defaultdict(list)
    no_id:       list            = []

    for row in rows:
        issns = _parse_issns(row.issn_arr)
        isbns = _parse_isbns(row.isbn_arr)
        if issns:
            issn_groups[issns[0]].append(row)
        elif isbns:
            isbn_groups[isbns[0]].append(row)
        else:
            no_id.append(row)

    print(f"[INFO] ISSN skupín: {len(issn_groups)} | ISBN skupín: {len(isbn_groups)} | bez ID: {len(no_id)}")

    if no_id:
        _batch_set_status(engine, [r.resource_id for r in no_id], JournalNormStatus.NO_CHANGE)
        print(f"  Záznamy bez ISSN/ISBN označené ako no_change: {len(no_id)}")

    updates:     list[dict] = []
    groups_done: int        = 0

    # ── ISSN skupiny ──────────────────────────────────────────────────
    for issn_key, grp in issn_groups.items():
        if 0 < limit <= groups_done:
            break

        api_result = _api_lookup_issn(issn_key, grp)

        if api_result:
            canon_pub  = api_result.publisher
            canon_isp  = api_result.title
            api_source = api_result.source
        else:
            canon_pub, canon_isp, api_source = _pick_canonical_from_existing(grp)

        for row in grp:
            updates.append(_build_update(row, issn_key, canon_pub, canon_isp, api_source))

        groups_done += 1
        proposals = sum(
            1 for u in updates[-len(grp):]
            if u["journal_norm_status"] == JournalNormStatus.HAS_PROPOSAL
        )
        print(f"  [{groups_done:3d}] ISSN {issn_key:<12} | {api_source:<20} | "
              f"{len(grp)} záznamov | {proposals} návrhov")

    # ── ISBN skupiny ──────────────────────────────────────────────────
    for isbn_key, grp in isbn_groups.items():
        if 0 < limit <= groups_done:
            break

        all_isbns = list(dict.fromkeys(
            isbn for r in grp for isbn in _parse_isbns(r.isbn_arr)
        ))
        api_result = None
        for isbn in all_isbns:
            api_result = lookup_by_isbn(isbn)
            if api_result:
                break
            time.sleep(0.1)

        if api_result:
            canon_pub  = api_result.publisher
            canon_isp  = api_result.title
            api_source = api_result.source
        else:
            canon_pub, canon_isp, api_source = _pick_canonical_from_existing(grp)

        for row in grp:
            updates.append(_build_update(row, isbn_key, canon_pub, canon_isp, api_source))

        groups_done += 1
        proposals = sum(
            1 for u in updates[-len(grp):]
            if u["journal_norm_status"] == JournalNormStatus.HAS_PROPOSAL
        )
        print(f"  [ISBN] {isbn_key:<20} | {api_source:<20} | "
              f"{len(grp)} záznamov | {proposals} návrhov")

    # ── Zápis ─────────────────────────────────────────────────────────
    if updates:
        _write_updates(engine, updates)

    total_proposals = sum(
        1 for u in updates if u["journal_norm_status"] == JournalNormStatus.HAS_PROPOSAL
    )
    total_no_change = sum(
        1 for u in updates if u["journal_norm_status"] == JournalNormStatus.NO_CHANGE
    )
    print(f"\n[OK] Lookup hotový. Skupín: {groups_done} | "
          f"S návrhom: {total_proposals} | Bez zmeny: {total_no_change}")


def _api_lookup_issn(issn_key: str, grp: list) -> LookupResult | None:
    """Skúsi lookup pre každé ISSN záznamu (nie len prvé) kým nenájde výsledok."""
    all_issns = list(dict.fromkeys(
        issn for r in grp for issn in _parse_issns(r.issn_arr)
    ))
    for issn in all_issns:
        result = lookup_by_issn(issn)
        if result:
            return result
        time.sleep(0.1)
    return None


def _build_update(
    row,
    issn_key:  str,
    canon_pub: str | None,
    canon_isp: str | None,
    api_source: str,
) -> dict:
    """Zostaví update dict pre jeden záznam: porovná aktuálnu hodnotu s kanonickou."""
    proposed_pub = canon_pub if (canon_pub and not _matches(row.publisher, canon_pub)) else None
    proposed_isp = canon_isp if (canon_isp and not _matches(row.ispartof, canon_isp)) else None

    status = (
        JournalNormStatus.HAS_PROPOSAL
        if (proposed_pub is not None or proposed_isp is not None)
        else JournalNormStatus.NO_CHANGE
    )
    return {
        "resource_id":                    row.resource_id,
        "journal_norm_status":            status,
        "journal_norm_proposed_publisher": proposed_pub,
        "journal_norm_proposed_ispartof":  proposed_isp,
        "journal_norm_api_source":         api_source,
        "journal_norm_issn_key":           issn_key,
        "journal_norm_version":            JOURNAL_NORM_VERSION,
        "journal_norm_processed_at":       datetime.now(timezone.utc),
    }


def _write_updates(engine: Engine, updates: list[dict]) -> None:
    schema = settings.local_schema
    table  = settings.local_table
    sql = f"""
        UPDATE "{schema}"."{table}"
        SET
            journal_norm_status             = %s,
            journal_norm_proposed_publisher = %s,
            journal_norm_proposed_ispartof  = %s,
            journal_norm_api_source         = %s,
            journal_norm_issn_key           = %s,
            journal_norm_version            = %s,
            journal_norm_processed_at       = %s
        WHERE resource_id = %s
    """
    params = [
        (
            u["journal_norm_status"],
            u["journal_norm_proposed_publisher"],
            u["journal_norm_proposed_ispartof"],
            u["journal_norm_api_source"],
            u["journal_norm_issn_key"],
            u["journal_norm_version"],
            u["journal_norm_processed_at"],
            u["resource_id"],
        )
        for u in updates
    ]
    raw = engine.raw_connection()
    try:
        with raw.cursor() as cur:
            cur.executemany(sql, params)
        raw.commit()
    finally:
        raw.close()


def _batch_set_status(engine: Engine, resource_ids: list[int], status: str) -> None:
    schema = settings.local_schema
    table  = settings.local_table
    raw = engine.raw_connection()
    try:
        with raw.cursor() as cur:
            cur.executemany(
                f'UPDATE "{schema}"."{table}" SET journal_norm_status = %s WHERE resource_id = %s',
                [(status, rid) for rid in resource_ids],
            )
        raw.commit()
    finally:
        raw.close()


# ═══════════════════════════════════════════════════════════════════════
# Apply runner
# ═══════════════════════════════════════════════════════════════════════

def run_journal_apply(
    engine:      Engine | None = None,
    preview:     bool          = False,
    interactive: bool          = False,
    limit:       int           = 0,
    issn_filter: str | None    = None,
) -> None:
    """
    Zobrazí navrhnuté zmeny a aplikuje schválené (po potvrdení knihovníkom).

    --preview     : zobraziť diff bez akýchkoľvek zmien
    --interactive : zobraziť každú ISSN skupinu a opýtať sa y/n
    default       : zobraziť všetky skupiny, potom jedno spoločné potvrdenie
    --issn XXXX   : spracovať len konkrétnu ISSN/ISBN skupinu
    """
    engine = engine or get_local_engine()
    schema = settings.local_schema
    table  = settings.local_table

    where  = "journal_norm_status = :st"
    params: dict = {"st": JournalNormStatus.HAS_PROPOSAL}
    if issn_filter:
        where += " AND journal_norm_issn_key = :issn"
        params["issn"] = issn_filter.lower().strip()

    limit_sql = f"LIMIT {limit}" if limit > 0 else ""

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT resource_id,
                   "dc.publisher"             AS current_publisher,
                   "dc.relation.ispartof"     AS current_ispartof,
                   journal_norm_proposed_publisher,
                   journal_norm_proposed_ispartof,
                   journal_norm_api_source,
                   journal_norm_issn_key
            FROM "{schema}"."{table}"
            WHERE {where}
            ORDER BY journal_norm_issn_key, resource_id
            {limit_sql}
        """), params).fetchall()

    if not rows:
        print("[INFO] Žiadne záznamy s návrhom zmien (journal_norm_status='has_proposal').")
        return

    # Zoskupenie podľa ISSN/ISBN kľúča
    groups: dict[str, list] = defaultdict(list)
    for r in rows:
        groups[r.journal_norm_issn_key or "?"].append(r)

    mode_label = "PREVIEW" if preview else ("INTERAKTÍVNY" if interactive else "BATCH")
    print(f"\n{'═'*68}")
    print(f"  Normalizácia publisher / relation.ispartof  [{mode_label}]")
    print(f"  ISSN/ISBN skupín: {len(groups)}  │  Záznamov na zmenu: {len(rows)}")
    print(f"{'═'*68}\n")

    # ── PREVIEW: zobraziť všetko, nič nezapisovať ──────────────────────
    if preview:
        for issn_key, grp in groups.items():
            _print_group(issn_key, grp)
        print(f"[PREVIEW] Zobrazených {len(groups)} skupín, {len(rows)} záznamov. Žiadne zmeny nezapísané.")
        return

    # ── INTERAKTÍVNY: per-skupinová konfirmácia ────────────────────────
    if interactive:
        to_apply: list = []
        for issn_key, grp in groups.items():
            _print_group(issn_key, grp)
            ans = input("  Aplikovať túto skupinu? [y/N]: ").strip().lower()
            if ans == "y":
                to_apply.extend(grp)
                print(f"  {_GREEN}✓ Zaradené ({len(grp)} záznamov).{_RST}\n")
            else:
                print(f"  {_DIM}Preskočené.{_RST}\n")

        if to_apply:
            _apply_rows(engine, to_apply)
            skipped = len(rows) - len(to_apply)
            print(f"\n[OK] Aplikovaných: {len(to_apply)} | Preskočených: {skipped}")
        else:
            print("\n[INFO] Žiadne zmeny aplikované.")
        return

    # ── BATCH: zobraziť všetko, jedno spoločné potvrdenie ─────────────
    for issn_key, grp in groups.items():
        _print_group(issn_key, grp)

    ans = input(
        f"Aplikovať VŠETKY zmeny ({len(rows)} záznamov v {len(groups)} skupinách)? [y/N]: "
    ).strip().lower()
    if ans == "y":
        _apply_rows(engine, list(rows))
        print(f"\n[OK] Aplikovaných {len(rows)} záznamov.")
    else:
        print("\n[INFO] Zrušené. Žiadne zmeny nezapísané.")


def _print_group(issn_key: str, grp: list) -> None:
    """Farebný diff pre jednu ISSN/ISBN skupinu."""
    api_src = grp[0].journal_norm_api_source or "?"

    pub_changes = [
        (_get_text(r.current_publisher), r.journal_norm_proposed_publisher)
        for r in grp
        if r.journal_norm_proposed_publisher
    ]
    isp_changes = [
        (_get_text(r.current_ispartof), r.journal_norm_proposed_ispartof)
        for r in grp
        if r.journal_norm_proposed_ispartof
    ]

    print(f"{_BOLD}{'─'*68}{_RST}")
    print(f"  {_BOLD}ISSN/ISBN:{_RST} {issn_key}  │  {_BOLD}Zdroj:{_RST} {api_src}  │  {len(grp)} záznamov na zmenu")
    print(f"{'─'*68}")

    if pub_changes:
        proposed = pub_changes[0][1]   # rovnaká pre celú skupinu
        counts   = Counter(curr for curr, _ in pub_changes)
        print(f"  {_BOLD}dc.publisher:{_RST}")
        for val, cnt in counts.most_common():
            display = val if val else "(prázdne)"
            print(f"    {_RED}─ {display!r}  ({cnt}×){_RST}")
        print(f"    {_GREEN}→ {proposed!r}{_RST}")

    if isp_changes:
        proposed = isp_changes[0][1]
        counts   = Counter(curr for curr, _ in isp_changes)
        print(f"  {_BOLD}dc.relation.ispartof:{_RST}")
        for val, cnt in counts.most_common():
            display = val if val else "(prázdne)"
            print(f"    {_RED}─ {display!r}  ({cnt}×){_RST}")
        print(f"    {_GREEN}→ {proposed!r}{_RST}")

    print()


def _apply_rows(engine: Engine, rows: list) -> None:
    """Zapíše kanonické hodnoty do dc.publisher / dc.relation.ispartof."""
    schema = settings.local_schema
    table  = settings.local_table

    sql_both = f"""
        UPDATE "{schema}"."{table}"
        SET "dc.publisher"         = %s,
            "dc.relation.ispartof" = %s,
            journal_norm_status    = %s
        WHERE resource_id = %s
    """
    sql_pub = f"""
        UPDATE "{schema}"."{table}"
        SET "dc.publisher"      = %s,
            journal_norm_status = %s
        WHERE resource_id = %s
    """
    sql_isp = f"""
        UPDATE "{schema}"."{table}"
        SET "dc.relation.ispartof" = %s,
            journal_norm_status    = %s
        WHERE resource_id = %s
    """
    applied = JournalNormStatus.APPLIED
    raw = engine.raw_connection()
    try:
        with raw.cursor() as cur:
            for r in rows:
                pub = r.journal_norm_proposed_publisher
                isp = r.journal_norm_proposed_ispartof
                if pub and isp:
                    cur.execute(sql_both, ([pub], [isp], applied, r.resource_id))
                elif pub:
                    cur.execute(sql_pub,  ([pub], applied, r.resource_id))
                elif isp:
                    cur.execute(sql_isp,  ([isp], applied, r.resource_id))
        raw.commit()
    finally:
        raw.close()


# ═══════════════════════════════════════════════════════════════════════
# Štatistiky
# ═══════════════════════════════════════════════════════════════════════

def print_journal_status(engine: Engine | None = None) -> None:
    engine = engine or get_local_engine()
    schema = settings.local_schema
    table  = settings.local_table

    with engine.connect() as conn:
        status_rows = conn.execute(text(f"""
            SELECT journal_norm_status, COUNT(*) AS cnt
            FROM "{schema}"."{table}"
            GROUP BY journal_norm_status
            ORDER BY cnt DESC
        """)).fetchall()

        source_rows = conn.execute(text(f"""
            SELECT journal_norm_api_source, COUNT(*) AS cnt
            FROM "{schema}"."{table}"
            WHERE journal_norm_api_source IS NOT NULL
            GROUP BY journal_norm_api_source
            ORDER BY cnt DESC
        """)).fetchall()

        top_groups = conn.execute(text(f"""
            SELECT journal_norm_issn_key, COUNT(*) AS cnt
            FROM "{schema}"."{table}"
            WHERE journal_norm_status = 'has_proposal'
              AND journal_norm_issn_key IS NOT NULL
            GROUP BY journal_norm_issn_key
            ORDER BY cnt DESC
            LIMIT 10
        """)).fetchall()

    print("\n=== Štatistiky normalizácie publisher / ispartof ===")
    print("\n  Stav záznamov:")
    for r in status_rows:
        print(f"    {(r.journal_norm_status or 'NULL'):20s} : {r.cnt:6d}")

    if source_rows:
        print("\n  API zdroje:")
        for r in source_rows:
            print(f"    {(r.journal_norm_api_source or 'NULL'):25s} : {r.cnt:6d}")

    if top_groups:
        print("\n  Top ISSN skupiny s návrhmi:")
        for r in top_groups:
            print(f"    {(r.journal_norm_issn_key or '?'):15s} : {r.cnt:4d} záznamov")
