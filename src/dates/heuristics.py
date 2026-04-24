"""Heuristický runner pre dátumy (utb_fulltext_dates).

Spravuje:
  1. Migráciu DB – pridanie DATE a LLM stĺpcov do lokálnej tabuľky
  2. Dávkové spracovanie záznamov (čítanie → parsovanie → zápis)

DB stĺpce (vytvorí setup-processing-queue):
  utb_date_received         DATE
  utb_date_reviewed         DATE
  utb_date_accepted         DATE
  utb_date_published_online DATE
  utb_date_published        DATE
  utb_date_extra            JSONB
  date_heuristic_status     TEXT    – 'not_processed' | 'processed' | 'needs_llm' | 'error'
  date_needs_llm            BOOLEAN
  date_flags                JSONB
  date_heuristic_version    TEXT
  date_processed_at         TIMESTAMPTZ
  date_llm_status           TEXT    – 'not_processed' | 'processed' | 'error'
  date_llm_processed_at     TIMESTAMPTZ
  date_llm_result           JSONB
"""

from __future__ import annotations

import json
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.common.constants import QUEUE_TABLE
from src.dates.parser import ParsedDates, parse_fulltext_dates
from src.db.engines import get_local_engine
from src.config.settings import settings

DATE_HEURISTIC_VERSION = "1.0.0"

# -----------------------------------------------------------------------
# SQL definície stĺpcov
# -----------------------------------------------------------------------

DATE_COLUMNS: list[tuple[str, str, str | None]] = [
    ("utb_date_received",         "DATE",        None),
    ("utb_date_reviewed",         "DATE",        None),
    ("utb_date_accepted",         "DATE",        None),
    ("utb_date_published_online", "DATE",        None),
    ("utb_date_published",        "DATE",        None),
    ("utb_date_extra",            "JSONB",       None),
    ("date_heuristic_status",     "TEXT",        "'not_processed'"),
    ("date_needs_llm",            "BOOLEAN",     "FALSE"),
    ("date_flags",                "JSONB",       "'{}'::jsonb"),
    ("date_heuristic_version",    "TEXT",        None),
    ("date_processed_at",         "TIMESTAMPTZ", None),
    # LLM stĺpce pre dátumy
    ("date_llm_status",           "TEXT",        "'not_processed'"),
    ("date_llm_processed_at",     "TIMESTAMPTZ", None),
    ("date_llm_result",           "JSONB",       None),
]


def setup_date_columns(engine: Engine | None = None) -> None:
    """
    DATE stĺpce sú teraz v utb_processing_queue.
    Spusti 'setup-processing-queue' namiesto tohto príkazu.
    """
    print("[INFO] DATE stĺpce sú v utb_processing_queue. Spusti 'setup-processing-queue'.")
    print("[INFO] Samostatný setup dátumov bol odstránený z CLI.")


# -----------------------------------------------------------------------
# Dávkové spracovanie
# -----------------------------------------------------------------------

def run_date_heuristics(
    engine:     Engine | None = None,
    batch_size: int           = 200,
    limit:      int           = 0,
    reprocess:  bool          = False,
) -> None:
    """
    Spustí heuristické parsovanie dátumov pre všetky záznamy.

    Argumenty:
      engine      – SQLAlchemy engine (použije lokálny ak None)
      batch_size  – počet záznamov v jednej dávke
      limit       – max počet záznamov (0 = všetky)
      reprocess   – ak True, spracuje aj záznamy s chybou
    """
    engine = engine or get_local_engine()
    schema = settings.local_schema
    table  = settings.local_table
    queue  = QUEUE_TABLE

    statuses = ["not_processed"]
    if reprocess:
        statuses.append("error")

    with engine.connect() as conn:
        total = conn.execute(
            text(f"""
                SELECT COUNT(*)
                FROM "{schema}"."{queue}" q
                JOIN "{schema}"."{table}" m ON q.resource_id = m.resource_id
                WHERE q.date_heuristic_status = ANY(:s)
                  AND m."utb.fulltext.dates" IS NOT NULL
            """),
            {"s": statuses},
        ).scalar_one()

    if limit > 0:
        total = min(total, limit)

    if total == 0:
        print("[INFO] Žiadne záznamy na spracovanie dátumov.")
        return

    print(f"[INFO] Záznamov na spracovanie dátumov: {total}")

    update_sql = f"""
        UPDATE "{schema}"."{queue}"
        SET
            utb_date_received         = %s,
            utb_date_reviewed         = %s,
            utb_date_accepted         = %s,
            utb_date_published_online = %s,
            utb_date_published        = %s,
            utb_date_extra            = %s::jsonb,
            date_heuristic_status     = %s,
            date_needs_llm            = %s,
            date_flags                = %s::jsonb,
            date_heuristic_version    = %s,
            date_processed_at         = %s
        WHERE resource_id = %s
    """

    processed = 0
    errors    = 0

    while processed < total:
        batch = min(batch_size, total - processed)

        with engine.connect() as conn:
            rows = conn.execute(
                text(f"""
                    SELECT
                        m.resource_id,
                        m."utb.fulltext.dates"[1] AS fulltext_dates,
                        m."dc.date.issued"[1]     AS dc_issued
                    FROM "{schema}"."{table}" m
                    JOIN "{schema}"."{queue}" q ON m.resource_id = q.resource_id
                    WHERE q.date_heuristic_status = ANY(:s)
                      AND m."utb.fulltext.dates" IS NOT NULL
                    ORDER BY m.resource_id
                    LIMIT :lim
                """),
                {"s": statuses, "lim": batch},
            ).fetchall()

        if not rows:
            break

        params = []
        for row in rows:
            try:
                result: ParsedDates = parse_fulltext_dates(
                    resource_id = row.resource_id,
                    raw_text    = row.fulltext_dates or "",
                    dc_issued   = row.dc_issued,
                )

                extra_json = None
                if result.flags.get("extra_dates"):
                    extra_json = json.dumps(result.flags["extra_dates"], ensure_ascii=False)

                params.append((
                    result.received,
                    result.reviewed,
                    result.accepted,
                    result.published_online,
                    result.published,
                    extra_json,
                    result.status,
                    result.needs_llm,
                    json.dumps(result.flags, ensure_ascii=False, default=str),
                    DATE_HEURISTIC_VERSION,
                    datetime.now(timezone.utc),
                    row.resource_id,
                ))

            except Exception as exc:
                errors += 1
                params.append((
                    None, None, None, None, None, None,
                    "error",
                    True,
                    json.dumps({"error": f"{type(exc).__name__}: {exc}"}),
                    DATE_HEURISTIC_VERSION,
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
        print(f"  Spracované: {processed}/{total} | chyby: {errors}")

    print(f"[OK] Parsovanie dátumov hotové. Spracovaných: {processed}, chýb: {errors}")


# -----------------------------------------------------------------------
# Štatistiky
# -----------------------------------------------------------------------

def print_date_status(engine: Engine | None = None) -> None:
    """Vypíše štatistiky spracovania dátumov."""
    engine = engine or get_local_engine()
    schema = settings.local_schema
    queue  = QUEUE_TABLE

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT
                date_heuristic_status,
                COUNT(*) AS cnt,
                COUNT(utb_date_received)         AS has_received,
                COUNT(utb_date_accepted)         AS has_accepted,
                COUNT(utb_date_published_online) AS has_pub_online,
                COUNT(utb_date_published)        AS has_published,
                SUM(CASE WHEN date_needs_llm THEN 1 ELSE 0 END) AS needs_llm_cnt
            FROM "{schema}"."{queue}"
            GROUP BY date_heuristic_status
            ORDER BY cnt DESC
        """)).fetchall()

    print("\n=== Štatistiky DATE heuristík ===")
    for r in rows:
        print(
            f"  {r.date_heuristic_status:20s} | celkom: {r.cnt:5d} | "
            f"received: {r.has_received:4d} | accepted: {r.has_accepted:4d} | "
            f"pub_online: {r.has_pub_online:4d} | published: {r.has_published:4d} | "
            f"needs_llm: {r.needs_llm_cnt:4d}"
        )

    # LLM status
    with engine.connect() as conn:
        llm_rows = conn.execute(text(f"""
            SELECT date_llm_status, COUNT(*) AS cnt
            FROM "{schema}"."{queue}"
            WHERE date_needs_llm = TRUE
            GROUP BY date_llm_status
            ORDER BY cnt DESC
        """)).fetchall()

    if llm_rows:
        print("\n=== Štatistiky DATE LLM ===")
        for r in llm_rows:
            print(f"  {(r.date_llm_status or 'NULL'):20s} | {r.cnt}")
