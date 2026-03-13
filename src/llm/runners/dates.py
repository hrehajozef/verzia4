"""LLM runner pre dátumy – spracuje záznamy s date_needs_llm=TRUE."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone

from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.config.settings import settings
from src.db.engines import get_local_engine
from src.llm.client import LLMSession, create_dates_session, get_llm_client, parse_llm_json_output
from src.llm.prompts.dates import DateLLMResult, build_date_user_message

DATE_LLM_VERSION = "1.0.0"


# -----------------------------------------------------------------------
# Spracovanie jedného záznamu
# -----------------------------------------------------------------------

def process_date_llm_record(
    resource_id:   int,
    raw_date_text: str,
    dc_issued:     str | None,
    date_flags:    dict | None,
    session:       LLMSession,
) -> dict:

    result: dict = {
        "resource_id":        resource_id,
        "date_llm_status":    "error",
        "date_llm_result":    None,
        "date_llm_processed_at": datetime.now(timezone.utc),
        "received":           None,
        "reviewed":           None,
        "accepted":           None,
        "published_online":   None,
        "published":          None,
    }

    raw_output = ""
    try:
        user_msg = build_date_user_message(
            resource_id   = resource_id,
            raw_date_text = raw_date_text,
            dc_issued     = dc_issued,
            date_flags    = date_flags or {},
        )

        raw_output  = session.ask(user_msg)
        parsed_dict = parse_llm_json_output(raw_output)
        llm_result  = DateLLMResult(**parsed_dict)

        result.update({
            "date_llm_status":  "processed",
            "date_llm_result":  llm_result.model_dump(),
            "received":         llm_result.to_date("received"),
            "reviewed":         llm_result.to_date("reviewed"),
            "accepted":         llm_result.to_date("accepted"),
            "published_online": llm_result.to_date("published_online"),
            "published":        llm_result.to_date("published"),
        })

    except ValidationError as exc:
        result["date_llm_status"] = "validation_error"
        result["date_llm_result"] = {"error": str(exc), "raw": raw_output[:2000]}

    except Exception as exc:
        result["date_llm_status"] = "error"
        result["date_llm_result"] = {"error": f"{type(exc).__name__}: {exc}", "raw": raw_output[:500]}

    return result


# -----------------------------------------------------------------------
# Dávkové spracovanie
# -----------------------------------------------------------------------

def run_date_llm(
    engine:     Engine | None = None,
    batch_size: int | None    = None,
    limit:      int           = 0,
    provider:   str | None    = None,
    reprocess:  bool          = False,
) -> None:
    """
    Spustí LLM parsovanie dátumov pre záznamy s date_needs_llm=TRUE.

    Args:
        engine:     SQLAlchemy engine (použije lokálny ak None).
        batch_size: Veľkosť dávky.
        limit:      Max počet záznamov (0 = všetky).
        provider:   LLM provider (ollama / openai).
        reprocess:  Ak True, spracuje aj záznamy s chybou.
    """
    engine     = engine     or get_local_engine()
    batch_size = batch_size or settings.llm_batch_size
    schema     = settings.local_schema
    table      = settings.local_table

    llm_client = get_llm_client(provider)
    session    = create_dates_session(llm_client)

    statuses = ["not_processed"]
    if reprocess:
        statuses.append("error")
        statuses.append("validation_error")

    with engine.connect() as conn:
        total = conn.execute(
            text(f"""
                SELECT COUNT(*) FROM "{schema}"."{table}"
                WHERE date_needs_llm = TRUE
                  AND date_llm_status = ANY(:s)
            """),
            {"s": statuses},
        ).scalar_one()

    if limit > 0:
        total = min(total, limit)
    if total == 0:
        print("[INFO] Žiadne záznamy na LLM spracovanie dátumov.")
        return

    print(f"[INFO] LLM dátumov – záznamov na spracovanie: {total}")
    processed = 0
    errors    = 0
    started   = time.time()

    while processed < total:
        batch = min(batch_size, total - processed)

        with engine.connect() as conn:
            rows = conn.execute(
                text(f"""
                    SELECT
                        resource_id,
                        "utb.fulltext.dates"[1] AS fulltext_dates,
                        "dc.date.issued"[1]     AS dc_issued,
                        date_flags
                    FROM "{schema}"."{table}"
                    WHERE date_needs_llm = TRUE
                      AND date_llm_status = ANY(:s)
                    ORDER BY resource_id
                    LIMIT :lim
                """),
                {"s": statuses, "lim": batch},
            ).fetchall()

        if not rows:
            break

        updates = [
            process_date_llm_record(
                resource_id   = row.resource_id,
                raw_date_text = row.fulltext_dates or "",
                dc_issued     = row.dc_issued,
                date_flags    = row.date_flags or {},
                session       = session,
            )
            for row in rows
        ]
        errors += sum(1 for u in updates if u["date_llm_status"] != "processed")

        update_sql = f"""
            UPDATE "{schema}"."{table}"
            SET
                date_llm_result       = %s::jsonb,
                date_llm_status       = %s,
                date_llm_processed_at = %s,
                utb_date_received         = COALESCE(%s, utb_date_received),
                utb_date_reviewed         = COALESCE(%s, utb_date_reviewed),
                utb_date_accepted         = COALESCE(%s, utb_date_accepted),
                utb_date_published_online = COALESCE(%s, utb_date_published_online),
                utb_date_published        = COALESCE(%s, utb_date_published)
            WHERE resource_id = %s
        """
        params = [
            (
                json.dumps(u["date_llm_result"], ensure_ascii=False) if u["date_llm_result"] else None,
                u["date_llm_status"],
                u["date_llm_processed_at"],
                u["received"],
                u["reviewed"],
                u["accepted"],
                u["published_online"],
                u["published"],
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
        speed = processed / max(time.time() - started, 1)
        print(f"  Spracované: {processed}/{total} | chyby: {errors} | {speed:.1f} záz/s")

        if (provider or settings.llm_provider or "").lower() != "ollama":
            time.sleep(5)

    print(f"[OK] LLM dátumov hotové. Spracovaných: {processed}, chýb: {errors}")
