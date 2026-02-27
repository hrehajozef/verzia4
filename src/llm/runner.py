"""LLM runner – spracuje záznamy s needs_llm=TRUE.

Výstupné stĺpce sú TEXT[] (PostgreSQL arrays).
Pydantic validácia prebehne PRED zápisom do DB.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone

from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.authors.internal import InternalAuthor, get_author_registry
from src.common.constants import LLMStatus
from src.config.settings import settings
from src.db.engines import get_local_engine
from src.llm.client import LLMClient, get_llm_client, parse_llm_json_output
from src.llm.prompt import SYSTEM_PROMPT, LLMResult, build_user_message


# -----------------------------------------------------------------------
# Filtrovanie LLM výstupu podľa whitelist registra
# -----------------------------------------------------------------------

def _filter_by_registry(llm_result: LLMResult, registry: list[InternalAuthor]) -> LLMResult:
    """
    Ponechá iba autorov, ktorých meno je v internom registri.
    Chráni pred halucináciami LLM.
    """
    allowed = {a.full_name for a in registry}
    filtered = [e for e in llm_result.internal_authors if e.name in allowed]
    return LLMResult(internal_authors=filtered)


# -----------------------------------------------------------------------
# Spracovanie jedného záznamu
# -----------------------------------------------------------------------

def process_llm_record(
    resource_id: int,
    wos_aff:     list[str] | None,
    scopus_aff:  list[str] | None,
    flags:       dict      | None,
    llm_client:  LLMClient,
    registry:    list[InternalAuthor],
) -> dict:
    """
    Spracuje jeden záznam cez LLM.

    Postup:
    1. Zostaví prompt (WoS + Scopus + flagy + whitelist mien)
    2. Zavolá LLM
    3. Parsuje JSON výstup
    4. Pydantic validácia (LLMResult)
    5. Filtruje autorov podľa registra
    6. Vráti dict pripravený na DB UPDATE

    TEXT[] výstupné polia:
      final_authors   → utb_contributor_internalauthor (append k heuristikám)
      final_faculties → utb_faculty
      final_ous       → utb_ou
    """
    result: dict = {
        "resource_id":      resource_id,
        "llm_status":       LLMStatus.ERROR,
        "llm_result":       None,
        "llm_processed_at": datetime.now(timezone.utc),
        "final_authors":    None,
        "final_faculties":  None,
        "final_ous":        None,
    }

    raw_output = ""
    try:
        allowed_names = [a.full_name for a in registry]
        wos_text      = "\n---\n".join(str(i) for i in (wos_aff or []) if i) or None
        scopus_text   = "; ".join(str(i)      for i in (scopus_aff or []) if i) or None

        user_msg = build_user_message(
            resource_id              = resource_id,
            wos_affiliation          = wos_text,
            scopus_affiliation       = scopus_text,
            flags                    = flags or {},
            allowed_internal_authors = allowed_names,
        )

        raw_output  = llm_client.complete(SYSTEM_PROMPT, user_msg)
        parsed_dict = parse_llm_json_output(raw_output)

        # --- Pydantic validácia: zahodí neplatné záznamy, nezdvihne výnimku ---
        llm_result  = LLMResult(**parsed_dict)

        # --- Sekundárna ochrana: whitelist filter ---
        llm_result  = _filter_by_registry(llm_result, registry)

        # Extrakcia polí – TEXT[] ako Python list
        authors     = [e.name    for e in llm_result.internal_authors]
        faculties   = list(dict.fromkeys(filter(None, [e.faculty for e in llm_result.internal_authors])))
        ous         = list(dict.fromkeys(filter(None, [e.ou      for e in llm_result.internal_authors])))

        result.update({
            "llm_status":       LLMStatus.PROCESSED,
            "llm_result":       llm_result.model_dump(),
            "final_authors":    authors   or None,
            "final_faculties":  faculties or None,
            "final_ous":        ous       or None,
        })

    except ValidationError as exc:
        result["llm_status"] = LLMStatus.VALIDATION_ERROR
        result["llm_result"] = {"error": str(exc), "raw": raw_output[:2000]}

    except Exception as exc:
        result["llm_status"] = LLMStatus.ERROR
        result["llm_result"] = {"error": str(exc), "raw": raw_output[:500]}

    return result


# -----------------------------------------------------------------------
# Dávkové spracovanie
# -----------------------------------------------------------------------

def run_llm(
    engine:     Engine | None = None,
    batch_size: int | None    = None,
    limit:      int           = 0,
    provider:   str | None    = None,
) -> None:
    engine     = engine     or get_local_engine()
    batch_size = batch_size or settings.llm_batch_size
    schema     = settings.local_schema
    table      = settings.local_table

    llm_client = get_llm_client(provider)
    registry   = get_author_registry(engine)

    with engine.connect() as conn:
        total = conn.execute(
            text(
                f'SELECT COUNT(*) FROM "{schema}"."{table}"'
                " WHERE needs_llm = TRUE AND llm_status IN (:np, :err)"
            ),
            {"np": LLMStatus.NOT_PROCESSED, "err": LLMStatus.ERROR},
        ).scalar_one()

    if limit > 0:
        total = min(total, limit)
    if total == 0:
        print("[INFO] Žiadne záznamy na LLM spracovanie.")
        return

    print(f"[INFO] LLM záznamov na spracovanie: {total}")
    processed = 0
    errors    = 0
    started   = time.time()

    while processed < total:
        batch = min(batch_size, total - processed)
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    f"""
                    SELECT resource_id,
                           "utb.wos.affiliation"    AS wos_aff,
                           "utb.scopus.affiliation"  AS scopus_aff,
                           flags
                    FROM "{schema}"."{table}"
                    WHERE needs_llm = TRUE
                      AND llm_status IN (:np, :err)
                    ORDER BY resource_id
                    LIMIT :lim
                    """
                ),
                {"np": LLMStatus.NOT_PROCESSED, "err": LLMStatus.ERROR, "lim": batch},
            ).fetchall()

        if not rows:
            break

        updates = [
            process_llm_record(
                resource_id = row.resource_id,
                wos_aff     = row.wos_aff,
                scopus_aff  = row.scopus_aff,
                flags       = row.flags or {},
                llm_client  = llm_client,
                registry    = registry,
            )
            for row in rows
        ]
        errors += sum(1 for u in updates if u["llm_status"] != LLMStatus.PROCESSED)

        # UPDATE:
        #   llm_result, llm_status, llm_processed_at – vždy prepíše
        #   utb_contributor_internalauthor, utb_faculty, utb_ou – COALESCE:
        #     ak LLM vrátilo výsledok, prepíše (ARRAY_CAT s heuristikou);
        #     ak nie, zachová pôvodnú heuristickú hodnotu
        update_sql = f"""
            UPDATE "{schema}"."{table}"
            SET
                llm_result                     = %s::jsonb,
                llm_status                     = %s,
                llm_processed_at               = %s,
                utb_contributor_internalauthor = COALESCE(%s, utb_contributor_internalauthor),
                utb_faculty                    = COALESCE(%s, utb_faculty),
                utb_ou                         = COALESCE(%s, utb_ou)
            WHERE resource_id = %s
        """
        # psycopg3 preloží Python list na TEXT[] automaticky
        params = [
            (
                json.dumps(u["llm_result"], ensure_ascii=False) if u["llm_result"] else None,
                u["llm_status"],
                u["llm_processed_at"],
                u["final_authors"],
                u["final_faculties"],
                u["final_ous"],
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
        speed      = processed / max(time.time() - started, 1)
        print(f"  Spracované: {processed}/{total} | chyby: {errors} | {speed:.1f} záz/s")

        # Rate limit pauza pre cloud providery
        if (provider or settings.llm_provider or "").lower() != "ollama":
            time.sleep(0.5)

    print(f"[OK] LLM hotové. Spracovaných: {processed}, chýb: {errors}")