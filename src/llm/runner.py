"""LLM runner pre záznamy s needs_llm=true."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone

from pydantic import ValidationError
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.authors.internal import InternalAuthor, get_author_registry
from src.common.constants import DELIMITER, LLMStatus
from src.config.settings import settings
from src.db.engines import get_local_engine
from src.llm.client import LLMClient, get_llm_client, parse_llm_json_output
from src.llm.prompt import LLMResult, SYSTEM_PROMPT, build_user_message


def _join(items: list[str]) -> str | None:
    """Spojí neprázdne položky cez interný delimiter alebo vráti None."""

    cleaned = [item for item in items if item and item.strip()]
    return DELIMITER.join(cleaned) if cleaned else None


def _filter_llm_authors_by_registry(
    llm_result: LLMResult,
    registry: list[InternalAuthor],
) -> LLMResult:
    """Odstráni autorov, ktorí nie sú v internom UTB registri."""

    allowed_names = {author.full_name for author in registry}
    filtered = [entry for entry in llm_result.internal_authors if entry.name in allowed_names]
    return LLMResult(internal_authors=filtered)


def process_llm_record(
    resource_id: int,
    wos_aff: list[str] | None,
    scopus_aff: list[str] | None,
    flags: dict | None,
    llm_client: LLMClient,
    registry: list[InternalAuthor],
) -> dict:
    """Spracuje jeden záznam cez LLM a vráti dáta pripravené na DB update."""

    result = {
        "resource_id": resource_id,
        "llm_status": LLMStatus.ERROR,
        "llm_result": None,
        "llm_processed_at": datetime.now(timezone.utc),
        "final_authors": None,
        "final_faculties": None,
        "final_ous": None,
    }

    raw_output = ""
    try:
        allowed_names = [author.full_name for author in registry]
        wos_text = "\n---\n".join(str(item) for item in (wos_aff or []) if item) or None
        scopus_text = "; ".join(str(item) for item in (scopus_aff or []) if item) or None
        user_message = build_user_message(
            resource_id=resource_id,
            wos_affiliation=wos_text,
            scopus_affiliation=scopus_text,
            flags=flags or {},
            allowed_internal_authors=allowed_names,
        )

        raw_output = llm_client.complete(SYSTEM_PROMPT, user_message)
        parsed_output = parse_llm_json_output(raw_output)
        llm_result = LLMResult(**parsed_output)
        llm_result = _filter_llm_authors_by_registry(llm_result, registry)

        authors = [author.name for author in llm_result.internal_authors]
        faculties = [author.faculty for author in llm_result.internal_authors]
        ous = [author.ou for author in llm_result.internal_authors]

        result.update(
            {
                "llm_status": LLMStatus.PROCESSED,
                "llm_result": llm_result.model_dump(),
                "final_authors": _join(authors),
                "final_faculties": _join(faculties),
                "final_ous": _join(ous),
            }
        )
    except ValidationError as exc:
        result["llm_status"] = LLMStatus.VALIDATION_ERROR
        result["llm_result"] = {"error": str(exc), "raw": raw_output[:1500]}
    except Exception as exc:
        result["llm_status"] = LLMStatus.ERROR
        result["llm_result"] = {"error": str(exc)}

    return result


def run_llm(
    engine: Engine | None = None,
    batch_size: int | None = None,
    limit: int = 0,
    provider: str | None = None,
) -> None:
    """Spustí LLM fázu dávkovo pre záznamy označené na LLM kontrolu."""

    engine = engine or get_local_engine()
    batch_size = batch_size or settings.llm_batch_size
    schema = settings.local_schema
    table = settings.local_table
    llm_client = get_llm_client(provider)
    registry = get_author_registry(engine)

    with engine.connect() as conn:
        total = conn.execute(
            text(
                f"""
                SELECT COUNT(*) FROM "{schema}"."{table}"
                WHERE needs_llm = TRUE
                  AND llm_status IN (:np, :err)
                """
            ),
            {"np": LLMStatus.NOT_PROCESSED, "err": LLMStatus.ERROR},
        ).scalar_one()

    if limit > 0:
        total = min(total, limit)

    if total == 0:
        print("[INFO] Žiadne záznamy na LLM spracovanie.")
        return

    processed = 0
    errors = 0
    started = time.time()

    while processed < total:
        current_batch = min(batch_size, total - processed)
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    f"""
                    SELECT resource_id,
                           "utb.wos.affiliation" AS wos_aff,
                           "utb.scopus.affiliation" AS scopus_aff,
                           flags
                    FROM "{schema}"."{table}"
                    WHERE needs_llm = TRUE
                      AND llm_status IN (:np, :err)
                    ORDER BY resource_id
                    LIMIT :lim
                    """
                ),
                {"np": LLMStatus.NOT_PROCESSED, "err": LLMStatus.ERROR, "lim": current_batch},
            ).fetchall()

        if not rows:
            break

        updates = [
            process_llm_record(
                resource_id=row.resource_id,
                wos_aff=row.wos_aff,
                scopus_aff=row.scopus_aff,
                flags=row.flags or {},
                llm_client=llm_client,
                registry=registry,
            )
            for row in rows
        ]
        errors += sum(1 for item in updates if item["llm_status"] != LLMStatus.PROCESSED)

        update_sql = f"""
            UPDATE "{schema}"."{table}"
            SET
                llm_result = %s::jsonb,
                llm_status = %s,
                llm_processed_at = %s,
                utb_contributor_internalauthor = COALESCE(%s, utb_contributor_internalauthor),
                utb_faculty = COALESCE(%s, utb_faculty),
                utb_ou = COALESCE(%s, utb_ou)
            WHERE resource_id = %s
        """
        params = [
            (
                json.dumps(item["llm_result"], ensure_ascii=False) if item["llm_result"] else None,
                item["llm_status"],
                item["llm_processed_at"],
                item["final_authors"],
                item["final_faculties"],
                item["final_ous"],
                item["resource_id"],
            )
            for item in updates
        ]

        raw_conn = engine.raw_connection()
        try:
            with raw_conn.cursor() as cursor:
                cursor.executemany(update_sql, params)
            raw_conn.commit()
        finally:
            raw_conn.close()

        processed += len(rows)
        speed = processed / max(time.time() - started, 1)
        print(f"  Spracované: {processed}/{total} | chyby: {errors} | {speed:.1f} záz/s")

        if settings.llm_provider == "openai":
            time.sleep(1.0)

    print(f"[OK] LLM fáza hotová. Spracované: {processed}, chyby: {errors}")
