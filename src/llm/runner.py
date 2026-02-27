"""LLM runner – spracuje záznamy s needs_llm=TRUE."""

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
from src.parsers.wos_affiliation import normalize_text


# -----------------------------------------------------------------------
# Výber relevantných kandidátov z registra
# -----------------------------------------------------------------------

def _select_candidates(
    registry:          list[InternalAuthor],
    unmatched_authors: list[str],
    max_candidates:    int = 80,
) -> list[str]:
    """
    Namiesto celého registra (2 298 mien) vráti len relevantných kandidátov.

    Postup:
    1. Pre každého nenájdeného autora z WoS hľadá v registri autorov
       so zhodným priezviskom (prvé slovo normalizovaného mena).
    2. Ak nenájde dosť kandidátov, doplní najbližších podľa Jaro-Winkler.
    3. Vráti deduplikovaný zoznam max_candidates mien.

    Takto sa prompt skráti z ~45 000 na ~2 000 znakov.
    """
    if not unmatched_authors:
        # Žiadni nenájdení autori → pošli prvých max_candidates zo registra
        # (fallback, zriedkavý prípad)
        return [a.full_name for a in registry[:max_candidates]]

    candidates: dict[str, float] = {}  # full_name → score

    for wos_name in unmatched_authors:
        norm_wos = normalize_text(wos_name)
        # Priezvisko = prvé slovo (WoS formát: "Priezvisko, Meno" alebo "Priezvisko Meno")
        wos_surname = norm_wos.split(",")[0].strip().split()[0] if norm_wos else ""

        for author in registry:
            norm_auth    = author.norm_name
            auth_surname = norm_auth.split(",")[0].strip().split()[0] if norm_auth else ""

            # Rýchla zhoda priezviska - nevyžaduje knižnicu
            if wos_surname and auth_surname:
                # Presná zhoda priezviska
                if wos_surname == auth_surname:
                    candidates[author.full_name] = 1.0
                    continue
                # Čiastočná zhoda - jedno je prefix druhého (napr. "novak" vs "novakova")
                if wos_surname.startswith(auth_surname[:4]) or auth_surname.startswith(wos_surname[:4]):
                    score = len(os.path.commonprefix([wos_surname, auth_surname])) / max(len(wos_surname), len(auth_surname))
                    if score > 0.6:
                        candidates[author.full_name] = max(candidates.get(author.full_name, 0), score)

    # Zoraď podľa skóre, vráť top max_candidates
    sorted_candidates = sorted(candidates.items(), key=lambda x: -x[1])
    result = [name for name, _ in sorted_candidates[:max_candidates]]

    # Ak je kandidátov málo, doplň náhodné zo registra pre pokrytie
    if len(result) < 10:
        extras = [a.full_name for a in registry if a.full_name not in set(result)]
        result.extend(extras[:max_candidates - len(result)])

    return result


# Importuj os pre commonprefix
import os


# -----------------------------------------------------------------------
# Filtrovanie LLM výstupu podľa whitelist registra
# -----------------------------------------------------------------------

def _filter_by_registry(llm_result: LLMResult, registry: list[InternalAuthor]) -> LLMResult:
    """Ponechá iba autorov, ktorých meno je v internom registri (anti-halucinácia)."""
    allowed  = {a.full_name for a in registry}
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
        # Nenájdení autori z heuristík – kľúčový vstup pre výber kandidátov
        unmatched = (flags or {}).get("utb_authors_unmatched", [])

        # Vyber len relevantných kandidátov namiesto celého registra
        candidate_names = _select_candidates(registry, unmatched, max_candidates=80)

        wos_text    = "\n---\n".join(str(i) for i in (wos_aff or []) if i) or None
        scopus_text = "; ".join(str(i) for i in (scopus_aff or []) if i) or None

        user_msg = build_user_message(
            resource_id              = resource_id,
            wos_affiliation          = wos_text,
            scopus_affiliation       = scopus_text,
            flags                    = flags or {},
            allowed_internal_authors = candidate_names,
        )

        raw_output  = llm_client.complete(SYSTEM_PROMPT, user_msg)
        parsed_dict = parse_llm_json_output(raw_output)
        llm_result  = LLMResult(**parsed_dict)
        # Sekundárna ochrana: overenie voči celému registru
        llm_result  = _filter_by_registry(llm_result, registry)

        authors   = [e.name    for e in llm_result.internal_authors]
        faculties = list(dict.fromkeys(filter(None, [e.faculty for e in llm_result.internal_authors])))
        ous       = list(dict.fromkeys(filter(None, [e.ou      for e in llm_result.internal_authors])))

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
        result["llm_result"] = {"error": f"{type(exc).__name__}: {exc}", "raw": raw_output[:500]}

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
                           "utb.wos.affiliation"   AS wos_aff,
                           "utb.scopus.affiliation" AS scopus_aff,
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
        speed = processed / max(time.time() - started, 1)
        print(f"  Spracované: {processed}/{total} | chyby: {errors} | {speed:.1f} záz/s")

        if (provider or settings.llm_provider or "").lower() != "ollama":
            time.sleep(5)  # Pauza pre ne-Ollama LLM, aby sme nepreťažili API

    print(f"[OK] LLM hotové. Spracovaných: {processed}, chýb: {errors}")