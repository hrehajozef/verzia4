"""Prompty, Pydantic schémy, JSON Schema a LLM runner pre extrakciu UTB autorov."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.authors.registry import InternalAuthor, _normalize_name, get_author_registry
from src.authors.parsers.wos import normalize_text
from src.common.constants import DEPARTMENTS, FACULTIES, LLMStatus, QUEUE_TABLE
from src.config.settings import settings
from src.db.engines import get_local_engine
from src.llm.session import LLMSession, create_authors_session
from src.llm.client import get_llm_client, parse_llm_json_output


# -----------------------------------------------------------------------
# Pydantic modely
# -----------------------------------------------------------------------

_VALID_FACULTY_NAMES: frozenset[str] = frozenset(FACULTIES.values())
_VALID_DEPT_NAMES:    frozenset[str] = frozenset(DEPARTMENTS.keys())


class LLMAuthorEntry(BaseModel):
    """Jeden interný UTB autor vrátane jeho inštitucionálnej príslušnosti."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(
        ...,
        description="Celé meno autora s diakritikou vo formáte 'Priezvisko, Meno'.",
        min_length=3,
    )
    faculty: str = Field(
        default="",
        description=(
            "Plný anglický názov fakulty UTB. "
            f"Povolené hodnoty: {sorted(_VALID_FACULTY_NAMES)}. "
            "Ak nie je známa, použi prázdny reťazec."
        ),
    )
    ou: str = Field(
        default="",
        description="Plný anglický názov oddelenia/ústavu UTB alebo prázdny reťazec.",
    )

    @field_validator("faculty")
    @classmethod
    def validate_faculty(cls, v: str) -> str:
        if v and v not in _VALID_FACULTY_NAMES:
            return ""
        return v


class LLMResult(BaseModel):
    """Výstupná štruktúra LLM odpovede pre autorov."""

    model_config = ConfigDict(extra="forbid")

    internal_authors: list[LLMAuthorEntry] = Field(
        default_factory=list,
        description="Zoznam interných UTB autorov identifikovaných z afiliácie.",
    )


# -----------------------------------------------------------------------
# JSON Schema pre structured output
# -----------------------------------------------------------------------

AUTHORS_JSON_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["internal_authors"],
    "additionalProperties": False,
    "properties": {
        "internal_authors": {
            "type": "array",
            "description": "Zoznam interných UTB autorov.",
            "items": {
                "type": "object",
                "required": ["name", "faculty", "ou"],
                "additionalProperties": False,
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "Meno vo formáte 'Priezvisko, Meno' s diakritikou.",
                    },
                    "faculty": {
                        "type": "string",
                        "description": "Plný anglický názov fakulty UTB alebo prázdny reťazec.",
                        "enum": list(_VALID_FACULTY_NAMES) + [""],
                    },
                    "ou": {
                        "type": "string",
                        "description": "Plný anglický názov oddelenia/ústavu UTB alebo prázdny reťazec.",
                    },
                },
            },
        }
    },
}

# Backward-compat aliases
LLM_OUTPUT_JSON_SCHEMA = AUTHORS_JSON_SCHEMA

LLM_FUNCTION_DEF: dict[str, Any] = {
    "name":        "extract_utb_authors",
    "description": (
        "Extrahuje interných UTB autorov z textu afiliácie publikácie. "
        "Vráti iba autorov, ktorých meno je v poskytnutom whitelist zozname."
    ),
    "parameters":  AUTHORS_JSON_SCHEMA,
}


# -----------------------------------------------------------------------
# System prompt
# -----------------------------------------------------------------------

_FACULTY_LIST = "\n".join(f"  - {name}" for name in sorted(_VALID_FACULTY_NAMES))
_DEPT_SAMPLE  = "\n".join(
    f"  - {dept} ({fid})"
    for dept, fid in list(DEPARTMENTS.items())[:12]
)

SYSTEM_PROMPT = f"""Si expert na analýzu afiliácií vedeckých publikácií UTB (Tomas Bata University in Zlín).

## Tvoja úloha:
Identifikuj interných UTB autorov z poskytnutých afiliačných textov (WoS a Scopus formát).
Pre každého autora urč fakultu a oddelenie/ústav.

## Pravidlá – MUSÍŠ ich dodržať
1. Do výstupu zaraď LEN autorov, ktorých meno sa nachádza v poskytnutom zozname "Povolené mená".
2. Výstup je VÝHRADNE JSON objekt so štruktúrou: {{"internal_authors": [...]}}
3. Každý autor má POVINNÉ kľúče: "name", "faculty", "ou".
4. "name" musí byť PRESNE meno z whitelistu – s diakritikou, formát "Priezvisko, Meno".
5. "faculty" musí byť jeden z povolených názvov (alebo prázdny reťazec ""):
{_FACULTY_LIST}
6. "ou" je plný anglický názov oddelenia/ústavu (alebo prázdny reťazec "").
7. Ak autor nie je v zozname "Povolené mená", nevypisuj ho – ani ako odhad.
8. Ak nevieš určiť fakultu alebo oddelenie, použi "".
9. Žiadne komentáre, markdown, vysvetlenia – iba JSON.
10. Ak sú WoS mená autorov skrátené (napr. "Novak J" alebo "Novak, J"), porovnaj ich
    s whitelistom podľa priezviska a inicálky mena. Ak priezvisko + inicálka zodpovedajú,
    použi PRESNÉ meno z whitelistu (s diakritikou).

## Príklady oddelení (ukážka)
{_DEPT_SAMPLE}

## WoS formát vstupu
[Priezvisko1, Meno1; Priezvisko2 M] Inštitúcia, Oddelenie, Adresa;
[Priezvisko3, Meno3] Iná inštitúcia, Adresa

## Scopus formát vstupu (bez mien autorov)
Oddelenie, Fakulta, Inštitúcia, Adresa; Oddelenie2, Fakulta2, Inštitúcia2

## Príklad správneho výstupu
{{"internal_authors":[{{"name":"Novák, Jan","faculty":"Faculty of Technology","ou":"Department of Polymer Engineering"}}]}}
"""

# Preamble pre Ollama konverzačný režim (KV-cache optimalizácia – načíta sa raz).
# Príklad musí byť realistický (nie prázdny), aby model videl očakávaný formát odpovede.
AUTHORS_SETUP_PREAMBLE: list[dict] = [
    {
        "role":    "user",
        "content": (
            "=== Záznam resource_id=0 (ukážka) ===\n\n"
            "WoS afiliácia (obsahuje mená autorov):\n"
            "[Novak, Jan; Dvorak, Petr] Tomas Bata Univ Zlin, Fac Technol, "
            "Dept Polymer Engn, Nam TG Masaryka 5555, Zlin 76001, Czech Republic\n\n"
            "Povolené mená interných autorov UTB:\n"
            "[\"Novák, Jan\", \"Dvořák, Petr\", \"Svoboda, Karel\"]\n\n"
            "Vráť JSON objekt obsahujúci kľúč 'internal_authors' "
            "so zoznamom identifikovaných interných autorov."
        ),
    },
    {
        "role":    "assistant",
        "content": (
            '{"internal_authors": ['
            '{"name": "Novák, Jan", "faculty": "Faculty of Technology", "ou": "Department of Polymer Engineering"}, '
            '{"name": "Dvořák, Petr", "faculty": "Faculty of Technology", "ou": "Department of Polymer Engineering"}'
            ']}'
        ),
    },
]


# -----------------------------------------------------------------------
# Zostavenie user promptu
# -----------------------------------------------------------------------

def build_user_message(
    resource_id:              int,
    wos_affiliation:          str | None,
    scopus_affiliation:       str | None,
    flags:                    dict[str, Any] | None,
    allowed_internal_authors: list[str],
) -> str:
    parts: list[str] = [f"=== Záznam resource_id={resource_id} ==="]

    if wos_affiliation:
        parts.append(f"WoS afiliácia (obsahuje mená autorov):\n{wos_affiliation}")
    else:
        parts.append("WoS afiliácia: (nedostupná)")

    if scopus_affiliation:
        parts.append(f"Scopus afiliácia (bez mien, len inštitúcie):\n{scopus_affiliation}")

    if flags:
        relevant = {
            k: flags[k]
            for k in (
                "utb_authors_unmatched",
                "utb_authors_found_count",
                "multiple_utb_blocks",
                "wos_parse_warnings",
                "error",
            )
            if k in flags
        }
        if relevant:
            parts.append(
                "Kontext z heuristík:\n" + json.dumps(relevant, ensure_ascii=False, indent=2)
            )

    parts.append(
        "Povolené mená interných autorov UTB (použi VÝHRADNE tieto mená):\n"
        + json.dumps(allowed_internal_authors, ensure_ascii=False)
    )

    parts.append(
        "Vráť JSON objekt obsahujúci kľúč 'internal_authors' "
        "so zoznamom identifikovaných interných autorov."
    )

    return "\n\n".join(parts)


# -----------------------------------------------------------------------
# Výber relevantných kandidátov z registra
# -----------------------------------------------------------------------

def _select_candidates(
    registry:          list[InternalAuthor],
    unmatched_authors: list[str],
    max_candidates:    int = 80,
) -> list[str]:
    """Vráti relevantných kandidátov z registra pre daných nenájdených autorov."""
    if not unmatched_authors:
        return [a.full_name for a in registry[:max_candidates]]

    candidates: dict[str, float] = {}

    for wos_name in unmatched_authors:
        norm_wos    = normalize_text(wos_name)
        wos_surname = norm_wos.split(",")[0].strip().split()[0] if norm_wos else ""

        for author in registry:
            norm_auth    = _normalize_name(author.full_name)
            auth_surname = norm_auth.split(",")[0].strip().split()[0] if norm_auth else ""

            if wos_surname and auth_surname:
                if wos_surname == auth_surname:
                    candidates[author.full_name] = 1.0
                    continue
                if wos_surname.startswith(auth_surname[:4]) or auth_surname.startswith(wos_surname[:4]):
                    score = len(os.path.commonprefix([wos_surname, auth_surname])) / max(len(wos_surname), len(auth_surname))
                    if score > 0.6:
                        candidates[author.full_name] = max(candidates.get(author.full_name, 0), score)

    sorted_candidates = sorted(candidates.items(), key=lambda x: -x[1])
    result = [name for name, _ in sorted_candidates[:max_candidates]]

    if len(result) < 10:
        extras = [a.full_name for a in registry if a.full_name not in set(result)]
        result.extend(extras[:max_candidates - len(result)])

    return result


# -----------------------------------------------------------------------
# Filter výstupu voči registru (anti-halucinácia)
# -----------------------------------------------------------------------

def _filter_by_registry(llm_result: LLMResult, registry: list[InternalAuthor]) -> LLMResult:
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
    session:     LLMSession,
    registry:    list[InternalAuthor],
) -> dict:

    result: dict = {
        "resource_id":          resource_id,
        "author_llm_status":    LLMStatus.ERROR,
        "author_llm_result":    None,
        "author_llm_processed_at": datetime.now(timezone.utc),
        "final_authors":        None,
        "final_faculties":      None,
        "final_ous":            None,
    }

    unmatched = (flags or {}).get("utb_authors_unmatched", [])
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

    raw_output = ""
    # 1 retry pre prechodné chyby (nevalidný JSON, sieťová chyba).
    # ValidationError sa neretriuje – ide o štrukturálny problém odpovede.
    for attempt in range(2):
        try:
            raw_output  = session.ask(user_msg)
            parsed_dict = parse_llm_json_output(raw_output)
            llm_result  = LLMResult(**parsed_dict)
            llm_result  = _filter_by_registry(llm_result, registry)

            authors   = [e.name    for e in llm_result.internal_authors]
            faculties = list(dict.fromkeys(filter(None, [e.faculty for e in llm_result.internal_authors])))
            ous       = list(dict.fromkeys(filter(None, [e.ou      for e in llm_result.internal_authors])))

            result.update({
                "author_llm_status":  LLMStatus.PROCESSED,
                "author_llm_result":  llm_result.model_dump(),
                "final_authors":      authors   or None,
                "final_faculties":    faculties or None,
                "final_ous":          ous       or None,
            })
            break  # úspech

        except ValidationError as exc:
            result["author_llm_status"] = LLMStatus.VALIDATION_ERROR
            result["author_llm_result"] = {"error": str(exc), "raw": raw_output[:2000]}
            break  # štrukturálna chyba – retry nepomôže

        except Exception as exc:
            if attempt == 0:
                time.sleep(2)
                continue   # jeden retry
            result["author_llm_status"] = LLMStatus.ERROR
            result["author_llm_result"] = {"error": f"{type(exc).__name__}: {exc}", "raw": raw_output[:1000]}

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
    queue      = QUEUE_TABLE

    llm_client = get_llm_client(provider)
    session    = create_authors_session(llm_client)
    registry   = get_author_registry(engine)

    with engine.connect() as conn:
        total = conn.execute(
            text(
                f'SELECT COUNT(*) FROM "{schema}"."{queue}"'
                " WHERE author_needs_llm = TRUE AND author_llm_status IN (:np, :err)"
            ),
            {"np": LLMStatus.NOT_PROCESSED, "err": LLMStatus.ERROR},
        ).scalar_one()

    if limit > 0:
        total = min(total, limit)
    if total == 0:
        print("[INFO] Žiadne záznamy na LLM spracovanie autorov.")
        return

    print(f"[INFO] LLM autorov – záznamov na spracovanie: {total}")
    processed = 0
    errors    = 0
    started   = time.time()

    while processed < total:
        batch = min(batch_size, total - processed)
        with engine.connect() as conn:
            rows = conn.execute(
                text(f"""
                    SELECT q.resource_id,
                           m."utb.wos.affiliation"    AS wos_aff,
                           m."utb.scopus.affiliation" AS scopus_aff,
                           q.author_flags
                    FROM "{schema}"."{queue}" q
                    JOIN "{schema}"."{table}" m ON q.resource_id = m.resource_id
                    WHERE q.author_needs_llm = TRUE
                      AND q.author_llm_status IN (:np, :err)
                    ORDER BY q.resource_id
                    LIMIT :lim
                """),
                {"np": LLMStatus.NOT_PROCESSED, "err": LLMStatus.ERROR, "lim": batch},
            ).fetchall()

        if not rows:
            break

        updates = [
            process_llm_record(
                resource_id = row.resource_id,
                wos_aff     = row.wos_aff,
                scopus_aff  = row.scopus_aff,
                flags       = row.author_flags or {},
                session     = session,
                registry    = registry,
            )
            for row in rows
        ]
        errors += sum(1 for u in updates if u["author_llm_status"] != LLMStatus.PROCESSED)

        update_sql = f"""
            UPDATE "{schema}"."{queue}"
            SET
                author_llm_result    = %s::jsonb,
                author_llm_status    = %s,
                author_llm_processed_at = %s,
                author_internal_names = COALESCE(%s, author_internal_names),
                author_faculty       = COALESCE(%s, author_faculty),
                author_ou            = COALESCE(%s, author_ou)
            WHERE resource_id = %s
        """
        params = [
            (
                json.dumps(u["author_llm_result"], ensure_ascii=False) if u["author_llm_result"] else None,
                u["author_llm_status"],
                u["author_llm_processed_at"],
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
            time.sleep(5)

    print(f"[OK] LLM autorov hotové. Spracovaných: {processed}, chýb: {errors}")
