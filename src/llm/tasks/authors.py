"""Prompty, Pydantic schémy, JSON Schema a LLM runner pre extrakciu UTB autorov."""

from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from typing import Any

import jellyfish
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.authors.registry import (
    InternalAuthor,
    _normalize_name,
    get_author_registry,
    match_author,
)
from src.authors.parsers.wos import normalize_text
from src.authors.source_authors import split_source_author_lists
from src.authors.workplace_tree import load_workplace_tree, walk_to_faculty
from src.common.constants import (
    CZECH_FACULTY_MAP_NORM,
    CZECH_DEPARTMENT_MAP_NORM,
    DEPARTMENTS,
    FACULTIES,
    LLMStatus,
    QUEUE_TABLE,
    _norm,
)
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
        if not v:
            return ""
        if v in _VALID_FACULTY_NAMES:
            return v
        translated_id = CZECH_FACULTY_MAP_NORM.get(_norm(v))
        if translated_id:
            return FACULTIES.get(translated_id, "")
        return ""

    @field_validator("ou")
    @classmethod
    def validate_ou(cls, v: str, info) -> str:
        if not v:
            return v
        allowed = set(info.context.get("allowed_workplaces", set())) if info.context else set()
        if allowed and v in allowed:
            return v
        if not allowed and v in _VALID_DEPT_NAMES:
            return v
        translated = CZECH_DEPARTMENT_MAP_NORM.get(_norm(v))
        if translated and ((not allowed and translated in _VALID_DEPT_NAMES) or translated in allowed):
            return translated
        if allowed:
            best_match = ""
            best_score = 0.0
            norm_value = _norm(v)
            for candidate in allowed:
                score = jellyfish.jaro_winkler_similarity(norm_value, _norm(candidate))
                if score > best_score:
                    best_score = score
                    best_match = candidate
            if best_score >= 0.92:
                return best_match
        return ""


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

SYSTEM_PROMPT = f"""Si expert na anal?zu afili?ci? vedeck?ch publik?ci? UTB (Tomas Bata University in Zl?n).

## Tvoja ?loha:
Identifikuj intern?ch UTB autorov z poskytnut?ch afilia?n?ch textov (WoS a Scopus form?t).
Pre ka?d?ho autora ur? fakultu a oddelenie/?stav.

## Pravidl? ? MUS?? ich dodr?a?
1. Do v?stupu zara? LEN autorov, ktor?ch meno sa nach?dza v poskytnutom zozname "Povolen? men?".
2. V?stup je V?HRADNE JSON objekt so ?trukt?rou: {{"internal_authors": [...]}}
3. Ka?d? autor m? POVINN? k???e: "name", "faculty", "ou".
4. "name" mus? by? PRESNE meno z whitelistu ? s diakritikou, form?t "Priezvisko, Meno".
5. "faculty" mus? by? jeden z povolen?ch n?zvov (alebo pr?zdny re?azec ""):
{_FACULTY_LIST}
6. "ou" je pln? anglick? n?zov oddelenia/?stavu (alebo pr?zdny re?azec "").
7. Pre "ou" pou??vaj V?HRADNE n?zvy z poskytnut?ho whitelistu pracov?sk (UTB_WORKPLACES). Nikdy neh?daj nov? n?zov OU.
8. Ak autor nie je v zozname "Povolen? men?", nevypisuj ho ? ani ako odhad.
9. Ak nevie? ur?i? fakultu alebo oddelenie, pou?i "".
10. ?iadne koment?re, markdown, vysvetlenia ? iba JSON.
11. Ak s? WoS men? autorov skr?ten? (napr. "Novak J" alebo "Novak, J"), porovnaj ich
    s whitelistom pod?a priezviska a inic?lky mena. Ak priezvisko + inic?lka zodpovedaj?,
    pou?i PRESN? meno z whitelistu (s diakritikou).
12. Scopus vstup m??e ma? nov? form?t "Author, affiliation; Author, affiliation". V takom pr?pade p?ruj autora priamo s jeho afili?ciou.

## Pr?klady oddelen? (uk??ka)
{_DEPT_SAMPLE}

## WoS form?t vstupu
[Priezvisko1, Meno1; Priezvisko2 M] In?tit?cia, Oddelenie, Adresa;
[Priezvisko3, Meno3] In? in?tit?cia, Adresa

## Scopus form?t vstupu
Star?? form?t: Oddelenie, Fakulta, In?tit?cia, Adresa; Oddelenie2, Fakulta2, In?tit?cia2
Nov? form?t: Author A., Oddelenie, Fakulta, In?tit?cia, Adresa; Author B., ...

## Pr?klad spr?vneho v?stupu
{{"internal_authors":[{{"name":"Nov?k, Jan","faculty":"Faculty of Technology","ou":"Department of Polymer Engineering"}}]}}
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
    resource_id: int,
    allowed_internal_authors: list[str],
    *,
    allowed_workplaces: list[str] | None = None,
    default_attributions: list[dict[str, str]] | None = None,
    repository_authors: list[str] | None = None,
    wos_authors: list[str] | None = None,
    scopus_authors: list[str] | None = None,
    wos_affiliation: str | None = None,
    scopus_affiliation: str | None = None,
    fulltext_affiliation: str | None = None,
    title: str | None = None,
    journal: str | None = None,
    doi: str | None = None,
    source_tags: list[str] | None = None,
    flags: dict[str, Any] | None = None,
) -> str:
    parts: list[str] = [f"=== Z?znam resource_id={resource_id} ==="]

    if title:
        parts.append(f"N?zov publik?cie:\n{title}")
    if journal:
        parts.append(f"?asopis / zdroj:\n{journal}")
    if doi:
        parts.append(f"DOI:\n{doi}")
    if source_tags:
        parts.append("Zdrojov? tagy z?znamu:\n" + json.dumps(source_tags, ensure_ascii=False))
    if repository_authors:
        parts.append(
            "Zjednoten? zoznam autorov v repozit?ri (dc.contributor.author):\n"
            + json.dumps(repository_authors, ensure_ascii=False)
        )
    if wos_authors:
        parts.append("Autori identifikovan? z WoS:\n" + json.dumps(wos_authors, ensure_ascii=False))
    if scopus_authors:
        parts.append("Autori identifikovan? zo Scopus:\n" + json.dumps(scopus_authors, ensure_ascii=False))
    if wos_affiliation:
        parts.append(f"WoS afili?cia (obsahuje men? autorov):\n{wos_affiliation}")
    else:
        parts.append("WoS afili?cia: (nedostupn?)")
    if scopus_affiliation:
        parts.append(f"Scopus afili?cia (m??e obsahova? men? autorov):\n{scopus_affiliation}")
    if fulltext_affiliation:
        parts.append(f"Fulltext afili?cia:\n{fulltext_affiliation}")

    if flags:
        relevant = {
            k: flags[k]
            for k in (
                "utb_authors_unmatched",
                "utb_authors_found_count",
                "multiple_utb_blocks",
                "wos_parse_warnings",
                "error",
                "ambiguous_authors",
                "attributions",
            )
            if k in flags
        }
        if relevant:
            parts.append(
                "Kontext z heurist?k:\n" + json.dumps(relevant, ensure_ascii=False, indent=2)
            )

    parts.append(
        "Povolen? men? intern?ch autorov UTB (pou?i V?HRADNE tieto men?):\n"
        + json.dumps(allowed_internal_authors, ensure_ascii=False)
    )
    if allowed_workplaces:
        parts.append(
            "UTB_WORKPLACES ? povolen? n?zvy pracov?sk / OU:\n"
            + json.dumps(allowed_workplaces, ensure_ascii=False)
        )
    if default_attributions:
        parts.append(
            "Predvolen? afili?cie intern?ch autorov z registra UTB:\n"
            + json.dumps(default_attributions, ensure_ascii=False, indent=2)
        )

    parts.append(
        "Vr?? JSON objekt obsahuj?ci k??? 'internal_authors' "
        "so zoznamom identifikovan?ch intern?ch autorov."
    )

    return "\n\n".join(parts)


def _registry_identity(author: InternalAuthor) -> str:
    if author.limited_author_id is not None:
        return f"id:{author.limited_author_id}"
    return f"name:{_normalize_name(author.canonical_name)}"


def _source_author_allowlist(
    source_authors: list[str] | None,
    registry: list[InternalAuthor],
) -> tuple[list[str], dict[str, InternalAuthor], dict[str, str]]:
    allowed: list[str] = []
    allowed_map: dict[str, InternalAuthor] = {}
    preferred_by_identity: dict[str, str] = {}

    for source_name in source_authors or []:
        clean_name = str(source_name).strip()
        if not clean_name:
            continue
        match = match_author(
            clean_name,
            registry,
            settings.author_match_threshold,
            normalize=False,
            require_surname_match=True,
        )
        if not (match.matched and match.author):
            continue
        identity = _registry_identity(match.author)
        if clean_name not in allowed_map:
            allowed.append(clean_name)
            allowed_map[clean_name] = match.author
        preferred_by_identity.setdefault(identity, clean_name)

    return allowed, allowed_map, preferred_by_identity


def _first_value(value: Any) -> str | None:
    if isinstance(value, (list, tuple)):
        for item in value:
            text_value = str(item).strip()
            if text_value:
                return text_value
        return None
    if value is None:
        return None
    text_value = str(value).strip()
    return text_value or None


def _allowed_workplaces_from_tree(workplace_tree: dict[int, Any]) -> list[str]:
    allowed: list[str] = []
    seen: set[str] = set()
    for node in workplace_tree.values():
        if not node.is_department:
            continue
        faculty_node = walk_to_faculty(node.id, workplace_tree)
        if faculty_node is None:
            continue
        if node.name_en in seen:
            continue
        seen.add(node.name_en)
        allowed.append(node.name_en)
    return sorted(allowed)


def _default_affiliation_for_author(author: InternalAuthor, workplace_tree: dict[int, Any]) -> tuple[str, str]:
    default_faculty = (author.faculty or "").strip()
    default_ou = ""
    if author.organization_id is not None:
        node = workplace_tree.get(int(author.organization_id))
        if node is not None:
            default_ou = node.name_en
            faculty_node = walk_to_faculty(node.id, workplace_tree)
            if faculty_node is not None and not default_faculty:
                default_faculty = faculty_node.name_en
    return default_faculty, default_ou


def _default_attributions_for_prompt(
    allowed_names: list[str],
    allowed_map: dict[str, InternalAuthor],
    registry: list[InternalAuthor],
    workplace_tree: dict[int, Any],
) -> list[dict[str, str]]:
    result: list[dict[str, str]] = []
    for allowed_name in allowed_names:
        author = allowed_map.get(allowed_name)
        if author is None:
            match = match_author(
                allowed_name,
                registry,
                settings.author_match_threshold,
                normalize=True,
                require_surname_match=True,
            )
            if not (match.matched and match.author):
                continue
            author = match.author
        default_faculty, default_ou = _default_affiliation_for_author(author, workplace_tree)
        result.append({
            "name": allowed_name,
            "default_faculty": default_faculty,
            "default_ou": default_ou,
        })
    return result


def _history_author_map(
    engine: Engine,
    schema: str,
    resource_ids: list[int],
) -> dict[int, list[dict[str, Any]]]:
    if not resource_ids:
        return {}
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(f"""
                SELECT
                    dedup_kept_resource_id,
                    "utb.source" AS source_arr,
                    "dc.contributor.author" AS authors_arr
                FROM "{schema}"."dedup_histoire"
                WHERE dedup_kept_resource_id = ANY(:ids)
            """), {"ids": resource_ids}).fetchall()
    except Exception:
        rows = []

    history_map: dict[int, list[dict[str, Any]]] = {rid: [] for rid in resource_ids}
    for row in rows:
        kept_id = int(row.dedup_kept_resource_id)
        history_map.setdefault(kept_id, []).append({
            "sources": row.source_arr,
            "authors": row.authors_arr,
        })
    return history_map


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

def _filter_by_registry(
    llm_result: LLMResult,
    registry: list[InternalAuthor],
    allowed_names: list[str],
    allowed_map: dict[str, InternalAuthor],
    preferred_by_identity: dict[str, str],
    allowed_workplaces: set[str] | None = None,
) -> LLMResult:
    allowed_name_set = set(allowed_names)
    normalized_entries: list[LLMAuthorEntry] = []
    seen_identities: set[str] = set()

    for entry in llm_result.internal_authors:
        chosen_name = entry.name.strip()
        matched_author = allowed_map.get(chosen_name)

        if matched_author is None:
            candidate_match = match_author(
                chosen_name,
                registry,
                settings.author_match_threshold,
                normalize=True,
                require_surname_match=True,
            )
            if not (candidate_match.matched and candidate_match.author):
                continue
            identity = _registry_identity(candidate_match.author)
            preferred_name = preferred_by_identity.get(identity)
            if not preferred_name:
                continue
            chosen_name = preferred_name
            matched_author = candidate_match.author

        identity = _registry_identity(matched_author)
        preferred_name = preferred_by_identity.get(identity, chosen_name)
        if preferred_name not in allowed_name_set or identity in seen_identities:
            continue
        seen_identities.add(identity)
        normalized_entries.append(
            LLMAuthorEntry.model_validate(
                {
                    "name": preferred_name,
                    "faculty": entry.faculty,
                    "ou": entry.ou,
                },
                context={"allowed_workplaces": allowed_workplaces or set()},
            )
        )

    return LLMResult(internal_authors=normalized_entries)


# -----------------------------------------------------------------------
# Spracovanie jedného záznamu
# -----------------------------------------------------------------------

def process_llm_record(
    resource_id: int,
    repo_authors: list[str] | None,
    wos_authors: list[str] | None,
    scopus_authors: list[str] | None,
    wos_aff:     list[str] | None,
    scopus_aff:  list[str] | None,
    fulltext_aff: list[str] | None,
    title: list[str] | None,
    journal: list[str] | None,
    doi: list[str] | None,
    source_arr: list[str] | None,
    flags:       dict      | None,
    session:     LLMSession,
    registry:    list[InternalAuthor],
    workplace_tree: dict[int, Any],
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
    candidate_names, allowed_map, preferred_by_identity = _source_author_allowlist(repo_authors, registry)
    if not candidate_names:
        candidate_names = _select_candidates(registry, unmatched, max_candidates=80)
        allowed_map = {}
        preferred_by_identity = {}
    allowed_workplaces = _allowed_workplaces_from_tree(workplace_tree)
    default_attributions = _default_attributions_for_prompt(
        candidate_names,
        allowed_map,
        registry,
        workplace_tree,
    )

    wos_text    = "\n---\n".join(str(i) for i in (wos_aff or []) if i) or None
    scopus_text = "; ".join(str(i) for i in (scopus_aff or []) if i) or None
    fulltext_text = "\n---\n".join(str(i) for i in (fulltext_aff or []) if i) or None

    user_msg = build_user_message(
        resource_id=resource_id,
        allowed_internal_authors=candidate_names,
        allowed_workplaces=allowed_workplaces,
        default_attributions=default_attributions,
        repository_authors=[str(name).strip() for name in (repo_authors or []) if str(name).strip()],
        wos_authors=[str(name).strip() for name in (wos_authors or []) if str(name).strip()],
        scopus_authors=[str(name).strip() for name in (scopus_authors or []) if str(name).strip()],
        wos_affiliation=wos_text,
        scopus_affiliation=scopus_text,
        fulltext_affiliation=fulltext_text,
        title=_first_value(title),
        journal=_first_value(journal),
        doi=_first_value(doi),
        source_tags=[str(value).strip() for value in (source_arr or []) if str(value).strip()],
        flags=flags or {},
    )

    raw_output = ""
    # 1 retry pre prechodné chyby (nevalidný JSON, sieťová chyba).
    # ValidationError sa neretriuje – ide o štrukturálny problém odpovede.
    for attempt in range(2):
        try:
            raw_output  = session.ask(user_msg)
            parsed_dict = parse_llm_json_output(raw_output)
            llm_result  = LLMResult.model_validate(
                parsed_dict,
                context={"allowed_workplaces": set(allowed_workplaces)},
            )
            llm_result  = _filter_by_registry(
                llm_result,
                registry,
                candidate_names,
                allowed_map,
                preferred_by_identity,
                set(allowed_workplaces),
            )

            authors = [e.name for e in llm_result.internal_authors]
            faculties = [e.faculty for e in llm_result.internal_authors]
            ous = [e.ou for e in llm_result.internal_authors]

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
    reprocess:  bool          = False,
) -> None:
    engine     = engine     or get_local_engine()
    batch_size = batch_size or settings.llm_batch_size
    schema     = settings.local_schema
    table      = settings.local_table
    queue      = QUEUE_TABLE

    llm_client = get_llm_client(provider)
    session    = create_authors_session(llm_client)
    registry   = get_author_registry()
    workplace_tree = load_workplace_tree()

    statuses = [LLMStatus.NOT_PROCESSED, LLMStatus.ERROR]
    if reprocess:
        statuses.append(LLMStatus.PROCESSED)
        statuses.append(LLMStatus.VALIDATION_ERROR)

    # Nazbieraj všetky ID vopred – aby sa zmenený status po spracovaní
    # neprekrýval s filtrom a nezapríčinil nekonečnú slučku.
    with engine.connect() as conn:
        orphan_count = conn.execute(
            text(
                f'SELECT COUNT(*) FROM "{schema}"."{queue}" q '
                f'LEFT JOIN "{schema}"."{table}" m ON q.resource_id = m.resource_id '
                "WHERE q.author_needs_llm = TRUE AND q.author_llm_status = ANY(:s) "
                "AND m.resource_id IS NULL"
            ),
            {"s": statuses},
        ).scalar_one()
        id_rows = conn.execute(
            text(
                f'SELECT q.resource_id FROM "{schema}"."{queue}" q '
                f'JOIN "{schema}"."{table}" m ON q.resource_id = m.resource_id'
                " WHERE q.author_needs_llm = TRUE AND q.author_llm_status = ANY(:s)"
                " ORDER BY q.resource_id"
            ),
            {"s": statuses},
        ).fetchall()

    all_ids: list[int] = [r[0] for r in id_rows]
    if limit > 0:
        all_ids = all_ids[:limit]

    total = len(all_ids)
    if total == 0:
        print("[INFO] Žiadne záznamy na LLM spracovanie autorov.")
        return

    if orphan_count:
        print(
            "[WARN] Preskakujem siroty v utb_processing_queue bez odpovedajúceho "
            f"záznamu v utb_metadata_arr: {orphan_count}"
        )
    print(f"[INFO] LLM autorov – záznamov na spracovanie: {total}")
    processed = 0
    errors    = 0
    started   = time.time()

    while processed < total:
        batch_ids = all_ids[processed: processed + batch_size]
        with engine.connect() as conn:
            rows = conn.execute(
                text(f"""
                    SELECT q.resource_id,
                           m."dc.contributor.author"  AS repo_authors,
                           m."utb.wos.affiliation"    AS wos_aff,
                           m."utb.scopus.affiliation" AS scopus_aff,
                           m."utb.fulltext.affiliation" AS fulltext_aff,
                           m."dc.title"               AS title_arr,
                           m."dc.relation.ispartof"   AS journal_arr,
                           m."dc.identifier.doi"      AS doi_arr,
                           m."utb.source"             AS source_arr,
                           q.author_flags
                    FROM "{schema}"."{queue}" q
                    JOIN "{schema}"."{table}" m ON q.resource_id = m.resource_id
                    WHERE q.resource_id = ANY(:ids)
                    ORDER BY q.resource_id
                """),
                {"ids": batch_ids},
            ).fetchall()

        if not rows:
            processed += len(batch_ids)
            print(f"  [WARN] Dávka bez platných záznamov: {batch_ids}")
            continue

        history_map = _history_author_map(engine, schema, [int(row.resource_id) for row in rows])
        updates = []
        for row in rows:
            source_split = split_source_author_lists(
                current_authors=row.repo_authors,
                current_sources=row.source_arr,
                history_rows=history_map.get(int(row.resource_id), []),
            )
            u = process_llm_record(
                resource_id=row.resource_id,
                repo_authors=row.repo_authors,
                wos_authors=source_split.get("wos"),
                scopus_authors=source_split.get("scopus"),
                wos_aff=row.wos_aff,
                scopus_aff=row.scopus_aff,
                fulltext_aff=row.fulltext_aff,
                title=row.title_arr,
                journal=row.journal_arr,
                doi=row.doi_arr,
                source_arr=row.source_arr,
                flags=row.author_flags or {},
                session=session,
                registry=registry,
                workplace_tree=workplace_tree,
            )
            updates.append(u)
            status = u["author_llm_status"]
            if status == LLMStatus.PROCESSED:
                authors = (u.get("author_llm_result") or {}).get("internal_authors", [])
                names   = [a.get("name", "") for a in authors]
                print(f"  [ID {row.resource_id}] OK  autori: {names}")
            else:
                err = (u.get("author_llm_result") or {}).get("error", "")
                raw = (u.get("author_llm_result") or {}).get("raw", "")
                print(f"  [ID {row.resource_id}] {status}  chyba: {err}")
                if raw:
                    print(f"    raw: {raw[:300]}")
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
