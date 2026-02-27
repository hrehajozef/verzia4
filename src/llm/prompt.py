"""LLM prompt definície, Pydantic schémy a JSON Schema pre structured output."""

from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from src.common.constants import DEPARTMENTS, FACULTIES


# -----------------------------------------------------------------------
# Pydantic modely – prísna validácia výstupu LLM
# -----------------------------------------------------------------------

# Zoznamy povolených hodnôt pre validáciu
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
        description=(
            "Plný anglický názov oddelenia/ústavu UTB. "
            "Ak nie je známy, použi prázdny reťazec."
        ),
    )

    @field_validator("faculty")
    @classmethod
    def validate_faculty(cls, v: str) -> str:
        if v and v not in _VALID_FACULTY_NAMES:
            # Mäkká validácia: ak LLM vrátilo neplatný názov, vyprázdni
            return ""
        return v

    @field_validator("ou")
    @classmethod
    def validate_ou(cls, v: str) -> str:
        # Akceptuj aj čiastočné zhody (LLM môže skrátiť názov)
        return v


class LLMResult(BaseModel):
    """Prísna výstupná štruktúra LLM odpovede."""

    model_config = ConfigDict(extra="forbid")

    internal_authors: list[LLMAuthorEntry] = Field(
        default_factory=list,
        description="Zoznam interných UTB autorov identifikovaných z afiliácie.",
    )


# -----------------------------------------------------------------------
# JSON Schema pre structured output / function calling
# -----------------------------------------------------------------------

# Plný JSON Schema objekt pre response_format alebo function definition
LLM_OUTPUT_JSON_SCHEMA: dict[str, Any] = {
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

# Definícia funkcie pre function calling (OpenAI format)
LLM_FUNCTION_DEF: dict[str, Any] = {
    "name":        "extract_utb_authors",
    "description": (
        "Extrahuje interných UTB autorov z textu afiliácie publikácie. "
        "Vráti iba autorov, ktorých meno je v poskytnutom whitelist zozname."
    ),
    "parameters":  LLM_OUTPUT_JSON_SCHEMA,
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

## Príklady oddelení (ukážka)
{_DEPT_SAMPLE}

## WoS formát vstupu
[Priezvisko1, Meno1; Priezvisko2, Meno2] Inštitúcia, Oddelenie, Adresa;
[Priezvisko3, Meno3] Iná inštitúcia, Adresa

## Scopus formát vstupu (bez mien autorov)
Oddelenie, Fakulta, Inštitúcia, Adresa; Oddelenie2, Fakulta2, Inštitúcia2

## Príklad správneho výstupu
{{"internal_authors":[{{"name":"Novák, Jan","faculty":"Faculty of Technology","ou":"Department of Polymer Engineering"}}]}}
"""


# -----------------------------------------------------------------------
# Zostavenie user promptu
# -----------------------------------------------------------------------

def build_user_message(
    resource_id:             int,
    wos_affiliation:         str | None,
    scopus_affiliation:      str | None,
    flags:                   dict[str, Any] | None,
    allowed_internal_authors: list[str],
) -> str:
    """
    Zostaví user message pre LLM s kontextom záznamu.

    Obsahuje:
      – WoS afiliáciu (s menami autorov)
      – Scopus afiliáciu (bez mien, len inštitúcie)
      – Relevantné flagy z heuristík
      – Whitelist povolených mien interných autorov
    """
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

    # Whitelist – kľúčová informácia pre LLM
    parts.append(
        "Povolené mená interných autorov UTB (použi VÝHRADNE tieto mená):\n"
        + json.dumps(allowed_internal_authors, ensure_ascii=False)
    )

    parts.append(
        "Vráť JSON objekt obsahujúci kľúč 'internal_authors' "
        "so zoznamom identifikovaných interných autorov."
    )

    return "\n\n".join(parts)