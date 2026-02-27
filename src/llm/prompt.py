"""Definícia LLM schémy a promptov pre prísne JSON odpovede."""

from __future__ import annotations

import json
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class LLMAuthorEntry(BaseModel):
    """Jeden interný autor identifikovaný z afiliácie."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(..., description="Celé meno s diakritikou vo formáte Priezvisko, Meno")
    faculty: str = Field(default="", description="Plný názov fakulty alebo prázdny reťazec")
    ou: str = Field(default="", description="Názov oddelenia alebo ústavu, inak prázdny reťazec")


class LLMResult(BaseModel):
    """Prísna výstupná štruktúra, ktorú smie LLM vrátiť."""

    model_config = ConfigDict(extra="forbid")

    internal_authors: list[LLMAuthorEntry] = Field(default_factory=list)


SYSTEM_PROMPT = """Si asistent na analýzu afiliácií publikácií.

Povinné pravidlá:
1. Výstup musí byť výhradne validný JSON objekt.
2. JSON objekt smie obsahovať iba kľúč "internal_authors".
3. "internal_authors" je pole objektov s kľúčmi "name", "faculty", "ou".
4. Kľúč "name" musí byť celé meno s diakritikou vo formáte "Priezvisko, Meno".
5. Kľúč "name" môže obsahovať iba mená zo zoznamu interných autorov, ktorý dostaneš vo vstupe.
6. Ak meno nie je v zozname interných autorov, nesmie byť vo výstupe.
7. Ak nevieš určiť fakultu alebo oddelenie, použi prázdny reťazec.
8. Nevypisuj komentáre, vysvetlenie ani markdown.

Príklad jediného povoleného formátu:
{"internal_authors":[{"name":"Priezvisko, Meno","faculty":"Faculty of Technology","ou":"Dept Polymer Engn"}]}
"""


def build_user_message(
    resource_id: int,
    wos_affiliation: str | None,
    scopus_affiliation: str | None,
    flags: dict[str, Any] | None,
    allowed_internal_authors: list[str],
) -> str:
    """Poskladá user prompt s afiliáciou a whitelistom interných autorov."""

    parts = [f"resource_id: {resource_id}"]

    if wos_affiliation:
        parts.append(f"WoS afiliácia:\n{wos_affiliation}")
    else:
        parts.append("WoS afiliácia: (nie je k dispozícii)")

    if scopus_affiliation:
        parts.append(f"Scopus afiliácia:\n{scopus_affiliation}")

    if flags:
        keys = [
            "utb_authors_found_count",
            "utb_authors_unmatched",
            "multiple_utb_blocks",
            "wos_parse_warnings",
            "error",
        ]
        filtered = {key: flags[key] for key in keys if key in flags}
        if filtered:
            parts.append(f"Kontext z heuristík:\n{json.dumps(filtered, ensure_ascii=False)}")

    parts.append(
        "Povolené mená interných autorov (použi iba mená z tohto zoznamu):\n"
        + json.dumps(allowed_internal_authors, ensure_ascii=False)
    )

    return "\n\n".join(parts)
