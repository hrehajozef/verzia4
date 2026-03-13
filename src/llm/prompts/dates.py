"""Prompty, Pydantic schémy a JSON Schema pre extrakciu dátumov publikácií."""

from __future__ import annotations

import re
from datetime import date
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator


# -----------------------------------------------------------------------
# Pydantic model
# -----------------------------------------------------------------------

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class DateLLMResult(BaseModel):
    """Výstupná štruktúra LLM odpovede pre dátumy.

    Každé pole je buď ISO dátum (YYYY-MM-DD) alebo prázdny reťazec.
    """

    model_config = ConfigDict(extra="forbid")

    received:         str = Field(default="", description="Dátum doručenia rukopisu (YYYY-MM-DD alebo '').")
    reviewed:         str = Field(default="", description="Dátum recenzie/revised form (YYYY-MM-DD alebo '').")
    accepted:         str = Field(default="", description="Dátum prijatia (YYYY-MM-DD alebo '').")
    published_online: str = Field(default="", description="Dátum online publikácie (YYYY-MM-DD alebo '').")
    published:        str = Field(default="", description="Dátum tlačenej publikácie (YYYY-MM-DD alebo '').")

    @field_validator("received", "reviewed", "accepted", "published_online", "published")
    @classmethod
    def validate_date_field(cls, v: str) -> str:
        if not v:
            return ""
        if _DATE_RE.match(v):
            try:
                date.fromisoformat(v)
                return v
            except ValueError:
                pass
        return ""

    def to_date(self, field_name: str) -> date | None:
        """Konvertuje string pole na date objekt alebo None."""
        value = getattr(self, field_name, "")
        if not value:
            return None
        try:
            return date.fromisoformat(value)
        except ValueError:
            return None


# -----------------------------------------------------------------------
# JSON Schema pre structured output
# -----------------------------------------------------------------------

DATES_JSON_SCHEMA: dict[str, Any] = {
    "type": "object",
    "required": ["received", "reviewed", "accepted", "published_online", "published"],
    "additionalProperties": False,
    "properties": {
        "received": {
            "type": "string",
            "description": "Dátum doručenia rukopisu vo formáte YYYY-MM-DD alebo prázdny reťazec.",
        },
        "reviewed": {
            "type": "string",
            "description": "Dátum recenzie vo formáte YYYY-MM-DD alebo prázdny reťazec.",
        },
        "accepted": {
            "type": "string",
            "description": "Dátum prijatia vo formáte YYYY-MM-DD alebo prázdny reťazec.",
        },
        "published_online": {
            "type": "string",
            "description": "Dátum online publikácie vo formáte YYYY-MM-DD alebo prázdny reťazec.",
        },
        "published": {
            "type": "string",
            "description": "Dátum tlačenej publikácie vo formáte YYYY-MM-DD alebo prázdny reťazec.",
        },
    },
}


# -----------------------------------------------------------------------
# System prompt
# -----------------------------------------------------------------------

DATES_SYSTEM_PROMPT = """Si expert na extrakciu dátumov z metadát vedeckých publikácií.

## Tvoja úloha:
Zo surového textu metadát urči kedy bol článok:
  1. received    – doručený do redakcie (Received, Submitted, Došlo)
  2. reviewed    – po recenzii (Received in revised form, Editorial decision)
  3. accepted    – prijatý na publikáciu (Accepted, Approved for publication)
  4. published_online – zverejnený online (Published online, Available online)
  5. published   – publikovaný tlačene (Published, Date of publication)

## Pravidlá – MUSÍŠ ich dodržať
1. Výstup je VÝHRADNE JSON objekt so 5 kľúčmi: received, reviewed, accepted, published_online, published.
2. Každá hodnota je ISO dátum vo formáte YYYY-MM-DD alebo prázdny reťazec "" ak dátum nie je dostupný.
3. Ak je dostupný iba mesiac a rok, použij prvý deň mesiaca (napr. "2019-03-01").
4. Ak je dostupný iba rok, použij "YYYY-01-01".
5. Zachovaj chronologické poradie: received ≤ reviewed ≤ accepted ≤ published_online ≤ published.
6. Žiadne komentáre, markdown, vysvetlenia – iba JSON.

## Príklad vstupu
Received: 15 March 2018; Accepted for publication: 20 June 2018; Published online: 5 July 2018

## Príklad správneho výstupu
{"received": "2018-03-15", "reviewed": "", "accepted": "2018-06-20", "published_online": "2018-07-05", "published": ""}
"""

# Preamble pre Ollama konverzačný režim
DATES_SETUP_PREAMBLE: list[dict] = [
    {
        "role":    "user",
        "content": (
            "Rozumieš svojej úlohe? Budem ti posielať texty s dátumami publikácií jeden po jednom. "
            "Pre každý vrátiš JSON s 5 dátumovými poľami."
        ),
    },
    {
        "role":    "assistant",
        "content": '{"received": "", "reviewed": "", "accepted": "", "published_online": "", "published": ""}',
    },
]


# -----------------------------------------------------------------------
# Zostavenie user promptu
# -----------------------------------------------------------------------

def build_date_user_message(
    resource_id:    int,
    raw_date_text:  str,
    dc_issued:      str | None,
    date_flags:     dict[str, Any] | None,
) -> str:
    """Zostaví user message pre LLM parsovanie dátumov."""
    parts: list[str] = [f"=== Záznam resource_id={resource_id} ==="]

    parts.append(f"Surový text dátumov:\n{raw_date_text}")

    if dc_issued:
        parts.append(f"dc.date.issued (rok vydania z katalógu): {dc_issued}")

    if date_flags:
        relevant = {
            k: date_flags[k]
            for k in ("unknown_labels", "unparseable_dates", "placeholder_dates",
                       "no_labels_found", "chrono_warnings", "year_only_dates")
            if k in date_flags
        }
        if relevant:
            import json
            parts.append(
                "Problémy heuristiky:\n" + json.dumps(relevant, ensure_ascii=False, indent=2)
            )

    parts.append(
        'Vráť JSON objekt s kľúčmi: "received", "reviewed", "accepted", '
        '"published_online", "published". Každá hodnota je YYYY-MM-DD alebo "".'
    )

    return "\n\n".join(parts)
