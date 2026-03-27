"""Prompty, Pydantic schémy, JSON Schema a LLM runner pre extrakciu dátumov publikácií."""

from __future__ import annotations

import json
import re
import time
from datetime import date, datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.config.settings import settings
from src.db.engines import get_local_engine
from src.llm.session import LLMSession, create_dates_session
from src.llm.client import get_llm_client, parse_llm_json_output

DATE_LLM_VERSION = "1.0.0"


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
                d = date.fromisoformat(v)
                # Rok musí byť v rozumnom rozsahu pre vedecké publikácie UTB
                if 1990 <= d.year <= 2035:
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
  1. received         – doručený do redakcie (Received, Submitted, Došlo)
  2. reviewed         – po recenzii (Received in revised form, Editorial decision)
  3. accepted         – prijatý na publikáciu (Accepted, Approved for publication)
  4. published_online – zverejnený online (Published online, Available online)
  5. published        – publikovaný tlačene (Published, Date of publication)

## Pravidlá – MUSÍŠ ich dodržať
1. Výstup je VÝHRADNE JSON objekt so 5 kľúčmi: received, reviewed, accepted, published_online, published.
2. Každá hodnota je ISO dátum vo formáte YYYY-MM-DD alebo prázdny reťazec "" ak dátum nie je dostupný.
3. Ak je dostupný iba mesiac a rok, použi prvý deň mesiaca (napr. "2019-03-01").
4. Ak je dostupný iba rok, použi "YYYY-01-01".
5. Extrahuj dátumy PRESNE tak ako sú uvedené v texte. Ak text obsahuje dátumy
   v nesprávnom chronologickom poradí (received > accepted), extrahuj ich aj tak – neupravuj ich.
6. Rok musí byť v rozsahu 1990–2035. Ak rok nespadá do rozsahu, použi "".
7. Žiadne komentáre, markdown, vysvetlenia – iba JSON.

## Formát bodkových dátumov (DD.MM.YYYY vs MM.DD.YYYY)
Ak vstupný text obsahuje dátumy vo formáte A.B.RRRR:
  • Ak A > 12: ide o DD.MM.RRRR (deň nemôže byť mesiac)
  • Ak B > 12: ide o MM.DD.RRRR (mesiac nemôže byť > 12)
  • Ak oba ≤ 12: urči formát podľa chronologického kontextu (received ≤ accepted ≤ published)
  • Uprednostni európsky formát DD.MM.RRRR ak kontext nepomôže

## Príklad vstupu
Received: 15 March 2018; Accepted for publication: 20 June 2018; Published online: 5 July 2018

## Príklad správneho výstupu
{"received": "2018-03-15", "reviewed": "", "accepted": "2018-06-20", "published_online": "2018-07-05", "published": ""}
"""

# Preamble pre Ollama konverzačný režim (KV-cache optimalizácia – načíta sa raz).
# Príklad musí byť realistický (nie prázdny), aby model videl očakávaný formát odpovede.
DATES_SETUP_PREAMBLE: list[dict] = [
    {
        "role":    "user",
        "content": (
            "=== Záznam resource_id=0 (ukážka) ===\n\n"
            "Surový text dátumov:\n"
            "Received: 15 March 2018; Accepted for publication: 20 June 2018; "
            "Published online: 5 July 2018\n\n"
            'Vráť JSON objekt s kľúčmi: "received", "reviewed", "accepted", '
            '"published_online", "published". Každá hodnota je YYYY-MM-DD alebo "".'
        ),
    },
    {
        "role":    "assistant",
        "content": '{"received": "2018-03-15", "reviewed": "", "accepted": "2018-06-20", "published_online": "2018-07-05", "published": ""}',
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
    flags = date_flags or {}
    parts: list[str] = [f"=== Záznam resource_id={resource_id} ==="]

    parts.append(f"Surový text dátumov:\n{raw_date_text}")

    if dc_issued:
        parts.append(f"dc.date.issued (rok vydania z katalógu): {dc_issued}")

    # Ostatné problémy heuristiky (bez MDR – tie riešime osobitne nižšie)
    _HEURISTIC_KEYS = (
        "unknown_labels", "unparseable_dates", "placeholder_dates",
        "no_labels_found", "chrono_warnings", "year_only_dates",
    )
    relevant = {k: flags[k] for k in _HEURISTIC_KEYS if k in flags}
    if relevant:
        parts.append(
            "Problémy heuristiky:\n" + json.dumps(relevant, ensure_ascii=False, indent=2)
        )

    # MDR (Month-Day / Day-Month) kontext – explicitné pokyny pre LLM
    if "mdr_ambiguous" in flags:
        mdr = flags["mdr_ambiguous"]
        dmy = mdr.get("dmy_interpretation", "")
        mdy = mdr.get("mdy_interpretation", "")
        note = (
            "DÔLEŽITÉ – Formát bodkového dátumu je nejednoznačný (DD.MM.RRRR vs MM.DD.RRRR).\n"
            f"  Interpretácia DD.MM.RRRR: {dmy}\n"
            f"  Interpretácia MM.DD.RRRR: {mdy}\n"
            "Urči správny formát z kontextu (chronologické poradie, rok vydania). "
            "Ak ani kontext nepomôže, použi DD.MM.RRRR (európsky formát)."
        )
        parts.append(note)

    elif "mdr_format_resolved" in flags:
        mdr = flags["mdr_format_resolved"]
        conf = mdr.get("confidence", "")
        fmt  = mdr.get("format", "")
        if conf == "medium":
            # Heuristika použila chronológiu, ale nie je si 100% istá
            parts.append(
                f"POZNÁMKA – Formát bodkového dátumu určený heuristikou: {fmt} "
                f"(stredná istota – overené chronologickým poradím). "
                "Ak vidíš iný formát z kontextu, použi ho."
            )

    elif "mdr_format_conflict" in flags:
        mdr = flags["mdr_format_conflict"]
        parts.append(
            "UPOZORNENIE – Konflikt formátov dátumov: niektoré dátumy sú jednoznačne DD.MM.RRRR "
            "a iné MM.DD.RRRR. Pravdepodobná chyba v zdrojových dátach. "
            "Skús každý dátum posúdiť zvlášť podľa kontextu.\n"
            "Detail: " + json.dumps(mdr, ensure_ascii=False)
        )

    elif "mdr_chrono_error" in flags:
        parts.append(
            "UPOZORNENIE – Ani DD.MM.RRRR ani MM.DD.RRRR interpretácia nedáva chronologicky "
            "konzistentné dátumy. Extrahuj dátumy presne z textu tak ako sú – "
            "nevymýšľaj ani neopravuj."
        )

    parts.append(
        'Vráť JSON objekt s kľúčmi: "received", "reviewed", "accepted", '
        '"published_online", "published". Každá hodnota je YYYY-MM-DD alebo "".'
    )

    return "\n\n".join(parts)


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

    user_msg = build_date_user_message(
        resource_id   = resource_id,
        raw_date_text = raw_date_text,
        dc_issued     = dc_issued,
        date_flags    = date_flags or {},
    )

    raw_output = ""
    # 1 retry pre prechodné chyby (nevalidný JSON, sieťová chyba).
    # ValidationError sa neretriuje – ide o štrukturálny problém odpovede.
    for attempt in range(2):
        try:
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
            break  # úspech

        except ValidationError as exc:
            result["date_llm_status"] = "validation_error"
            result["date_llm_result"] = {"error": str(exc), "raw": raw_output[:2000]}
            break  # štrukturálna chyba – retry nepomôže

        except Exception as exc:
            if attempt == 0:
                time.sleep(2)
                continue   # jeden retry
            result["date_llm_status"] = "error"
            result["date_llm_result"] = {"error": f"{type(exc).__name__}: {exc}", "raw": raw_output[:1000]}

    return result


# -----------------------------------------------------------------------
# Dávkové spracovanie
# -----------------------------------------------------------------------

def run_date_llm(
    engine:        Engine | None = None,
    batch_size:    int | None    = None,
    limit:         int           = 0,
    provider:      str | None    = None,
    reprocess:     bool          = False,
    include_dash:  bool          = False,
) -> None:
    """
    Spustí LLM parsovanie dátumov pre záznamy s date_needs_llm=TRUE.

    Args:
        engine:       SQLAlchemy engine (použije lokálny ak None).
        batch_size:   Veľkosť dávky.
        limit:        Max počet záznamov (0 = všetky).
        provider:     LLM provider (ollama / openai).
        reprocess:    Ak True, spracuje aj záznamy s chybou.
        include_dash: Ak True, spracuje aj záznamy kde utb.fulltext.dates = '{-}'.
                      Štandardne (False) sa tieto záznamy preskakujú.
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

    dash_filter = "" if include_dash else "AND (\"utb.fulltext.dates\"[1] IS NULL OR \"utb.fulltext.dates\"[1] != '{-}')"

    with engine.connect() as conn:
        total = conn.execute(
            text(f"""
                SELECT COUNT(*) FROM "{schema}"."{table}"
                WHERE date_needs_llm = TRUE
                  AND date_llm_status = ANY(:s)
                  {dash_filter}
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
                      {dash_filter}
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
