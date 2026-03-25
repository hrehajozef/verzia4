"""LLM Session management a factory funkcie.

LLMSession       – obal okolo klienta s fixným kontextom:
                   • system_prompt + json_schema nastavené raz
                   • Pre Ollama: voliteľný preamble pre KV-cache
                   • ask(user_message) → string odpoveď

Factory funkcie:
  create_authors_session – session pre extrakciu UTB autorov
  create_dates_session   – session pre parsovanie dátumov
"""

from __future__ import annotations

from typing import Any

from src.llm.client import LLMClient, OllamaClient


# ═══════════════════════════════════════════════════════════════════════
# LLM Session
# ═══════════════════════════════════════════════════════════════════════

class LLMSession:
    """
    Relácia LLM s fixným kontextom pre dávkové spracovanie záznamov.

    Inicializácia prebieha raz:
      – system_prompt  s popisom úlohy a formátu výstupu
      – json_schema    pre structured output (Ollama aj Cloud)
      – preamble       (voliteľné) pre Ollama: ukážkový dialog potvrdzujúci
                       pochopenie úlohy (KV-cache optimalizácia)

    Pre každý záznam sa volá ask(user_message):
      – Ollama:  [system, preamble..., user_record]   → čistý kontext, bez histórie
      – Cloud:   [system, user_record]                → čistý kontext, každý request

    Záznamy si navzájom NEOVPLYVŇUJÚ výstup (história sa neakumuluje).
    """

    def __init__(
        self,
        client:        LLMClient,
        system_prompt: str,
        json_schema:   dict[str, Any],
        preamble:      list[dict] | None = None,
    ):
        self._client        = client
        self._system_prompt = system_prompt
        self._json_schema   = json_schema
        # Preamble sa použije iba pre Ollamu (lokálny model)
        self._preamble = preamble if isinstance(client, OllamaClient) else None

    def ask(self, user_message: str) -> str:
        """Vykoná jedno volanie v rámci session a vráti surový string."""
        return self._client.complete(
            self._system_prompt,
            user_message,
            json_schema = self._json_schema,
            preamble    = self._preamble,
        )


# ═══════════════════════════════════════════════════════════════════════
# Factory funkcie
# ═══════════════════════════════════════════════════════════════════════

def create_authors_session(client: LLMClient) -> LLMSession:
    """Vytvorí LLM session pre extrakciu UTB autorov z afiliácií."""
    from src.llm.tasks.authors import (
        SYSTEM_PROMPT,
        AUTHORS_JSON_SCHEMA,
        AUTHORS_SETUP_PREAMBLE,
    )
    return LLMSession(
        client        = client,
        system_prompt = SYSTEM_PROMPT,
        json_schema   = AUTHORS_JSON_SCHEMA,
        preamble      = AUTHORS_SETUP_PREAMBLE,
    )


def create_dates_session(client: LLMClient) -> LLMSession:
    """Vytvorí LLM session pre parsovanie dátumov publikácií."""
    from src.llm.tasks.dates import (
        DATES_SYSTEM_PROMPT,
        DATES_JSON_SCHEMA,
        DATES_SETUP_PREAMBLE,
    )
    return LLMSession(
        client        = client,
        system_prompt = DATES_SYSTEM_PROMPT,
        json_schema   = DATES_JSON_SCHEMA,
        preamble      = DATES_SETUP_PREAMBLE,
    )
