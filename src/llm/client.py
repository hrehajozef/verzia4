"""LLM klienti a session management.

Architektúra:
  LLMClient        – abstraktný základ (complete metóda)
  OllamaClient     – lokálny model cez Ollama API
                     • Structured output cez JSON Schema (format parameter)
                     • Podporuje konverzačný preamble (KV-cache optimalizácia)
  CloudLLMCompatibleClient – OpenAI-kompatibilné endpointy
                     • Structured output → function calling → json_object fallback
                     • Každý request je čistý kontext (bez preamble)

  LLMSession       – obal okolo klienta s fixným kontextom:
                     • system_prompt + json_schema nastavené raz
                     • Pre Ollama: voliteľný preamble pre KV-cache
                     • ask(user_message) → string odpoveď
"""

from __future__ import annotations

import json
import re
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any

import httpx

from src.config.settings import settings


# ═══════════════════════════════════════════════════════════════════════
# Abstraktný základ
# ═══════════════════════════════════════════════════════════════════════

class LLMClient(ABC):

    @abstractmethod
    def complete(
        self,
        system_prompt: str,
        user_message:  str,
        *,
        json_schema: dict[str, Any] | None = None,
        preamble:    list[dict]     | None = None,
    ) -> str:
        """
        Vykoná jedno volanie LLM a vráti surový string.

        Args:
            system_prompt: Systémová inštrukcia pre model.
            user_message:  Vstupná správa od používateľa.
            json_schema:   JSON Schema pre structured output (Ollama/OpenAI).
                           Ak None, použije sa základný json mode.
            preamble:      Zoznam doplňujúcich správ pred user_message
                           vo formáte [{"role": ..., "content": ...}, ...].
                           Len pre Ollama – u Cloud klientov ignorované.
        """
        raise NotImplementedError

    def health_check(self) -> bool:
        try:
            return bool(self.complete("Odpovedz iba ok", "test"))
        except Exception:
            return False


# ═══════════════════════════════════════════════════════════════════════
# Ollama klient (lokálny model)
# ═══════════════════════════════════════════════════════════════════════

class OllamaClient(LLMClient):
    """
    Klient pre lokálnu Ollamu.

    Structured output:
      • Ak json_schema je poskytnutý, použije sa ako format parameter
        (Ollama structured outputs – zaručuje validný JSON podľa schémy).
      • Ak json_schema je None, použije sa format='json' (voľný JSON).

    Konverzačný preamble:
      • Preamble správy sa vložia medzi system a user message.
      • Ollama KV-cache znovupoužije spoločný prefix → rýchlejšie spracovanie.
      • Záznamy si navzájom NEOVPLYVŇUJÚ výstup (história sa neakumuluje).
    """

    def __init__(
        self,
        base_url: str | None = None,
        model:    str | None = None,
        timeout:  int | None = None,
    ):
        self.base_url = (base_url or settings.local_llm_base_url).rstrip("/")
        self.model    = model   or settings.local_llm_model
        self.timeout  = timeout or settings.llm_timeout

    def complete(
        self,
        system_prompt: str,
        user_message:  str,
        *,
        json_schema: dict[str, Any] | None = None,
        preamble:    list[dict]     | None = None,
    ) -> str:
        messages: list[dict] = [{"role": "system", "content": system_prompt}]

        if preamble:
            messages.extend(preamble)

        messages.append({"role": "user", "content": user_message})

        payload: dict[str, Any] = {
            "model":    self.model,
            "messages": messages,
            "stream":   False,
            "format":   json_schema if json_schema is not None else "json",
        }

        with httpx.Client(timeout=self.timeout) as client:
            response = client.post(f"{self.base_url}/api/chat", json=payload)
            response.raise_for_status()
            return response.json()["message"]["content"]


# ═══════════════════════════════════════════════════════════════════════
# Cloud / OpenAI-kompatibilný klient
# ═══════════════════════════════════════════════════════════════════════

class CloudLLMCompatibleClient(LLMClient):
    """
    Klient pre OpenAI-kompatibilné endpointy (OpenAI, Groq, Mistral, ...).

    Každý request je ČISTÝ kontext – preamble je ignorovaný.

    Hierarchia structured output (od najprísnejšieho):
      1. response_format json_schema (OpenAI strict mode)
      2. function calling s json_schema ako parametrami
      3. response_format json_object (fallback)
    """

    def __init__(
        self,
        base_url: str | None = None,
        api_key:  str | None = None,
        model:    str | None = None,
        timeout:  int | None = None,
    ):
        self.base_url = (base_url or settings.openai_base_url).rstrip("/")
        self.api_key  = api_key  or settings.openai_api_key
        self.model    = model    or settings.openai_model
        self.timeout  = timeout  or settings.llm_timeout

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type":  "application/json",
        }

    def _post_with_retry(self, payload: dict) -> dict:
        max_retries = max(settings.llm_max_retries, 1)
        for attempt in range(1, max_retries + 1):
            with httpx.Client(timeout=self.timeout) as client:
                resp = client.post(
                    f"{self.base_url}/chat/completions",
                    json=payload,
                    headers=self._headers(),
                )
            if resp.status_code in {429, 500, 502, 503, 504} and attempt < max_retries:
                time.sleep(settings.llm_retry_base_delay * attempt)
                continue
            resp.raise_for_status()
            return resp.json()
        raise RuntimeError("LLM požiadavka zlyhala po viacerých pokusoch.")

    def complete(
        self,
        system_prompt: str,
        user_message:  str,
        *,
        json_schema: dict[str, Any] | None = None,
        preamble:    list[dict]     | None = None,   # ignorované pre Cloud
    ) -> str:
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": user_message},
        ]

        # Jednoduchý json_object režim ak nie je schéma (napr. health_check)
        if json_schema is None:
            payload = {
                "model":           self.model,
                "messages":        messages,
                "response_format": {"type": "json_object"},
            }
            data    = self._post_with_retry(payload)
            return data["choices"][0]["message"]["content"]

        # --- Pokus 1: Structured output s JSON Schema ---
        try:
            payload = {
                "model":    self.model,
                "messages": messages,
                "response_format": {
                    "type":        "json_schema",
                    "json_schema": {
                        "name":   "extract_result",
                        "strict": True,
                        "schema": json_schema,
                    },
                },
            }
            data    = self._post_with_retry(payload)
            content = data["choices"][0]["message"]["content"]
            json.loads(content)   # validácia parsovateľnosti
            return content
        except Exception:
            pass

        # --- Pokus 2: Function calling s json_schema ---
        func_def = {
            "name":        "extract_result",
            "description": "Extrahuj štruktúrované dáta zo vstupu.",
            "parameters":  json_schema,
        }
        try:
            payload = {
                "model":       self.model,
                "messages":    messages,
                "tools":       [{"type": "function", "function": func_def}],
                "tool_choice": {"type": "function", "function": {"name": "extract_result"}},
            }
            data = self._post_with_retry(payload)
            msg  = data["choices"][0]["message"]
            if "tool_calls" in msg and msg["tool_calls"]:
                args = msg["tool_calls"][0]["function"].get("arguments", "{}")
                json.loads(args)
                return args
        except Exception:
            pass

        # --- Pokus 3: json_object fallback ---
        payload = {
            "model":           self.model,
            "messages":        messages,
            "response_format": {"type": "json_object"},
        }
        data    = self._post_with_retry(payload)
        return data["choices"][0]["message"]["content"]


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

def get_llm_client(provider: str | None = None) -> LLMClient:
    selected = (provider or settings.llm_provider or "openai").lower()

    if selected == "ollama":
        client = OllamaClient()
        if client.health_check():
            print(f"[LLM] Ollama: {settings.local_llm_model}")
            return client
        print("[LLM] Ollama nedostupná – prepínam na OpenAI endpoint")

    print(f"[LLM] OpenAI-kompatibilný endpoint: {settings.openai_model}")
    return CloudLLMCompatibleClient()


def create_authors_session(client: LLMClient) -> LLMSession:
    """Vytvorí LLM session pre extrakciu UTB autorov z afiliácií."""
    from src.llm.prompts.authors import (
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
    from src.llm.prompts.dates import (
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


# ═══════════════════════════════════════════════════════════════════════
# Pomocné funkcie
# ═══════════════════════════════════════════════════════════════════════

def parse_llm_json_output(raw: str) -> dict:
    """
    Vyčistí markdown obal a parsuje JSON z odpovede LLM.
    Toleruje backticky, prefix text, suffix text.
    """
    cleaned = re.sub(r"```(?:json)?\s*", "", raw).strip().strip("`").strip()
    match   = re.search(r"\{.*\}", cleaned, re.DOTALL)
    if match:
        cleaned = match.group(0)
    return json.loads(cleaned)
