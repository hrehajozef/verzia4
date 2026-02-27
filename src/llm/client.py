"""LLM klienti s podporou structured output / function calling."""

from __future__ import annotations

import json
import re
import time
from abc import ABC, abstractmethod

import httpx

from src.config.settings import settings
from src.llm.prompt import LLM_FUNCTION_DEF, LLM_OUTPUT_JSON_SCHEMA


class LLMClient(ABC):

    @abstractmethod
    def complete(self, system_prompt: str, user_message: str) -> str:
        raise NotImplementedError

    def health_check(self) -> bool:
        try:
            return bool(self.complete("Odpovedz iba ok", "test"))
        except Exception:
            return False


class OllamaClient(LLMClient):
    """
    Klient pre lokálnu Ollamu.
    Použije format='json' pre garantovaný JSON výstup.
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

    def complete(self, system_prompt: str, user_message: str) -> str:
        with httpx.Client(timeout=self.timeout) as client:
            response = client.post(
                f"{self.base_url}/api/chat",
                json={
                    "model":    self.model,
                    "messages": [
                        {"role": "system",  "content": system_prompt},
                        {"role": "user",    "content": user_message},
                    ],
                    "stream": False,
                    "format": "json",   # Ollama JSON mode
                },
            )
            response.raise_for_status()
            return response.json()["message"]["content"]


class OpenAICompatibleClient(LLMClient):
    """
    Klient pre OpenAI-kompatibilné endpointy (OpenAI, Groq, Mistral, ...).

    Hierarchia structured output (od najprísnejšieho):
    1. response_format structured output so schémou (OpenAI >= gpt-4o-2024-08-06)
    2. function calling (ak endpoint podporuje tools)
    3. response_format json_object (Groq, starší OpenAI)
    Fallback automaticky na json_object ak structured output zlyhá.
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
        """Odošle POST s retry logikou pre 429/5xx."""
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

    def complete(self, system_prompt: str, user_message: str) -> str:
        """
        Pokúsi sa o structured output so schémou; fallback na json_object.
        Vráti surový JSON string z odpovede LLM.
        """
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user",   "content": user_message},
        ]

        # --- Pokus 1: Structured output s JSON Schema (OpenAI strict mode) ---
        payload_structured = {
            "model":    self.model,
            "messages": messages,
            "response_format": {
                "type":        "json_schema",
                "json_schema": {
                    "name":   "extract_utb_authors",
                    "strict": True,
                    "schema": LLM_OUTPUT_JSON_SCHEMA,
                },
            },
        }
        try:
            data = self._post_with_retry(payload_structured)
            content = data["choices"][0]["message"]["content"]
            # Overenie, že výstup je parsovateľný JSON
            json.loads(content)
            return content
        except Exception:
            pass  # Endpoint nepodporuje json_schema – skúsime ďalšiu metódu

        # --- Pokus 2: Function calling ---
        payload_func = {
            "model":       self.model,
            "messages":    messages,
            "tools":       [{"type": "function", "function": LLM_FUNCTION_DEF}],
            "tool_choice": {"type": "function", "function": {"name": "extract_utb_authors"}},
        }
        try:
            data = self._post_with_retry(payload_func)
            msg  = data["choices"][0]["message"]
            if "tool_calls" in msg and msg["tool_calls"]:
                args = msg["tool_calls"][0]["function"].get("arguments", "{}")
                json.loads(args)   # validácia
                return args
        except Exception:
            pass

        # --- Pokus 3: json_object fallback (Groq, starší OpenAI) ---
        payload_json = {
            "model":           self.model,
            "messages":        messages,
            "response_format": {"type": "json_object"},
        }
        data    = self._post_with_retry(payload_json)
        content = data["choices"][0]["message"]["content"]
        return content


def get_llm_client(provider: str | None = None) -> LLMClient:
    selected = (provider or settings.llm_provider or "openai").lower()

    if selected == "ollama":
        client = OllamaClient()
        if client.health_check():
            print(f"[LLM] Ollama: {settings.local_llm_model}")
            return client
        print("[LLM] Ollama nedostupná – prepínam na OpenAI endpoint")

    print(f"[LLM] OpenAI-kompatibilný endpoint: {settings.openai_model}")
    return OpenAICompatibleClient()


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