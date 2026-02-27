"""Klienti pre LLM providery a pomocné parsovanie JSON odpovede."""

from __future__ import annotations

import json
import re
import time
from abc import ABC, abstractmethod

import httpx

from src.config.settings import settings


class LLMClient(ABC):
    """Základné rozhranie pre LLM klientov."""

    @abstractmethod
    def complete(self, system_prompt: str, user_message: str) -> str:
        """Pošle prompt provideru a vráti textovú odpoveď."""

        raise NotImplementedError

    def health_check(self) -> bool:
        """Overí dostupnosť providera jednoduchým test dopytom."""

        try:
            return bool(self.complete("Odpovedz iba ok", "test"))
        except Exception:
            return False


class OllamaClient(LLMClient):
    """Implementácia klienta pre Ollama API endpoint."""

    def __init__(self, base_url: str | None = None, model: str | None = None, timeout: int | None = None):
        """Inicializuje endpoint, model a timeout hodnoty."""

        self.base_url = (base_url or settings.local_llm_base_url).rstrip("/")
        self.model = model or settings.local_llm_model
        self.timeout = timeout or settings.llm_timeout

    def complete(self, system_prompt: str, user_message: str) -> str:
        """Pošle chat požiadavku na Ollama a vráti čistý obsah správy."""

        with httpx.Client(timeout=self.timeout) as client:
            response = client.post(
                f"{self.base_url}/api/chat",
                json={
                    "model": self.model,
                    "messages": [
                        {"role": "system", "content": system_prompt},
                        {"role": "user", "content": user_message},
                    ],
                    "stream": False,
                    "format": "json",
                },
            )
            response.raise_for_status()
            return response.json()["message"]["content"]


class OpenAICompatibleClient(LLMClient):
    """Implementácia klienta pre OpenAI kompatibilné endpointy."""

    def __init__(self, base_url: str | None = None, api_key: str | None = None, model: str | None = None, timeout: int | None = None):
        """Inicializuje endpoint, API token, model a timeout."""

        self.base_url = (base_url or settings.openai_base_url).rstrip("/")
        self.api_key = api_key or settings.openai_api_key
        self.model = model or settings.openai_model
        self.timeout = timeout or settings.llm_timeout

    def complete(self, system_prompt: str, user_message: str) -> str:
        """Pošle chat požiadavku s retry mechanizmom a vráti obsah odpovede."""

        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
            "response_format": {"type": "json_object"},
        }
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }

        max_retries = max(settings.llm_max_retries, 1)
        for attempt in range(1, max_retries + 1):
            with httpx.Client(timeout=self.timeout) as client:
                response = client.post(
                    f"{self.base_url}/chat/completions",
                    json=payload,
                    headers=headers,
                )

            if response.status_code in {429, 500, 502, 503, 504} and attempt < max_retries:
                sleep_time = settings.llm_retry_base_delay * attempt
                time.sleep(sleep_time)
                continue

            response.raise_for_status()
            return response.json()["choices"][0]["message"]["content"]

        raise RuntimeError("LLM požiadavka zlyhala po viacerých pokusoch")


def get_llm_client(provider: str | None = None) -> LLMClient:
    """Vyberie klienta podľa konfigurácie a fallback pravidiel."""

    selected = (provider or settings.llm_provider).lower()
    if selected == "ollama":
        client = OllamaClient()
        if client.health_check():
            print(f"[LLM] Používam Ollama model {settings.local_llm_model}")
            return client
        print("[LLM] Ollama nedostupná, prepínam na OpenAI kompatibilný endpoint")

    print(f"[LLM] Používam OpenAI kompatibilný model {settings.openai_model}")
    return OpenAICompatibleClient()


def parse_llm_json_output(raw_output: str) -> dict:
    """Vyčistí markdown obal a parsuje prvý JSON objekt z odpovede."""

    cleaned = re.sub(r"```(?:json)?\s*", "", raw_output).strip().strip("`").strip()
    match = re.search(r"\{.*\}", cleaned, re.DOTALL)
    if match:
        cleaned = match.group(0)
    return json.loads(cleaned)
