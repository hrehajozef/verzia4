"""Načítanie konfigurácie projektu z prostredia a .env súboru."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[0] / ".env")   # toto je cesta k .env súboru, ktorý je 2 úrovne nad src/config


DEFAULT_UTB_KEYWORDS = [
    "tomas bata univ",
    "tomas bata university",
    "univerzita tomase bati",
    "utb zlin",
    "zlin",
    "t. bata univ",
    "utb",
    "tbu"
]


def _get_str(key: str, default: str = "") -> str:
    """
    Načíta string hodnotu z .env
    """
    return os.getenv(key, default).strip()


def _get_int(key: str, default: int = 0) -> int:
    """
    Načíta číselnú hodnotu z .env
    """
    try:
        return int(_get_str(key, str(default)))
    except ValueError:
        return default


def _get_float(key: str, default: float) -> float:
    try:
        return float(_get_str(key, str(default)))
    except ValueError:
        return default


def _get_csv_list(key: str, default: list[str]) -> list[str]:
    value = _get_str(key)
    if not value:
        return default
    return [part.strip() for part in value.split(",") if part.strip()]


@dataclass
class Settings:
    remote_db_host: str = field(default_factory=lambda: _get_str("REMOTE_DB_HOST"))
    remote_db_port: int = field(default_factory=lambda: _get_int("REMOTE_DB_PORT"))
    remote_db_name: str = field(default_factory=lambda: _get_str("REMOTE_DB_NAME"))
    remote_db_user: str = field(default_factory=lambda: _get_str("REMOTE_DB_USER"))
    remote_db_password: str = field(default_factory=lambda: _get_str("REMOTE_DB_PASSWORD"))

    local_db_host: str = field(default_factory=lambda: _get_str("LOCAL_DB_HOST"))
    local_db_port: int = field(default_factory=lambda: _get_int("LOCAL_DB_PORT"))
    local_db_name: str = field(default_factory=lambda: _get_str("LOCAL_DB_NAME"))
    local_db_user: str = field(default_factory=lambda: _get_str("LOCAL_DB_USER"))
    local_db_password: str = field(default_factory=lambda: _get_str("LOCAL_DB_PASSWORD"))

    remote_schema: str = field(default_factory=lambda: _get_str("REMOTE_SCHEMA"))
    remote_table: str = field(default_factory=lambda: _get_str("REMOTE_TABLE"))
    local_schema: str = field(default_factory=lambda: _get_str("LOCAL_SCHEMA"))
    local_table: str = field(default_factory=lambda: _get_str("LOCAL_TABLE"))

    copy_batch_size: int = field(default_factory=lambda: _get_int("COPY_BATCH_SIZE", 500))
    copy_limit: int = field(default_factory=lambda: _get_int("COPY_LIMIT", 0))

    utb_keywords: list[str] = field(
        default_factory=lambda: _get_csv_list("UTB_KEYWORDS", DEFAULT_UTB_KEYWORDS)
    )

    author_match_threshold: float = field(
        default_factory=lambda: _get_float("AUTHOR_MATCH_THRESHOLD", 0.85)
    )
    heuristics_batch_size: int = field(
        default_factory=lambda: _get_int("HEURISTICS_BATCH_SIZE", 200)
    )

    llm_provider: str = field(default_factory=lambda: _get_str("LLM_PROVIDER"))
    local_llm_base_url: str = field(
        default_factory=lambda: _get_str("LOCAL_LLM_BASE_URL")
    )
    local_llm_model: str = field(default_factory=lambda: _get_str("LOCAL_LLM_MODEL"))
    openai_base_url: str = field(
        default_factory=lambda: _get_str("OPENAI_BASE_URL")
    )
    openai_api_key: str = field(default_factory=lambda: _get_str("OPENAI_API_KEY"))
    openai_model: str = field(default_factory=lambda: _get_str("OPENAI_MODEL"))
    llm_batch_size: int = field(default_factory=lambda: _get_int("LLM_BATCH_SIZE", 20))
    llm_timeout: int = field(default_factory=lambda: _get_int("LLM_TIMEOUT", 60))
    llm_max_retries: int = field(default_factory=lambda: _get_int("LLM_MAX_RETRIES", 3))
    llm_retry_base_delay: float = field(
        default_factory=lambda: _get_float("LLM_RETRY_BASE_DELAY", 1.5)
    )

    @property
    def remote_db_url(self) -> str:
        return (
            f"postgresql+psycopg://{self.remote_db_user}:{self.remote_db_password}"
            f"@{self.remote_db_host}:{self.remote_db_port}/{self.remote_db_name}"
        )

    @property
    def local_db_url(self) -> str:
        return (
            f"postgresql+psycopg://{self.local_db_user}:{self.local_db_password}"
            f"@{self.local_db_host}:{self.local_db_port}/{self.local_db_name}"
        )

    @property
    def local_table_full(self) -> str:
        return f"{self.local_schema}.{self.local_table}"

    @property
    def remote_table_full(self) -> str:
        return f"{self.remote_schema}.{self.remote_table}"  # Celý názov schémy a tabuľky


settings = Settings()