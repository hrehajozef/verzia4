"""Import a matching interných autorov UTB."""

from __future__ import annotations

import csv
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path

import jellyfish
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.config.settings import settings
from src.db.engines import get_local_engine


@dataclass(frozen=True)
class InternalAuthor:
    surname: str
    firstname: str
    norm_name: str

    @property
    def full_name(self) -> str:
        """
        Vráti celé meno autora vo formáte "Priezvisko, Meno" alebo len "Priezvisko".
        """
        return f"{self.surname}, {self.firstname}" if self.firstname else self.surname


@dataclass(frozen=True)
class MatchResult:
    input_name: str
    matched: bool
    author: InternalAuthor | None = None
    score: float = 0.0
    match_type: str = "none"


def _normalize_name(name: str) -> str:
    if not name:
        return ""
    normalized = unicodedata.normalize("NFD", name)
    without_marks = "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")
    compact = re.sub(r"\s+", " ", without_marks.lower()).strip()
    return compact


def load_authors_from_csv(csv_path: str | Path) -> list[InternalAuthor]:
    path = Path(csv_path)
    authors: list[InternalAuthor] = []
    seen: set[str] = set()

    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.reader(handle, delimiter=";")
        next(reader, None)
        for row in reader:
            padded = row + [""] * (8 - len(row))
            for idx in range(0, 8, 2):
                surname = padded[idx].strip()
                firstname = padded[idx + 1].strip()
                if not surname:
                    continue
                norm = _normalize_name(f"{surname}, {firstname}")
                if norm in seen:
                    continue
                seen.add(norm)
                authors.append(InternalAuthor(surname=surname, firstname=firstname, norm_name=norm))

    return authors


def setup_authors_table(engine: Engine) -> None:
    ddl = """
    DROP TABLE IF EXISTS utb_internal_authors CASCADE;
    CREATE TABLE utb_internal_authors (
        id SERIAL PRIMARY KEY,
        surname TEXT NOT NULL,
        firstname TEXT,
        norm_name TEXT NOT NULL
    );
    CREATE INDEX idx_utb_internal_authors_norm ON utb_internal_authors (norm_name);
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def import_authors_to_db(authors: list[InternalAuthor], engine: Engine) -> int:
    insert_sql = text(
        """
        INSERT INTO utb_internal_authors (surname, firstname, norm_name)
        VALUES (:surname, :firstname, :norm_name)
        """
    )
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE utb_internal_authors RESTART IDENTITY"))
        conn.execute(
            insert_sql,
            [
                {"surname": a.surname, "firstname": a.firstname, "norm_name": a.norm_name}
                for a in authors
            ],
        )
    return len(authors)


_AUTHOR_REGISTRY: list[InternalAuthor] = []


def clear_author_registry_cache() -> None:
    _AUTHOR_REGISTRY.clear()


def get_author_registry(engine: Engine | None = None) -> list[InternalAuthor]:
    if _AUTHOR_REGISTRY:
        return _AUTHOR_REGISTRY

    query = text("SELECT surname, firstname, norm_name FROM utb_internal_authors")
    with (engine or get_local_engine()).connect() as conn:
        rows = conn.execute(query).fetchall()
    _AUTHOR_REGISTRY.extend(
        InternalAuthor(row.surname, row.firstname, row.norm_name) for row in rows
    )
    return _AUTHOR_REGISTRY


def match_author(
    candidate_name: str,
    registry: list[InternalAuthor],
    threshold: float = 0.85,
) -> MatchResult:
    """
    Porovnáva 1 kandidátske meno s registrom interných autorov a vráti výsledok porovnávania.
    """
    if not candidate_name or not candidate_name.strip():
        return MatchResult(input_name=candidate_name, matched=False, match_type="none")

    normalized_candidate = _normalize_name(candidate_name)

    for author in registry:
        if normalized_candidate == author.norm_name:
            return MatchResult(candidate_name, True, author, 1.0, "exact")

    best_score = 0.0
    best_author: InternalAuthor | None = None
    for author in registry:
        score = jellyfish.jaro_winkler_similarity(normalized_candidate, author.norm_name)
        if score > best_score:
            best_score = score
            best_author = author

    if best_author and best_score >= threshold:
        return MatchResult(candidate_name, True, best_author, best_score, "fuzzy")

    return MatchResult(candidate_name, False, None, best_score, "none")


def match_authors_batch(
    candidate_names: list[str],
    registry: list[InternalAuthor],
    threshold: float | None = None,
) -> list[MatchResult]:
    """
    Porovnáva ZOZNAM kandidátskych mien s registrom interných autorov a vráti výsledky porovnávania.
    """
    threshold = settings.author_match_threshold if threshold is None else threshold
    return [match_author(name, registry, threshold) for name in candidate_names]
