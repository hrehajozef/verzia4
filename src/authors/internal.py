"""Import a matching interných autorov UTB.

Zdroj autorov: CSV súbor (surname;firstname, s hlavičkou, 1 riadok = 1 osoba).

Matching prebieha štandardne na PRESNÝCH surových menách s diakritikou.
Voliteľne (normalize=True) sa zapínajú normalizované zhody + fuzzy Jaro-Winkler.

Načítanie z remote DB (tabuľky obd_prac, obd_lideprac, S_LIDE) je
zakomentované – bude k dispozícii keď budú prístupné remote tabuľky.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path

import jellyfish
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.config.settings import settings
from src.db.engines import get_local_engine


# -----------------------------------------------------------------------
# Dátové štruktúry
# -----------------------------------------------------------------------

@dataclass(frozen=True)
class InternalAuthor:
    surname:   str
    firstname: str

    @property
    def full_name(self) -> str:
        """Meno vo formáte 'Priezvisko, Meno'."""
        return f"{self.surname}, {self.firstname}" if self.firstname else self.surname

    @property
    def full_name_reversed(self) -> str:
        """Alternatívny formát 'Meno Priezvisko' pre matching."""
        return f"{self.firstname} {self.surname}".strip()


@dataclass(frozen=True)
class MatchResult:
    input_name: str
    matched:    bool
    author:     InternalAuthor | None = None
    score:      float                 = 0.0
    match_type: str                   = "none"   # exact_diacritic | exact_normalized | fuzzy | none


# -----------------------------------------------------------------------
# Normalizácia – pre voliteľný normalize režim
# -----------------------------------------------------------------------

def _normalize_name(name: str) -> str:
    """Lowercase + bez diakritiky + komprimované medzery."""
    if not name:
        return ""
    nfd    = unicodedata.normalize("NFD", name)
    no_acc = "".join(ch for ch in nfd if unicodedata.category(ch) != "Mn")
    return re.sub(r"\s+", " ", no_acc.lower()).strip()


# -----------------------------------------------------------------------
# Načítanie autorov z CSV (surname;firstname, 1 riadok = 1 osoba, s hlavičkou)
# -----------------------------------------------------------------------

def load_authors_from_csv(csv_path: str | Path) -> list[InternalAuthor]:
    """Načíta autorov z CSV súboru. Formát: priezvisko;krstné_meno, 1 riadok = 1 osoba."""
    import csv as _csv
    path    = Path(csv_path)
    authors: list[InternalAuthor] = []
    seen:    set[str]             = set()

    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = _csv.reader(handle, delimiter=";")
        next(reader, None)   # preskočiť hlavičku
        for row in reader:
            surname   = row[0].strip() if len(row) > 0 else ""
            firstname = row[1].strip() if len(row) > 1 else ""
            if not surname:
                continue
            key = f"{surname}|{firstname}"
            if key in seen:
                continue
            seen.add(key)
            authors.append(InternalAuthor(surname=surname, firstname=firstname))

    return authors


# -----------------------------------------------------------------------
# Načítanie autorov zo školskej remote DB – zatiaľ nedostupné
# -----------------------------------------------------------------------

# _REMOTE_QUERY = text("""
#     SELECT
#         l.jmeno        AS firstname,
#         l.prijmeni     AS surname,
#         o.kodprac      AS workplace_code,
#         o.nazev        AS workplace_name,
#         COALESCE(p.kodprac, '')  AS parent_code,
#         COALESCE(p.nazev,   '')  AS parent_name
#     FROM obd_prac         AS o
#     JOIN obd_lideprac     AS ol ON o.id         = ol.idprac
#     JOIN "S_LIDE"         AS l  ON ol.idlide    = l.id
#     LEFT JOIN obd_prac    AS p  ON o.id_nadrizene = p.id
#     WHERE l.jmeno    IS NOT NULL
#       AND l.prijmeni IS NOT NULL
#       AND l.jmeno    <> ''
#       AND l.prijmeni <> ''
# """)
#
#
# def load_authors_from_remote_db(
#     remote_engine: Engine | None = None,
# ) -> list[InternalAuthor]:
#     """
#     Načíta zamestnancov UTB zo školskej DB.
#     Jedna osoba môže mať viac pracovísk → viac záznamov.
#     """
#     engine  = remote_engine or get_remote_engine()
#     authors: list[InternalAuthor] = []
#     seen:    set[str]             = set()
#
#     with engine.connect() as conn:
#         rows = conn.execute(_REMOTE_QUERY).fetchall()
#
#     for row in rows:
#         firstname = (row.firstname or "").strip()
#         surname   = (row.surname   or "").strip()
#         if not surname:
#             continue
#
#         workplace_code = (row.workplace_code or "").strip()
#         # workplace_name = (row.workplace_name or "").strip()
#         # parent_code    = (row.parent_code    or "").strip()
#         # parent_name    = (row.parent_name    or "").strip()
#
#         key = f"{surname}|{firstname}|{workplace_code}"
#         if key in seen:
#             continue
#         seen.add(key)
#
#         authors.append(InternalAuthor(surname=surname, firstname=firstname))
#
#     return authors


# -----------------------------------------------------------------------
# DB tabuľka pre lokálne uloženie registra – len surname a firstname
# -----------------------------------------------------------------------

def setup_authors_table(engine: Engine) -> None:
    ddl = """
    DROP TABLE IF EXISTS utb_internal_authors CASCADE;
    CREATE TABLE utb_internal_authors (
        id        SERIAL PRIMARY KEY,
        surname   TEXT NOT NULL,
        firstname TEXT
    );
    CREATE INDEX idx_utb_internal_authors_surname ON utb_internal_authors (surname);
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def import_authors_to_db(authors: list[InternalAuthor], engine: Engine) -> int:
    insert_sql = text("""
        INSERT INTO utb_internal_authors (surname, firstname)
        VALUES (:surname, :firstname)
    """)
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE utb_internal_authors RESTART IDENTITY"))
        conn.execute(insert_sql, [
            {"surname": a.surname, "firstname": a.firstname}
            for a in authors
        ])
    return len(authors)


# -----------------------------------------------------------------------
# Registra cache
# -----------------------------------------------------------------------

_AUTHOR_REGISTRY: list[InternalAuthor] = []


def clear_author_registry_cache() -> None:
    _AUTHOR_REGISTRY.clear()


def get_author_registry(engine: Engine | None = None) -> list[InternalAuthor]:
    if _AUTHOR_REGISTRY:
        return _AUTHOR_REGISTRY

    with (engine or get_local_engine()).connect() as conn:
        rows = conn.execute(text("""
            SELECT surname, firstname FROM utb_internal_authors
        """)).fetchall()

    _AUTHOR_REGISTRY.extend(
        InternalAuthor(surname=r.surname, firstname=r.firstname or "")
        for r in rows
    )
    return _AUTHOR_REGISTRY


# -----------------------------------------------------------------------
# Matching
# -----------------------------------------------------------------------

def match_author(
    candidate_name: str,
    registry:       list[InternalAuthor],
    threshold:      float = 0.85,
    normalize:      bool  = False,
) -> MatchResult:
    """
    Porovná kandidátske meno s registrom interných autorov.

    normalize=False (default):
        1. Presná zhoda s diakritikou ('Novák, Jan' == 'Novák, Jan')
        2. Fuzzy Jaro-Winkler na SUROVÝCH (nenormalizovaných) menách

    normalize=True:
        1. Presná zhoda s diakritikou
        2. Presná zhoda normalizovaná ('novak, jan' == 'novak, jan')
        3. Fuzzy Jaro-Winkler na NORMALIZOVANÝCH menách
    """
    if not candidate_name or not candidate_name.strip():
        return MatchResult(input_name=candidate_name, matched=False)

    # --- Krok 1: Presná zhoda s diakritikou (vždy aktívna) ---
    for a in registry:
        if a.full_name == candidate_name or a.full_name_reversed == candidate_name:
            return MatchResult(candidate_name, True, a, 1.0, "exact_diacritic")

    if normalize:
        norm_candidate = _normalize_name(candidate_name)

        # --- Krok 2: Presná zhoda normalizovaná ---
        for a in registry:
            if _normalize_name(a.full_name) == norm_candidate:
                return MatchResult(candidate_name, True, a, 1.0, "exact_normalized")

        # --- Krok 3: Fuzzy Jaro-Winkler na normalizovaných menách ---
        best_score  = 0.0
        best_author: InternalAuthor | None = None
        seen_norms: set[str] = set()
        for author in registry:
            norm_a = _normalize_name(author.full_name)
            if norm_a in seen_norms:
                continue
            seen_norms.add(norm_a)
            score = jellyfish.jaro_winkler_similarity(norm_candidate, norm_a)
            if score > best_score:
                best_score  = score
                best_author = author

        if best_author and best_score >= threshold:
            return MatchResult(candidate_name, True, best_author, best_score, "fuzzy")
        return MatchResult(candidate_name, False, None, best_score, "none")

    else:
        # --- Krok 2 (bez normalize): Fuzzy Jaro-Winkler na surových menách ---
        best_score  = 0.0
        best_author: InternalAuthor | None = None
        seen_names: set[str] = set()
        for author in registry:
            raw_a = author.full_name
            if raw_a in seen_names:
                continue
            seen_names.add(raw_a)
            score = jellyfish.jaro_winkler_similarity(candidate_name, raw_a)
            if score > best_score:
                best_score  = score
                best_author = author

        if best_author and best_score >= threshold:
            return MatchResult(candidate_name, True, best_author, best_score, "fuzzy")
        return MatchResult(candidate_name, False, None, best_score, "none")


def match_authors_batch(
    candidate_names: list[str],
    registry:        list[InternalAuthor],
    threshold:       float | None = None,
    normalize:       bool         = False,
) -> list[MatchResult]:
    threshold = settings.author_match_threshold if threshold is None else threshold
    return [
        match_author(name, registry, threshold, normalize=normalize)
        for name in candidate_names
    ]
