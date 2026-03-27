"""Import a matching interných autorov UTB.

Zdroj autorov pre matching: CSV súbor → lokálna tabuľka utb_internal_authors.
Import: load_authors_from_csv() → import_authors_to_db() (príkaz import-authors).

Matching prebieha na PRESNÝCH surových menách s diakritikou.
Voliteľne (normalize=True) sa zapínajú normalizované zhody + fuzzy Jaro-Winkler.

Afiliácie (fakulta, ústav) sa zisťujú osobitne pre každého nájdeného autora
priamo z remote DB (lookup_author_affiliations). WoS má prednosť, remote DB
je fallback.
"""

from __future__ import annotations

import csv as _csv
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import jellyfish
from sqlalchemy import text

from src.config.settings import settings
from src.db.engines import get_local_engine, get_remote_engine

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine


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
# Načítanie autorov z CSV → lokálna DB
# -----------------------------------------------------------------------

def load_authors_from_csv(csv_path: str | Path) -> list[InternalAuthor]:
    """Načíta autorov z CSV súboru. Formát: priezvisko;krstné_meno, s hlavičkou."""
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


def setup_authors_table(engine: "Engine") -> None:
    """Zmaže a vytvorí tabuľku utb_internal_authors."""
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


def import_authors_to_db(authors: list[InternalAuthor], engine: "Engine") -> int:
    """Naplní tabuľku utb_internal_authors zoznamom autorov."""
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
# Registra cache (načítava z lokálnej DB)
# -----------------------------------------------------------------------

_AUTHOR_REGISTRY: list[InternalAuthor] = []


def clear_author_registry_cache() -> None:
    _AUTHOR_REGISTRY.clear()


def get_author_registry(engine: "Engine | None" = None) -> list[InternalAuthor]:
    """Vráti (prípadne načíta) zoznam interných autorov z lokálnej DB. Výsledok sa kešuje."""
    if _AUTHOR_REGISTRY:
        return _AUTHOR_REGISTRY

    with (engine or get_local_engine()).connect() as conn:
        rows = conn.execute(text(
            "SELECT surname, firstname FROM utb_internal_authors"
        )).fetchall()

    _AUTHOR_REGISTRY.extend(
        InternalAuthor(surname=r.surname, firstname=r.firstname or "")
        for r in rows
    )
    return _AUTHOR_REGISTRY


# -----------------------------------------------------------------------
# Per-autor lookup afiliácií z remote DB
# -----------------------------------------------------------------------

def _is_faculty_level(name: str) -> bool:
    """Vráti True ak názov pracoviska je fakulta."""
    low = name.lower()
    return "fakult" in low or "faculty" in low


def _is_top_level(name: str) -> bool:
    """Vráti True ak pracovisko je rektorát / celouniverzitné."""
    low = name.lower()
    return any(kw in low for kw in ("universit", "univerzit", "rektora", "rektorát"))


def _is_real_ou(name: str) -> bool:
    """Vráti True ak pracovisko je skutočný ústav (nie fakulta ani rektorát)."""
    return bool(name) and not _is_faculty_level(name) and not _is_top_level(name)


def _extract_person_data(
    workplaces: list[tuple[str, str]],
) -> tuple[tuple[str, ...], str]:
    """
    Z viacerých pracovísk jednej osoby určí (fakulty, najlepší_ústav).

    Priorita pre ústav:
      1. Pracovisko = skutočný ústav, rodič = fakulta
      2. Priame priradenie k fakulte
      3. Nič → ("", "")

    Vracia: (tuple českých názvov fakúlt, český názov ústavu)
    """
    faculties: set[str]  = set()
    ou_candidates: list[str] = []

    for workplace_name, parent_name in workplaces:
        if _is_faculty_level(workplace_name):
            faculties.add(workplace_name.strip())

        if (
            _is_real_ou(workplace_name)
            and parent_name
            and _is_faculty_level(parent_name)
        ):
            faculties.add(parent_name.strip())
            ou_candidates.append(workplace_name.strip())

    best_ou = max(ou_candidates, key=len) if ou_candidates else ""
    return tuple(sorted(faculties)), best_ou


_REMOTE_SCHEMA = settings.remote_schema

_AFFILIATION_CACHE: dict[tuple[str, str], tuple[tuple[str, ...], str]] = {}


def clear_affiliation_cache() -> None:
    _AFFILIATION_CACHE.clear()


def lookup_author_affiliations(
    surname:       str,
    firstname:     str,
    remote_engine: "Engine | None" = None,
) -> tuple[tuple[str, ...], str]:
    """
    Vyhľadá fakulty a ústav konkrétneho autora v remote DB.

    Výsledky sa kešujú pre celý beh pipeline.
    Vracia: (tuple českých názvov fakúlt, český názov ústavu)
    """
    key = (surname, firstname)
    if key in _AFFILIATION_CACHE:
        return _AFFILIATION_CACHE[key]

    engine = remote_engine or get_remote_engine()
    s      = _REMOTE_SCHEMA
    sql    = text(f"""
        SELECT
            o.nazev        AS workplace_name,
            COALESCE(p.nazev, '') AS parent_name
        FROM "{s}".obd_prac     AS o
        JOIN "{s}".obd_lideprac AS ol ON o.id           = ol.idprac
        JOIN "{s}".s_lide       AS l  ON ol.idlide      = l.id
        LEFT JOIN "{s}".obd_prac AS p ON o.id_nadrizene = p.id
        WHERE l.prijmeni = :surname
          AND l.jmeno    = :firstname
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql, {"surname": surname, "firstname": firstname}).fetchall()

    if not rows:
        result: tuple[tuple[str, ...], str] = ((), "")
    else:
        workplaces = list({
            (r.workplace_name or "", r.parent_name or "")
            for r in rows
        })
        result = _extract_person_data(workplaces)

    _AFFILIATION_CACHE[key] = result
    return result


# -----------------------------------------------------------------------
# Matching
# -----------------------------------------------------------------------

def _extract_surname_norm(name: str) -> str:
    """
    Extrahuje a normalizuje priezvisko z mena.
    Formát 'Priezvisko, Meno' → 'priezvisko'.
    Formát 'M. Priezvisko' (bez čiarky) → posledné slovo.
    """
    if "," in name:
        return _normalize_name(name.split(",")[0].strip())
    parts = name.strip().split()
    return _normalize_name(parts[-1]) if parts else _normalize_name(name)


def match_author(
    candidate_name:       str,
    registry:             list[InternalAuthor],
    threshold:            float = 0.85,
    normalize:            bool  = False,
    require_surname_match: bool = False,
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

    require_surname_match=True:
        Pred fuzzy matching sa registre filtrujú tak, aby normalizované priezvisko
        kandidáta presne zodpovedalo priezvisku z registra.
        Eliminuje falošné pozitíva v Path B (kde niet WoS scoping).
        Presné zhody (exact_diacritic / exact_normalized) nie sú touto voľbou dotknuté.
    """
    if not candidate_name or not candidate_name.strip():
        return MatchResult(input_name=candidate_name, matched=False)

    # --- Krok 1: Presná zhoda s diakritikou (vždy aktívna, bez surname filtra) ---
    for a in registry:
        if a.full_name == candidate_name or a.full_name_reversed == candidate_name:
            return MatchResult(candidate_name, True, a, 1.0, "exact_diacritic")

    # Pre fuzzy kroky: ak require_surname_match, predfiltrovanie na zhodné priezvisko
    if require_surname_match:
        candidate_sn = _extract_surname_norm(candidate_name)
        fuzzy_pool   = [a for a in registry if _extract_surname_norm(a.full_name) == candidate_sn]
    else:
        fuzzy_pool = registry

    if normalize:
        norm_candidate = _normalize_name(candidate_name)

        # --- Krok 2: Presná zhoda normalizovaná ---
        for a in registry:   # prechádza celý register – exact match ignoruje surname filter
            if _normalize_name(a.full_name) == norm_candidate:
                return MatchResult(candidate_name, True, a, 1.0, "exact_normalized")

        # --- Krok 3: Fuzzy Jaro-Winkler na normalizovaných menách ---
        best_score  = 0.0
        best_author: InternalAuthor | None = None
        seen_norms: set[str] = set()
        for author in fuzzy_pool:
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
        for author in fuzzy_pool:
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
