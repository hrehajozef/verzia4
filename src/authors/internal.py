"""Import a matching interných autorov UTB.

Zdroj autorov: školská remote DB (tabuľky obd_prac, obd_lideprac, S_LIDE).
Obsahuje mená s diakritikou + kód a názov pracoviska (fakulta/oddelenie).

Matching prebieha na PRESNÝCH menách s diakritikou (nie normalizovaných).
Fuzzy matching (Jaro-Winkler) sa používa ako záloha pre WoS mená bez diakritiky.
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
from src.db.engines import get_local_engine, get_remote_engine


# -----------------------------------------------------------------------
# Dátové štruktúry
# -----------------------------------------------------------------------

@dataclass(frozen=True)
class InternalAuthor:
    surname:           str
    firstname:         str
    norm_name:         str   # lowercase bez diakritiky – pre fuzzy matching
    workplace_code:    str   # napr. "FT", "00000042"
    workplace_name:    str   # plný názov oddelenia/pracoviska
    parent_code:       str   # kód nadradeného pracoviska (fakulta)
    parent_name:       str   # plný názov fakulty

    @property
    def full_name(self) -> str:
        """Meno s diakritikou vo formáte 'Priezvisko, Meno'."""
        return f"{self.surname}, {self.firstname}" if self.firstname else self.surname

    @property
    def full_name_reversed(self) -> str:
        """Alternatívny formát 'Meno Priezvisko' pre matching."""
        return f"{self.firstname} {self.surname}".strip()


@dataclass(frozen=True)
class MatchResult:
    input_name:  str
    matched:     bool
    author:      InternalAuthor | None = None
    score:       float                 = 0.0
    match_type:  str                   = "none"   # exact_diacritic | exact_normalized | fuzzy | none


# -----------------------------------------------------------------------
# Normalizácia (iba pre fuzzy fallback)
# -----------------------------------------------------------------------

def _normalize_name(name: str) -> str:
    """Lowercase + bez diakritiky + komprimované medzery."""
    if not name:
        return ""
    nfd     = unicodedata.normalize("NFD", name)
    no_acc  = "".join(ch for ch in nfd if unicodedata.category(ch) != "Mn")
    return re.sub(r"\s+", " ", no_acc.lower()).strip()


# -----------------------------------------------------------------------
# Načítanie autorov zo školskej remote DB
# -----------------------------------------------------------------------

_REMOTE_QUERY = text("""
    SELECT
        l.jmeno        AS firstname,
        l.prijmeni     AS surname,
        o.kodprac      AS workplace_code,
        o.nazev        AS workplace_name,
        COALESCE(p.kodprac, '')  AS parent_code,
        COALESCE(p.nazev,   '')  AS parent_name
    FROM obd_prac         AS o
    JOIN obd_lideprac     AS ol ON o.id         = ol.idprac
    JOIN "S_LIDE"         AS l  ON ol.idlide    = l.id
    LEFT JOIN obd_prac    AS p  ON o.id_nadrizene = p.id
    WHERE l.jmeno    IS NOT NULL
      AND l.prijmeni IS NOT NULL
      AND l.jmeno    <> ''
      AND l.prijmeni <> ''
""")


def load_authors_from_remote_db(
    remote_engine: Engine | None = None,
) -> list[InternalAuthor]:
    """
    Načíta zamestnancov UTB zo školskej DB.
    Jedna osoba môže mať viac pracovísk → viac záznamov.
    """
    engine = remote_engine or get_remote_engine()
    authors: list[InternalAuthor] = []
    seen:    set[str]             = set()

    with engine.connect() as conn:
        rows = conn.execute(_REMOTE_QUERY).fetchall()

    for row in rows:
        firstname      = (row.firstname or "").strip()
        surname        = (row.surname   or "").strip()
        if not surname:
            continue

        workplace_code = (row.workplace_code or "").strip()
        workplace_name = (row.workplace_name or "").strip()
        parent_code    = (row.parent_code    or "").strip()
        parent_name    = (row.parent_name    or "").strip()

        # Deduplikácia: rovnaká osoba + rovnaké pracovisko
        key = f"{surname}|{firstname}|{workplace_code}"
        if key in seen:
            continue
        seen.add(key)

        norm = _normalize_name(f"{surname}, {firstname}")
        authors.append(InternalAuthor(
            surname        = surname,
            firstname      = firstname,
            norm_name      = norm,
            workplace_code = workplace_code,
            workplace_name = workplace_name,
            parent_code    = parent_code,
            parent_name    = parent_name,
        ))

    return authors


# -----------------------------------------------------------------------
# Fallback: načítanie z CSV (pôvodná metóda – ak remote DB nedostupná)
# -----------------------------------------------------------------------

def load_authors_from_csv(csv_path: str | Path) -> list[InternalAuthor]:
    """Načíta autorov z CSV súboru (bez info o pracovisku)."""
    import csv as _csv
    path    = Path(csv_path)
    authors: list[InternalAuthor] = []
    seen:    set[str]             = set()

    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = _csv.reader(handle, delimiter=";")
        next(reader, None)
        for row in reader:
            padded = row + [""] * (8 - len(row))
            for idx in range(0, 8, 2):
                surname   = padded[idx].strip()
                firstname = padded[idx + 1].strip()
                if not surname:
                    continue
                norm = _normalize_name(f"{surname}, {firstname}")
                if norm in seen:
                    continue
                seen.add(norm)
                authors.append(InternalAuthor(
                    surname        = surname,
                    firstname      = firstname,
                    norm_name      = norm,
                    workplace_code = "",
                    workplace_name = "",
                    parent_code    = "",
                    parent_name    = "",
                ))

    return authors


# -----------------------------------------------------------------------
# DB tabuľka pre lokálne uloženie registra
# -----------------------------------------------------------------------

def setup_authors_table(engine: Engine) -> None:
    ddl = """
    DROP TABLE IF EXISTS utb_internal_authors CASCADE;
    CREATE TABLE utb_internal_authors (
        id             SERIAL PRIMARY KEY,
        surname        TEXT NOT NULL,
        firstname      TEXT,
        norm_name      TEXT NOT NULL,
        workplace_code TEXT NOT NULL DEFAULT '',
        workplace_name TEXT NOT NULL DEFAULT '',
        parent_code    TEXT NOT NULL DEFAULT '',
        parent_name    TEXT NOT NULL DEFAULT ''
    );
    CREATE INDEX idx_utb_internal_authors_norm    ON utb_internal_authors (norm_name);
    CREATE INDEX idx_utb_internal_authors_surname ON utb_internal_authors (surname);
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))


def import_authors_to_db(authors: list[InternalAuthor], engine: Engine) -> int:
    insert_sql = text("""
        INSERT INTO utb_internal_authors
            (surname, firstname, norm_name, workplace_code, workplace_name, parent_code, parent_name)
        VALUES
            (:surname, :firstname, :norm_name, :workplace_code, :workplace_name, :parent_code, :parent_name)
    """)
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE utb_internal_authors RESTART IDENTITY"))
        conn.execute(insert_sql, [
            {
                "surname":        a.surname,
                "firstname":      a.firstname,
                "norm_name":      a.norm_name,
                "workplace_code": a.workplace_code,
                "workplace_name": a.workplace_name,
                "parent_code":    a.parent_code,
                "parent_name":    a.parent_name,
            }
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
            SELECT surname, firstname, norm_name,
                   workplace_code, workplace_name, parent_code, parent_name
            FROM utb_internal_authors
        """)).fetchall()

    _AUTHOR_REGISTRY.extend(
        InternalAuthor(
            surname        = r.surname,
            firstname      = r.firstname,
            norm_name      = r.norm_name,
            workplace_code = r.workplace_code,
            workplace_name = r.workplace_name,
            parent_code    = r.parent_code,
            parent_name    = r.parent_name,
        )
        for r in rows
    )
    return _AUTHOR_REGISTRY


# -----------------------------------------------------------------------
# Matching – presné mená majú prednosť pred fuzzy
# -----------------------------------------------------------------------

def match_author(
    candidate_name: str,
    registry:       list[InternalAuthor],
    threshold:      float = 0.85,
) -> MatchResult:
    """
    Porovná kandidátske meno s registrom v tomto poradí:

    1. PRESNÁ zhoda s diakritikou – 'Novák, Jan' == 'Novák, Jan'
       (dc.contributor.author obsahuje mená s diakritikou priamo z IS)
    2. PRESNÁ zhoda normalizovaná – 'novak, jan' == 'novak, jan'
       (pre prípad rôznych encodingov)
    3. FUZZY Jaro-Winkler – pre WoS mená bez diakritiky ('Novak, Jan')

    Pre každého autora v registri môže byť viac záznamov (viac pracovísk).
    Vracia prvý nájdený match – pri presnej zhode prioritizuje
    záznam s neprázdnym parent_name (= má afiliáciu).
    """
    if not candidate_name or not candidate_name.strip():
        return MatchResult(input_name=candidate_name, matched=False)

    norm_candidate = _normalize_name(candidate_name)

    # --- Krok 1: Presná zhoda s diakritikou ---
    exact_matches = [
        a for a in registry
        if a.full_name == candidate_name
        or f"{a.firstname} {a.surname}" == candidate_name
    ]
    if exact_matches:
        # Preferuj záznam s afiliáciou
        best = next((a for a in exact_matches if a.parent_name), exact_matches[0])
        return MatchResult(candidate_name, True, best, 1.0, "exact_diacritic")

    # --- Krok 2: Presná zhoda normalizovaná ---
    norm_matches = [
        a for a in registry
        if a.norm_name == norm_candidate
    ]
    if norm_matches:
        best = next((a for a in norm_matches if a.parent_name), norm_matches[0])
        return MatchResult(candidate_name, True, best, 1.0, "exact_normalized")

    # --- Krok 3: Fuzzy Jaro-Winkler ---
    best_score  = 0.0
    best_author: InternalAuthor | None = None
    # Deduplikuj registry podľa norm_name pre rýchlosť (viacero pracovísk tej istej osoby)
    seen_norms: set[str] = set()
    for author in registry:
        if author.norm_name in seen_norms:
            continue
        seen_norms.add(author.norm_name)
        score = jellyfish.jaro_winkler_similarity(norm_candidate, author.norm_name)
        if score > best_score:
            best_score  = score
            best_author = author

    if best_author and best_score >= threshold:
        # Nájdi variant s afiliáciou ak existuje
        variants = [a for a in registry if a.norm_name == best_author.norm_name]
        best = next((a for a in variants if a.parent_name), variants[0])
        return MatchResult(candidate_name, True, best, best_score, "fuzzy")

    return MatchResult(candidate_name, False, None, best_score, "none")


def match_authors_batch(
    candidate_names: list[str],
    registry:        list[InternalAuthor],
    threshold:       float | None = None,
) -> list[MatchResult]:
    threshold = settings.author_match_threshold if threshold is None else threshold
    return [match_author(name, registry, threshold) for name in candidate_names]