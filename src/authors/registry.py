"""Matching internych autorov UTB proti remote registru autorov a pracovisk."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from typing import TYPE_CHECKING

import jellyfish
from sqlalchemy import text

from src.common.constants import (
    CZECH_DEPARTMENT_MAP_NORM,
    CZECH_FACULTY_MAP_NORM,
    FACULTIES,
    FACULTY_ENGLISH_TO_ID,
    IGNORED_OU_NAMES_NORM,
)
from src.config.settings import settings
from src.db.engines import get_remote_engine

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class InternalAuthor:
    surname: str
    firstname: str
    middle_name: str = ""
    aliases: tuple[str, ...] = ()
    limited_author_id: int | None = None
    utb_id: str | None = None
    display_name: str = ""
    scopus_id: str | None = None
    wos_id: str | None = None
    orcid: str | None = None
    obd_id: str | None = None
    organization_id: int | None = None
    faculty: str | None = None

    @property
    def canonical_name(self) -> str:
        given_parts = [self.firstname, self.middle_name]
        given = " ".join(part.strip() for part in given_parts if part and part.strip())
        return f"{self.surname}, {given}" if given else self.surname

    @property
    def full_name(self) -> str:
        if self.display_name.strip():
            return self.display_name.strip()
        return self.aliases[0] if self.aliases else self.canonical_name

    @property
    def full_name_reversed(self) -> str:
        if "," in self.full_name:
            left, right = self.full_name.split(",", 1)
            return f"{right.strip()} {left.strip()}".strip()
        given_parts = [self.firstname, self.middle_name]
        given = " ".join(part.strip() for part in given_parts if part and part.strip())
        return f"{given} {self.surname}".strip()

    @property
    def all_names(self) -> tuple[str, ...]:
        values: list[str] = [self.full_name, self.canonical_name, self.full_name_reversed]
        for alias in self.aliases:
            values.append(alias)
            if "," in alias:
                left, right = alias.split(",", 1)
                values.append(f"{right.strip()} {left.strip()}".strip())
        deduped: list[str] = []
        seen: set[str] = set()
        for value in values:
            if not value:
                continue
            key = _match_norm(value)
            if key in seen:
                continue
            seen.add(key)
            deduped.append(value)
        return tuple(deduped)


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
    nfd = unicodedata.normalize("NFD", name)
    no_acc = "".join(ch for ch in nfd if unicodedata.category(ch) != "Mn")
    return re.sub(r"\s+", " ", no_acc.lower()).strip()


def _match_norm(name: str) -> str:
    normalized = _normalize_name(name)
    normalized = re.sub(r"[^\w\s]", " ", normalized, flags=re.UNICODE)
    return re.sub(r"\s+", " ", normalized).strip()


def _name_words(name: str) -> list[str]:
    return re.findall(r"[a-z]+", _match_norm(name))


def _author_signature(author: InternalAuthor) -> tuple[str, str]:
    surname = _match_norm(author.surname)
    initials = "".join(word[0] for word in _name_words(author.firstname))
    return surname, initials


def _candidate_signatures(name: str) -> list[tuple[str, str]]:
    """Return possible (surname, initials) interpretations for a candidate name."""
    if not name:
        return []

    signatures: list[tuple[str, str]] = []
    if "," in name:
        left, right = name.split(",", 1)
        surname_words = _name_words(left)
        given_words = _name_words(right)
        if surname_words:
            initials = "".join(w[0] for w in given_words)
            surname_candidates = [" ".join(surname_words)]
            if len(surname_words) > 1:
                surname_candidates.extend([surname_words[0], surname_words[-1]])
            for surname in surname_candidates:
                signatures.append((surname, initials))
    else:
        words = _name_words(name)
        if words:
            if len(words) == 1:
                signatures.append((words[0], ""))
            else:
                signatures.append((words[-1], "".join(w[0] for w in words[:-1])))
                signatures.append((words[0], "".join(w[0] for w in words[1:])))

    return list(dict.fromkeys(signatures))


def _author_signatures(author: InternalAuthor) -> list[tuple[str, str]]:
    signatures: list[tuple[str, str]] = []
    for value in author.all_names:
        signatures.extend(_candidate_signatures(value))
    canonical = _author_signature(author)
    if canonical[0]:
        signatures.append(canonical)
    return list(dict.fromkeys(signatures))


def _author_surnames(author: InternalAuthor) -> set[str]:
    return {surname for surname, _ in _author_signatures(author) if surname}


def _shares_candidate_initials(candidate_name: str, author: InternalAuthor) -> bool:
    candidate_signatures = [
        (surname, initials)
        for surname, initials in _candidate_signatures(candidate_name)
        if surname and initials
    ]
    if not candidate_signatures:
        return True
    for candidate_surname, candidate_initials in candidate_signatures:
        for author_surname, author_initials in _author_signatures(author):
            if candidate_surname != author_surname:
                continue
            if not author_initials:
                return True
            if (
                author_initials.startswith(candidate_initials)
                or candidate_initials.startswith(author_initials)
            ):
                return True
    return False


def _candidate_surnames(name: str) -> set[str]:
    return {surname for surname, _ in _candidate_signatures(name) if surname}


def _find_unique_initial_match(
    candidate_name: str,
    registry: list[InternalAuthor],
) -> InternalAuthor | None:
    signatures = [
        (surname, initials)
        for surname, initials in _candidate_signatures(candidate_name)
        if surname and initials
    ]
    if not signatures:
        return None

    matches: list[InternalAuthor] = []
    for author in registry:
        author_signatures = [
            (author_surname, author_initials)
            for author_surname, author_initials in _author_signatures(author)
            if author_surname and author_initials
        ]
        if not author_signatures:
            continue
        for candidate_surname, candidate_initials in signatures:
            if any(
                candidate_surname == author_surname
                and (
                    author_initials.startswith(candidate_initials)
                    or candidate_initials.startswith(author_initials)
                )
                for author_surname, author_initials in author_signatures
            ):
                matches.append(author)
                break

    unique = list(dict.fromkeys(matches))
    return unique[0] if len(unique) == 1 else None


def _has_ambiguous_initial_match(
    candidate_name: str,
    registry: list[InternalAuthor],
) -> bool:
    signatures = [
        (surname, initials)
        for surname, initials in _candidate_signatures(candidate_name)
        if surname and initials
    ]
    if not signatures:
        return False

    matches: list[InternalAuthor] = []
    for author in registry:
        author_signatures = [
            (author_surname, author_initials)
            for author_surname, author_initials in _author_signatures(author)
            if author_surname and author_initials
        ]
        if not author_signatures:
            continue
        for candidate_surname, candidate_initials in signatures:
            if any(
                candidate_surname == author_surname
                and (
                    author_initials.startswith(candidate_initials)
                    or candidate_initials.startswith(author_initials)
                )
                for author_surname, author_initials in author_signatures
            ):
                matches.append(author)
                break

    return len(list(dict.fromkeys(matches))) > 1


def _author_match_variants(author: InternalAuthor, *, normalize: bool) -> list[str]:
    values = list(author.all_names)
    if normalize:
        return list(dict.fromkeys(_match_norm(v) for v in values if v))
    return list(dict.fromkeys(v for v in values if v))


_AUTHOR_REGISTRY: list[InternalAuthor] = []
_REMOTE_SCHEMA = settings.remote_schema
_AFFILIATION_CACHE: dict[tuple[str, str], tuple[tuple[str, ...], str]] = {}
_AUTHORS_TABLE = "utb_authors"


def _utb_tree_cte(schema: str) -> str:
    return f"""
        WITH RECURSIVE utb_tree AS (
            SELECT id, id_nadrizene, nazev
            FROM "{schema}".obd_prac
            WHERE nazev IN (
                'Univerzita Tomáše Bati ve Zlíně',
                'Univerzita Tomase Bati ve Zline',
                'Tomas Bata University in Zlin'
            )
            UNION ALL
            SELECT child.id, child.id_nadrizene, child.nazev
            FROM "{schema}".obd_prac AS child
            JOIN utb_tree AS parent ON child.id_nadrizene = parent.id
        )
    """


def _translate_faculty(name: str) -> str:
    normalized = _match_norm(name)
    faculty_id = CZECH_FACULTY_MAP_NORM.get(normalized) or FACULTY_ENGLISH_TO_ID.get(name)
    return FACULTIES.get(faculty_id, "") if faculty_id else ""


def _translate_ou(*names: str) -> str:
    for name in names:
        normalized = _match_norm(name)
        if not normalized or normalized in IGNORED_OU_NAMES_NORM:
            continue
        translated = CZECH_DEPARTMENT_MAP_NORM.get(normalized)
        if translated:
            return translated
    return ""


def clear_author_registry_cache() -> None:
    _AUTHOR_REGISTRY.clear()


def _split_alias_values(*raw_values: object) -> tuple[str, ...]:
    values: list[str] = []
    seen: set[str] = set()
    for raw in raw_values:
        if raw is None:
            continue
        parts = str(raw).split("||")
        for part in parts:
            value = part.strip()
            if not value:
                continue
            key = _match_norm(value)
            if key in seen:
                continue
            seen.add(key)
            values.append(value)
    return tuple(values)


def _parse_limited_author(row) -> InternalAuthor | None:
    aliases = _split_alias_values(
        getattr(row, "display_name", None),
        getattr(row, "other_name", None),
    )
    surname = (row.surname or "").strip()
    firstname = (row.given_name or "").strip()
    middle_name = (getattr(row, "middle_name", None) or "").strip()
    display_name = aliases[0] if aliases else str(getattr(row, "display_name", "") or "").strip()
    author_id = int(row.author_id) if getattr(row, "author_id", None) is not None else None
    organization_id = (
        int(getattr(row, "organization_id"))
        if getattr(row, "organization_id", None) is not None
        else None
    )
    wos_id = (
        str(getattr(row, "researcherid", "") or "").strip()
        or str(getattr(row, "wos_id", "") or "").strip()
        or None
    )
    scopus_id = str(getattr(row, "scopusid", "") or "").strip() or None
    utb_id = str(getattr(row, "utbid", "") or "").strip() or None
    orcid = str(getattr(row, "orcid", "") or "").strip() or None
    obd_id = str(getattr(row, "obd_id", "") or "").strip() or None
    faculty = str(getattr(row, "faculty", "") or "").strip() or None

    if surname:
        return InternalAuthor(
            surname=surname,
            firstname=firstname,
            middle_name=middle_name,
            aliases=aliases,
            limited_author_id=author_id,
            utb_id=utb_id,
            display_name=display_name,
            scopus_id=scopus_id,
            wos_id=wos_id,
            orcid=orcid,
            obd_id=obd_id,
            organization_id=organization_id,
            faculty=faculty,
        )

    primary = aliases[0] if aliases else ""
    if not primary:
        return None
    if "," in primary:
        left, right = primary.split(",", 1)
        surname = left.strip()
        firstname = right.strip()
        if surname:
            return InternalAuthor(
                surname=surname,
                firstname=firstname,
                middle_name=middle_name,
                aliases=aliases,
                limited_author_id=author_id,
                utb_id=utb_id,
                display_name=display_name,
                scopus_id=scopus_id,
                wos_id=wos_id,
                orcid=orcid,
                obd_id=obd_id,
                organization_id=organization_id,
                faculty=faculty,
            )
    return InternalAuthor(
        surname=primary,
        firstname="",
        middle_name=middle_name,
        aliases=aliases,
        limited_author_id=author_id,
        utb_id=utb_id,
        display_name=display_name,
        scopus_id=scopus_id,
        wos_id=wos_id,
        orcid=orcid,
        obd_id=obd_id,
        organization_id=organization_id,
        faculty=faculty,
    )


def get_author_registry(remote_engine: Engine | None = None) -> list[InternalAuthor]:
    """Return cached internal authors loaded from remote utb_authors."""
    if _AUTHOR_REGISTRY:
        return _AUTHOR_REGISTRY

    schema = settings.remote_schema
    sql = text(f"""
        SELECT
            poradie,
            author_id,
            utbid,
            display_name,
            surname,
            given_name,
            middle_name,
            other_name,
            scopusid,
            researcherid,
            wos_id,
            orcid,
            obd_id,
            organization_id,
            faculty
        FROM "{schema}"."{_AUTHORS_TABLE}"
        WHERE COALESCE(utb, '') ILIKE 'ano'
          AND (
              COALESCE(surname, '') <> ''
              OR COALESCE(display_name, '') <> ''
          )
        ORDER BY surname, given_name
    """)

    with (remote_engine or get_remote_engine()).connect() as conn:
        rows = conn.execute(sql).fetchall()

    seen: set[tuple[int | None, str, str]] = set()
    for row in rows:
        author = _parse_limited_author(row)
        if not author:
            continue
        key = (
            author.limited_author_id,
            _match_norm(author.surname),
            _match_norm(author.firstname),
            _match_norm(author.middle_name),
        )
        if key in seen:
            continue
        seen.add(key)
        _AUTHOR_REGISTRY.append(author)
    return _AUTHOR_REGISTRY


def _is_faculty_level(name: str) -> bool:
    normalized = _match_norm(name)
    return "fakult" in normalized or "faculty" in normalized or normalized == "univerzitni institut"


def _is_top_level(name: str) -> bool:
    normalized = _match_norm(name)
    return any(keyword in normalized for keyword in ("universit", "univerzit", "rektora", "rektorat"))


def _is_real_ou(name: str) -> bool:
    normalized = _match_norm(name)
    return (
        bool(name)
        and normalized not in IGNORED_OU_NAMES_NORM
        and not _is_faculty_level(name)
        and not _is_top_level(name)
    )


def _extract_person_data(
    workplaces: list[tuple[str, str, str]],
) -> tuple[tuple[str, ...], str]:
    """Determine English faculties and the best English OU from one author's workplaces."""
    faculties: set[str] = set()
    ou_candidates: list[str] = []

    for workplace_name, parent_name, grandparent_name in workplaces:
        workplace_faculty = _translate_faculty(workplace_name) if _is_faculty_level(workplace_name) else ""
        parent_faculty = _translate_faculty(parent_name) if parent_name and _is_faculty_level(parent_name) else ""
        grandparent_faculty = _translate_faculty(grandparent_name) if grandparent_name and _is_faculty_level(grandparent_name) else ""

        for faculty in (workplace_faculty, parent_faculty, grandparent_faculty):
            if faculty:
                faculties.add(faculty)

        if not _is_real_ou(workplace_name) and not _is_real_ou(parent_name):
            continue

        nearest_faculty = parent_faculty or grandparent_faculty or workplace_faculty
        translated_ou = _translate_ou(workplace_name, parent_name)
        if translated_ou:
            if nearest_faculty:
                faculties.add(nearest_faculty)
            ou_candidates.append(translated_ou)

    best_ou = max(ou_candidates, key=len) if ou_candidates else ""
    return tuple(sorted(faculties)), best_ou


def clear_affiliation_cache() -> None:
    _AFFILIATION_CACHE.clear()


def lookup_author_affiliations(
    surname: str,
    firstname: str,
    remote_engine: Engine | None = None,
) -> tuple[tuple[str, ...], str]:
    """Look up English faculties and OU for a specific author inside the UTB subtree."""
    key = (surname, firstname)
    if key in _AFFILIATION_CACHE:
        return _AFFILIATION_CACHE[key]

    engine = remote_engine or get_remote_engine()
    schema = _REMOTE_SCHEMA
    sql = text(_utb_tree_cte(schema) + f"""
        SELECT
            o.nazev AS workplace_name,
            COALESCE(p.nazev, '') AS parent_name,
            COALESCE(gp.nazev, '') AS grandparent_name
        FROM utb_tree AS utb
        JOIN "{schema}".obd_prac AS o ON o.id = utb.id
        JOIN "{schema}".obd_lideprac AS ol ON o.id = ol.idprac
        JOIN "{schema}".s_lide AS l ON ol.idlide = l.id
        LEFT JOIN "{schema}".obd_prac AS p ON o.id_nadrizene = p.id
        LEFT JOIN "{schema}".obd_prac AS gp ON p.id_nadrizene = gp.id
        WHERE l.prijmeni = :surname
          AND l.jmeno = :firstname
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql, {"surname": surname, "firstname": firstname}).fetchall()

    if not rows:
        result: tuple[tuple[str, ...], str] = ((), "")
    else:
        workplaces = list({
            (row.workplace_name or "", row.parent_name or "", row.grandparent_name or "")
            for row in rows
        })
        result = _extract_person_data(workplaces)

    _AFFILIATION_CACHE[key] = result
    return result


def _extract_surname_norm(name: str) -> str:
    """Extract and normalize surname from a name string."""
    if "," in name:
        return _match_norm(name.split(",")[0].strip())
    parts = name.strip().split()
    return _match_norm(parts[-1]) if parts else _match_norm(name)


_ORCID_RE = re.compile(r"\b\d{4}-\d{4}-\d{4}-\d{3}[0-9X]\b", flags=re.IGNORECASE)
_SCOPUS_ID_RE = re.compile(r"\b\d{8,11}\b")
_WOS_TOKEN_RE = re.compile(r"\b[A-Z0-9][A-Z0-9-]{5,}\b", flags=re.IGNORECASE)


def _match_external_id(candidate_name: str, registry: list[InternalAuthor]) -> MatchResult | None:
    orcid_match = _ORCID_RE.search(candidate_name or "")
    if orcid_match:
        target = orcid_match.group(0).upper()
        for author in registry:
            if (author.orcid or "").upper() == target:
                return MatchResult(candidate_name, True, author, 1.0, "orcid")

    scopus_ids = set(_SCOPUS_ID_RE.findall(candidate_name or ""))
    if scopus_ids:
        for author in registry:
            if author.scopus_id and author.scopus_id in scopus_ids:
                return MatchResult(candidate_name, True, author, 1.0, "scopus_id")

    wos_tokens = {token.upper() for token in _WOS_TOKEN_RE.findall(candidate_name or "")}
    if wos_tokens:
        for author in registry:
            if author.wos_id and author.wos_id.upper() in wos_tokens:
                return MatchResult(candidate_name, True, author, 1.0, "wos_id")

    return None


def match_author(
    candidate_name: str,
    registry: list[InternalAuthor],
    threshold: float = 0.85,
    normalize: bool = False,
    require_surname_match: bool = False,
) -> MatchResult:
    """
    Match a candidate name against the internal author registry.

    normalize=False:
      1. exact match with diacritics
      2. fuzzy Jaro-Winkler on raw names

    normalize=True:
      1. exact match with diacritics
      2. exact normalized match
      3. fuzzy Jaro-Winkler on normalized names

    require_surname_match=True:
      Apply surname filtering before fuzzy matching.
    """
    if not candidate_name or not candidate_name.strip():
        return MatchResult(input_name=candidate_name, matched=False)

    id_match = _match_external_id(candidate_name, registry)
    if id_match is not None:
        return id_match

    for author in registry:
        if author.full_name == candidate_name or author.full_name_reversed == candidate_name:
            return MatchResult(candidate_name, True, author, 1.0, "exact_diacritic")

    norm_candidate = _match_norm(candidate_name)
    for author in registry:
        if norm_candidate in _author_match_variants(author, normalize=True):
            return MatchResult(candidate_name, True, author, 1.0, "exact_normalized")

    initial_author = _find_unique_initial_match(candidate_name, registry)
    if initial_author:
        return MatchResult(candidate_name, True, initial_author, 0.98, "initial_surname")
    if _has_ambiguous_initial_match(candidate_name, registry):
        return MatchResult(candidate_name, False, None, 0.0, "ambiguous_initials")

    candidate_surnames = _candidate_surnames(candidate_name)
    candidate_has_initials = any(initials for _, initials in _candidate_signatures(candidate_name))
    if candidate_surnames and not candidate_has_initials:
        surname_matches = [
            author for author in registry
            if _author_surnames(author) & candidate_surnames
        ]
        if len(surname_matches) > 1:
            return MatchResult(candidate_name, False, None, 0.0, "ambiguous_surname")

    if require_surname_match:
        fuzzy_pool = [
            author for author in registry
            if _author_surnames(author) & candidate_surnames
        ]
    elif candidate_surnames:
        fuzzy_pool = [
            author for author in registry
            if _author_surnames(author) & candidate_surnames
        ] or registry
    else:
        fuzzy_pool = registry

    if not fuzzy_pool:
        return MatchResult(candidate_name, False, None, 0.0, "none")

    if candidate_has_initials:
        initial_pool = [author for author in fuzzy_pool if _shares_candidate_initials(candidate_name, author)]
        if not initial_pool:
            return MatchResult(candidate_name, False, None, 0.0, "initial_mismatch")
        fuzzy_pool = initial_pool

    if normalize:
        best_score = 0.0
        best_author: InternalAuthor | None = None
        seen_norms: set[str] = set()
        for author in fuzzy_pool:
            for norm_variant in _author_match_variants(author, normalize=True):
                if norm_variant in seen_norms:
                    continue
                seen_norms.add(norm_variant)
                score = jellyfish.jaro_winkler_similarity(norm_candidate, norm_variant)
                if score > best_score:
                    best_score = score
                    best_author = author

        if best_author and best_score >= threshold:
            return MatchResult(candidate_name, True, best_author, best_score, "fuzzy")
        return MatchResult(candidate_name, False, None, best_score, "none")

    best_score = 0.0
    best_author = None
    seen_names: set[str] = set()
    for author in fuzzy_pool:
        for raw_variant in _author_match_variants(author, normalize=False):
            if raw_variant in seen_names:
                continue
            seen_names.add(raw_variant)
            score = jellyfish.jaro_winkler_similarity(candidate_name, raw_variant)
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
    normalize: bool = False,
) -> list[MatchResult]:
    threshold = settings.author_match_threshold if threshold is None else threshold
    return [
        match_author(name, registry, threshold, normalize=normalize)
        for name in candidate_names
    ]
