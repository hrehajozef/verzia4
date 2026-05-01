"""Heuristicky runner pre mena autorov a afiliacie."""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.authors.parsers.scopus import parse_scopus_affiliation_array
from src.authors.parsers.wos import (
    detect_utb_affiliation,
    extract_ou_candidates,
    normalize_text,
    parse_wos_affiliation,
)
from src.authors.registry import (
    InternalAuthor,
    _candidate_signatures,
    get_author_registry,
    lookup_author_affiliations,
    match_author,
)
from src.authors.workplace_tree import (
    WorkplaceNode,
    find_workplace_by_name,
    load_workplace_tree,
    walk_to_faculty,
)
from src.authors.source_authors import merge_author_lists, split_source_author_lists
from src.common.constants import (
    DEPT_KEYWORD_MAP,
    FACULTIES,
    FACULTY_ENGLISH_TO_ID,
    FACULTY_KEYWORD_RULES,
    FlagKey,
    HeuristicStatus,
    QUEUE_TABLE,
    WOS_ABBREV_NORM,
    _norm,
)
from src.config.settings import settings
from src.db.engines import get_local_engine, get_remote_engine

HEURISTIC_VERSION = "4.2.0"

_DEPT_PREFIXES = ("dept", "ctr ", "inst ", "lab ", "centre", "center", "language", "res ctr")
_MIN_STRONG_FUZZY_SCORE = 0.90


@dataclass
class AuthorAttribution:
    matched_author: InternalAuthor | None
    display_name: str
    per_paper_faculty: str = ""
    per_paper_ou: str = ""
    per_paper_source: str = ""
    per_paper_confidence: float = 0.0
    default_faculty: str = ""
    default_ou: str = ""
    scopus_raw_affiliation: str = ""
    wos_raw_affiliation: str = ""
    flags: dict[str, Any] = field(default_factory=dict)


def _keyword_score(keyword: str) -> int:
    base = len(keyword)
    if any(keyword.startswith(prefix) for prefix in _DEPT_PREFIXES):
        base += 20
    return base


def resolve_faculty_and_ou(affiliation_text: str) -> tuple[str, str]:
    """
    Return (english_faculty_name, english_department_name) from affiliation text.

    Department matches are preferred over faculty-only matches.
    """
    norm = _norm(affiliation_text)
    best_dept, best_fid, best_score = "", "", 0

    for keyword, (dept_name, fid) in {**WOS_ABBREV_NORM, **DEPT_KEYWORD_MAP}.items():
        if keyword in norm:
            score = _keyword_score(keyword)
            if score > best_score:
                best_score, best_dept, best_fid = score, dept_name, fid

    if best_fid:
        return FACULTIES.get(best_fid, best_fid), best_dept

    for keywords, fid in FACULTY_KEYWORD_RULES:
        if any(keyword in norm for keyword in keywords):
            return FACULTIES.get(fid, fid), ""

    return "", ""


def _collect_affiliation_hints(
    scopus_aff_arr: list[str] | None,
    fulltext_aff_arr: list[str] | None,
) -> list[tuple[str, str, str]]:
    """Collect unique (faculty, ou, source) hints from Scopus/fulltext affiliations."""
    hints: list[tuple[str, str, str]] = []

    for parsed in parse_scopus_affiliation_array(scopus_aff_arr):
        for block in parsed.utb_blocks:
            faculty, ou = resolve_faculty_and_ou(block.affiliation)
            if faculty or ou:
                hints.append((faculty, ou, "scopus"))

    for raw_text in fulltext_aff_arr or []:
        if not raw_text:
            continue
        parts = [part.strip() for part in re.split(r"[\r\n;]+", str(raw_text)) if part.strip()]
        for part in parts:
            is_utb, _ = detect_utb_affiliation(part)
            if not is_utb:
                continue
            faculty, ou = resolve_faculty_and_ou(part)
            if faculty or ou:
                hints.append((faculty, ou, "fulltext"))

    return list(dict.fromkeys(hints))


def _collect_author_specific_hints(
    scopus_results: list[Any],
    parsed_wos_results: list[Any],
) -> dict[str, list[tuple[str, str, str]]]:
    """Collect affiliation hints keyed by concrete source author names."""
    hints_by_author: dict[str, list[tuple[str, str, str]]] = {}

    def add_hint(author_name: str, faculty: str, ou: str, source: str) -> None:
        if not author_name or not (faculty or ou):
            return
        key = normalize_text(author_name)
        if not key:
            return
        hints_by_author.setdefault(key, [])
        hint = (faculty, ou, source)
        if hint not in hints_by_author[key]:
            hints_by_author[key].append(hint)

    for parsed in scopus_results:
        for block in parsed.utb_blocks:
            if not block.author_name:
                continue
            faculty, ou = resolve_faculty_and_ou(block.affiliation)
            add_hint(block.author_name, faculty, ou, "scopus")

    for parsed in parsed_wos_results:
        for block in parsed.utb_blocks:
            faculty, ou = resolve_faculty_and_ou(block.affiliation_raw)
            if not ou:
                candidates = extract_ou_candidates(block.affiliation_raw)
                ou = candidates[0] if candidates else ""
            for author_name in block.authors:
                add_hint(author_name, faculty, ou, "wos")

    return hints_by_author


def _author_candidate_hints(
    author_name: str,
    author_specific_hints: dict[str, list[tuple[str, str, str]]],
) -> list[tuple[str, str, str]]:
    return author_specific_hints.get(normalize_text(author_name), [])


def _choose_affiliation_from_hints(
    db_faculties: tuple[str, ...],
    db_ou: str,
    hints: list[tuple[str, str, str]],
) -> tuple[str, str, bool]:
    """Choose the best faculty/OU for one author using DB data and affiliation context."""
    hint_faculties = [faculty for faculty, _, _ in hints if faculty]
    hint_ous = [ou for _, ou, _ in hints if ou]
    unique_hint_faculties = list(dict.fromkeys(hint_faculties))
    unique_hint_ous = list(dict.fromkeys(hint_ous))

    used_hint = False
    faculty = ""
    ou = db_ou or ""

    if len(db_faculties) == 1:
        faculty = db_faculties[0]
    elif len(db_faculties) > 1:
        matching = [value for value in unique_hint_faculties if value in db_faculties]
        if len(matching) == 1:
            faculty = matching[0]
            used_hint = True
        elif len(unique_hint_faculties) == 1:
            faculty = unique_hint_faculties[0]
            used_hint = True
    elif len(unique_hint_faculties) == 1:
        faculty = unique_hint_faculties[0]
        used_hint = True

    if len(unique_hint_ous) == 1:
        ou = unique_hint_ous[0]
        used_hint = True

    if not faculty and ou:
        inferred_faculty_id = DEPT_KEYWORD_MAP.get(_norm(ou), ("", ""))[1]
        if inferred_faculty_id:
            faculty = FACULTIES.get(inferred_faculty_id, "")

    return faculty, ou, used_hint


def _author_output(values: list[str]) -> list[str] | None:
    return values or None


def _registry_identity(author: InternalAuthor) -> str:
    if author.limited_author_id is not None:
        return f"id:{author.limited_author_id}"
    return f"name:{normalize_text(author.canonical_name)}"


def _preferred_repo_author_names(
    repo_authors: list[str] | None,
    registry: list[InternalAuthor],
    *,
    normalize: bool,
) -> dict[str, str]:
    preferred: dict[str, str] = {}
    for author_name in repo_authors or []:
        if not author_name:
            continue
        match = match_author(
            author_name,
            registry,
            settings.author_match_threshold,
            normalize=normalize,
            require_surname_match=True,
        )
        if not (match.matched and match.author):
            continue
        identity = _registry_identity(match.author)
        preferred.setdefault(identity, author_name.strip())
    return preferred


def _resolve_author_candidate_match(
    candidate_name: str,
    registry: list[InternalAuthor],
    *,
    normalize: bool,
    hint_faculties: list[str],
    low_confidence_matches: list[dict[str, Any]],
    require_surname_match: bool,
) -> Any | None:
    match = match_author(
        candidate_name,
        registry,
        settings.author_match_threshold,
        normalize=normalize,
        require_surname_match=require_surname_match,
    )
    if not (match.matched and match.author):
        return None

    if match.match_type == "fuzzy" and match.score < _MIN_STRONG_FUZZY_SCORE:
        low_confidence_matches.append({
            "input": candidate_name,
            "matched": match.author.full_name,
            "score": round(match.score, 4),
            "accepted": False,
        })
        return None

    if (
        match.match_type == "fuzzy"
        and len(hint_faculties) == 1
        and match.author.faculty
        and hint_faculties[0] != match.author.faculty
    ):
        low_confidence_matches.append({
            "input": candidate_name,
            "matched": match.author.full_name,
            "score": round(match.score, 4),
            "accepted": False,
            "reason": "affiliation_conflict",
        })
        return None

    if match.match_type == "fuzzy":
        low_confidence_matches.append({
            "input": candidate_name,
            "matched": match.author.full_name,
            "score": round(match.score, 4),
            "accepted": True,
        })

    return match


def _default_author_affiliation(
    author: InternalAuthor,
    workplace_tree: dict[int, WorkplaceNode] | None,
    remote_engine: Engine | None,
) -> tuple[str, str]:
    default_faculty = (author.faculty or "").strip()
    default_ou = ""

    if workplace_tree and author.organization_id is not None:
        node = workplace_tree.get(int(author.organization_id))
        if node:
            default_ou = node.name_en
            faculty_node = walk_to_faculty(node.id, workplace_tree)
            if not default_faculty and faculty_node:
                default_faculty = faculty_node.name_en

    if not default_faculty or not default_ou:
        db_faculties, db_ou = lookup_author_affiliations(
            author.surname,
            author.firstname,
            remote_engine,
        )
        if not default_faculty and db_faculties:
            default_faculty = db_faculties[0]
        if not default_ou and db_ou:
            default_ou = db_ou

    return default_faculty, default_ou


def _collect_author_affiliation_texts(
    scopus_results: list[Any],
    parsed_wos_results: list[Any],
) -> list[dict[str, Any]]:
    per_author: dict[str, dict[str, Any]] = {}

    def add(source: str, author_name: str, raw_affiliation: str) -> None:
        key = normalize_text(author_name)
        if not key or not raw_affiliation:
            return
        bucket = per_author.setdefault(
            key,
            {
                "author_name": author_name,
                "signatures": set(_candidate_signatures(author_name)),
                "scopus": [],
                "wos": [],
            },
        )
        if raw_affiliation not in bucket[source]:
            bucket[source].append(raw_affiliation)

    for parsed in scopus_results:
        for block in parsed.utb_blocks:
            if block.author_name and block.affiliation:
                add("scopus", block.author_name, block.affiliation)

    for parsed in parsed_wos_results:
        for block in parsed.utb_blocks:
            for author_name in block.authors:
                if author_name and block.affiliation_raw:
                    add("wos", author_name, block.affiliation_raw)

    return list(per_author.values())


def _author_affiliations_for_candidate(
    candidate_name: str,
    author_aff_texts: list[dict[str, Any]],
) -> dict[str, list[str]]:
    candidate_key = normalize_text(candidate_name)
    candidate_signatures = set(_candidate_signatures(candidate_name))
    merged = {"scopus": [], "wos": []}
    for entry in author_aff_texts:
        if entry["author_name"] and normalize_text(entry["author_name"]) == candidate_key:
            for source in ("scopus", "wos"):
                for value in entry[source]:
                    if value not in merged[source]:
                        merged[source].append(value)
            continue
        if candidate_signatures and entry["signatures"] & candidate_signatures:
            for source in ("scopus", "wos"):
                for value in entry[source]:
                    if value not in merged[source]:
                        merged[source].append(value)
    return merged


def _match_workplace_from_affiliations(
    raw_affiliations: list[str],
    workplace_tree: dict[int, WorkplaceNode] | None,
) -> tuple[WorkplaceNode | None, float, str]:
    if not workplace_tree:
        return None, 0.0, ""
    def specificity(node: WorkplaceNode | None) -> tuple[int, int]:
        if node is None:
            return (-1, -1)
        if node.is_department:
            level = 2
        elif node.name_en.startswith("Faculty of ") or node.name_en == "University Institute":
            level = 1
        else:
            level = 0
        return level, len(node.name_en or node.name_cs or "")

    best_node: WorkplaceNode | None = None
    best_score = 0.0
    best_raw = ""
    for raw_affiliation in raw_affiliations:
        candidates = [raw_affiliation]
        candidates.extend(
            fragment.strip()
            for fragment in re.split(r"[,;]", raw_affiliation)
            if fragment and fragment.strip()
        )
        for candidate in candidates:
            node, score = find_workplace_by_name(candidate, workplace_tree)
            if node is None:
                continue
            if score > best_score or (
                score == best_score and specificity(node) > specificity(best_node)
            ):
                best_node = node
                best_score = score
                best_raw = raw_affiliation
    if best_node is not None and specificity(best_node)[0] <= 0:
        return None, 0.0, ""
    return best_node, best_score, best_raw


def _serialize_attribution(attribution: AuthorAttribution) -> dict[str, Any]:
    data = asdict(attribution)
    data["matched_author"] = (
        {
            "author_id": attribution.matched_author.limited_author_id,
            "utbid": attribution.matched_author.utb_id,
            "display_name": attribution.matched_author.display_name,
            "canonical_name": attribution.matched_author.canonical_name,
            "organization_id": attribution.matched_author.organization_id,
            "faculty": attribution.matched_author.faculty,
        }
        if attribution.matched_author
        else None
    )
    return data


def _ambiguity_candidates(candidate_name: str, registry: list[InternalAuthor]) -> list[str]:
    candidate_norm = normalize_text(candidate_name)
    parts = [part for part in candidate_norm.replace(",", " ").split() if part]
    surnames = {parts[0], parts[-1]} if parts else set()
    candidates: list[str] = []
    for author in registry:
        author_names = [normalize_text(value) for value in author.all_names]
        if any(any(surname and surname in name for surname in surnames) for name in author_names):
            candidates.append(author.full_name)
    deduped: list[str] = []
    seen: set[str] = set()
    for value in candidates:
        key = normalize_text(value)
        if key in seen:
            continue
        seen.add(key)
        deduped.append(value)
    return deduped


def process_record(
    resource_id: int,
    wos_aff_arr: list[str] | None,
    dc_authors_arr: list[str] | None,
    registry: list[InternalAuthor],
    normalize: bool = False,
    remote_engine: Engine | None = None,
    workplace_tree: dict[int, WorkplaceNode] | None = None,
    scopus_aff_arr: list[str] | None = None,
    fulltext_aff_arr: list[str] | None = None,
    wos_author_arr: list[str] | None = None,
    scopus_author_arr: list[str] | None = None,
) -> dict:
    scopus_results = parse_scopus_affiliation_array(scopus_aff_arr)
    scopus_aff_author_names = [
        block.author_name.strip()
        for parsed in scopus_results
        for block in parsed.utb_blocks
        if block.author_name and block.author_name.strip()
    ]
    parsed_wos_results = [
        parse_wos_affiliation(str(raw_aff), resource_id=resource_id)
        for raw_aff in (wos_aff_arr or [])
        if raw_aff
    ]
    wos_aff_author_names = [
        author_str.strip()
        for parsed in parsed_wos_results
        for block in parsed.utb_blocks
        for author_str in block.authors
        if author_str and author_str.strip()
    ]

    combined_authors = merge_author_lists(
        dc_authors_arr,
        wos_author_arr,
        scopus_author_arr,
        scopus_aff_author_names,
        wos_aff_author_names,
    )
    author_aff_texts = _collect_author_affiliation_texts(scopus_results, parsed_wos_results)
    preferred_repo_names = _preferred_repo_author_names(combined_authors, registry, normalize=normalize)
    result: dict = {
        "resource_id": resource_id,
        "author_heuristic_status": HeuristicStatus.ERROR,
        "author_heuristic_version": HEURISTIC_VERSION,
        "author_heuristic_processed_at": datetime.now(timezone.utc),
        "author_needs_llm": False,
        "author_dc_names": combined_authors or None,
        "author_internal_names": None,
        "author_faculty": None,
        "author_ou": None,
        "author_flags": {},
    }

    try:
        has_wos = bool(wos_aff_arr and any(value for value in wos_aff_arr if value))

        low_confidence_matches: list[dict] = []
        warnings: list[str] = []
        unmatched_utb: list[str] = []
        needs_llm = False
        ambiguous_authors: list[dict[str, Any]] = []
        attributions_by_identity: dict[str, AuthorAttribution] = {}
        ordered_identities: list[str] = []

        if has_wos:
            seen_wos_authors: set[str] = set()
            for parsed in parsed_wos_results:
                warnings.extend(parsed.warnings)
                if not parsed.ok:
                    needs_llm = True
                for block in parsed.utb_blocks:
                    for author_str in block.authors:
                        norm_author = normalize_text(author_str)
                        if norm_author in seen_wos_authors:
                            continue
                        seen_wos_authors.add(norm_author)

        author_candidates = combined_authors or []
        if not author_candidates:
            result["author_heuristic_status"] = HeuristicStatus.PROCESSED
            flags_empty: dict = {FlagKey.NO_WOS_DATA: True}
            if unmatched_utb:
                flags_empty[FlagKey.UNMATCHED_UTB_AUTHORS] = unmatched_utb
            if warnings:
                flags_empty[FlagKey.PARSE_WARNINGS] = warnings
            if any("Viac UTB blokov" in warning for warning in warnings):
                flags_empty[FlagKey.MULTIPLE_UTB_BLOCKS] = True
            result["author_flags"] = flags_empty
            return result

        seen_source_authors: set[str] = set()
        for author_str in author_candidates:
            if not author_str:
                continue
            norm_author = normalize_text(author_str)
            if norm_author in seen_source_authors:
                continue
            seen_source_authors.add(norm_author)

            resolved = _resolve_author_candidate_match(
                author_str,
                registry,
                normalize=normalize,
                hint_faculties=[],
                low_confidence_matches=low_confidence_matches,
                require_surname_match=True,
            )
            if not resolved:
                probe = match_author(
                    author_str,
                    registry,
                    settings.author_match_threshold,
                    normalize=normalize,
                    require_surname_match=True,
                )
                if probe.match_type in {"ambiguous_surname", "ambiguous_initials", "initial_mismatch"}:
                    ambiguous_authors.append({
                        "input": author_str,
                        "match_type": probe.match_type,
                        "candidates": _ambiguity_candidates(author_str, registry),
                    })
                    needs_llm = True
                continue
            match_obj = resolved
            identity = _registry_identity(match_obj.author)
            if identity in attributions_by_identity:
                continue

            output_author = preferred_repo_names.get(identity, author_str.strip())
            default_faculty, default_ou = _default_author_affiliation(
                match_obj.author,
                workplace_tree,
                remote_engine,
            )
            candidate_affiliations = _author_affiliations_for_candidate(author_str, author_aff_texts)
            raw_scopus_affiliations = candidate_affiliations.get("scopus", [])
            raw_wos_affiliations = candidate_affiliations.get("wos", [])

            attribution = AuthorAttribution(
                matched_author=match_obj.author,
                display_name=output_author,
                default_faculty=default_faculty,
                default_ou=default_ou,
                scopus_raw_affiliation=" || ".join(raw_scopus_affiliations),
                wos_raw_affiliation=" || ".join(raw_wos_affiliations),
            )
            scopus_node, scopus_score, scopus_raw = _match_workplace_from_affiliations(
                raw_scopus_affiliations,
                workplace_tree,
            )
            if scopus_node is not None:
                faculty_node = walk_to_faculty(scopus_node.id, workplace_tree or {})
                attribution.per_paper_ou = scopus_node.name_en
                attribution.per_paper_faculty = faculty_node.name_en if faculty_node else ""
                attribution.per_paper_source = "scopus_verified"
                attribution.per_paper_confidence = scopus_score
                attribution.scopus_raw_affiliation = scopus_raw or attribution.scopus_raw_affiliation
            else:
                wos_node, wos_score, wos_raw = _match_workplace_from_affiliations(
                    raw_wos_affiliations,
                    workplace_tree,
                )
                if wos_node is not None:
                    faculty_node = walk_to_faculty(wos_node.id, workplace_tree or {})
                    attribution.per_paper_ou = wos_node.name_en
                    attribution.per_paper_faculty = faculty_node.name_en if faculty_node else ""
                    attribution.per_paper_source = "wos_verified"
                    attribution.per_paper_confidence = wos_score
                    attribution.wos_raw_affiliation = wos_raw or attribution.wos_raw_affiliation

            if (
                attribution.per_paper_source in {"scopus_verified", "wos_verified"}
                and attribution.per_paper_faculty
                and attribution.default_faculty
                and attribution.per_paper_faculty != attribution.default_faculty
            ):
                attribution.flags["obd_default_conflict"] = {
                    "per_paper": attribution.per_paper_faculty,
                    "default": attribution.default_faculty,
                }

            attributions_by_identity[identity] = attribution
            ordered_identities.append(identity)

        attributions = [attributions_by_identity[identity] for identity in ordered_identities]

        dominant_pair: tuple[str, str] | None = None
        dominant_ratio = 0.0
        pair_counts: dict[tuple[str, str], int] = {}
        for attribution in attributions:
            if attribution.per_paper_source not in {"scopus_verified", "wos_verified"}:
                continue
            pair = (attribution.per_paper_faculty, attribution.per_paper_ou)
            if not pair[0] or not pair[1]:
                continue
            pair_counts[pair] = pair_counts.get(pair, 0) + 1
        if pair_counts and attributions:
            dominant_pair, dominant_count = max(pair_counts.items(), key=lambda item: item[1])
            dominant_ratio = dominant_count / len(attributions)
            if dominant_ratio >= 0.8:
                for attribution in attributions:
                    if attribution.per_paper_source:
                        continue
                    if attribution.default_faculty and attribution.default_faculty == dominant_pair[0]:
                        attribution.per_paper_faculty = dominant_pair[0]
                        attribution.per_paper_ou = dominant_pair[1]
                        attribution.per_paper_source = "cohort_inference"
                        attribution.per_paper_confidence = round(0.85 * dominant_ratio, 4)

        for attribution in attributions:
            if attribution.per_paper_source:
                continue
            if attribution.default_faculty or attribution.default_ou:
                attribution.per_paper_faculty = attribution.default_faculty
                attribution.per_paper_ou = attribution.default_ou
                attribution.per_paper_source = "default_fallback"
                attribution.per_paper_confidence = 0.70
            else:
                attribution.per_paper_source = "unresolved"
                attribution.per_paper_confidence = 0.0
                needs_llm = True

        matched_authors = [attribution.display_name for attribution in attributions]
        matched_faculties = [attribution.per_paper_faculty for attribution in attributions]
        matched_ous = [attribution.per_paper_ou for attribution in attributions]

        flags_b: dict = {
            FlagKey.MATCHED_UTB_AUTHORS: len(matched_authors),
        }
        if not has_wos:
            flags_b[FlagKey.NO_WOS_DATA] = True
        if unmatched_utb:
            flags_b[FlagKey.UNMATCHED_UTB_AUTHORS] = unmatched_utb
        if warnings:
            flags_b[FlagKey.PARSE_WARNINGS] = warnings
        if any("Viac UTB blokov" in warning for warning in warnings):
            flags_b[FlagKey.MULTIPLE_UTB_BLOCKS] = True
        if low_confidence_matches:
            flags_b[FlagKey.PATH_B_LOW_CONFIDENCE] = low_confidence_matches
        if ambiguous_authors:
            flags_b["ambiguous_authors"] = ambiguous_authors
        flags_b["attributions"] = [_serialize_attribution(attribution) for attribution in attributions]
        if dominant_pair and dominant_ratio >= 0.8:
            flags_b["cohort_inference"] = {
                "faculty": dominant_pair[0],
                "ou": dominant_pair[1],
                "ratio": round(dominant_ratio, 4),
            }

        result.update({
            "author_heuristic_status": HeuristicStatus.PROCESSED,
            "author_needs_llm": needs_llm,
            "author_internal_names": _author_output(matched_authors),
            "author_faculty": _author_output(matched_faculties),
            "author_ou": _author_output(matched_ous),
            "author_flags": flags_b,
        })

    except Exception as exc:
        result["author_needs_llm"] = True
        result["author_flags"] = {FlagKey.ERROR: str(exc)}

    return result


def process_batch(
    rows: list,
    registry: list[InternalAuthor],
    normalize: bool = False,
    remote_engine: Engine | None = None,
    source_author_map: dict[int, dict[str, list[str]]] | None = None,
) -> list[dict]:
    workplace_tree = load_workplace_tree(remote_engine=remote_engine)
    return [
        process_record(
            resource_id=row.resource_id,
            wos_aff_arr=row.wos_aff,
            dc_authors_arr=row.dc_authors,
            registry=registry,
            normalize=normalize,
            remote_engine=remote_engine,
            workplace_tree=workplace_tree,
            scopus_aff_arr=row.scopus_aff,
            fulltext_aff_arr=row.fulltext_aff,
            wos_author_arr=(source_author_map or {}).get(row.resource_id, {}).get("wos"),
            scopus_author_arr=(source_author_map or {}).get(row.resource_id, {}).get("scopus"),
        )
        for row in rows
    ]


def _build_source_author_map(
    engine: Engine,
    rows: list,
) -> dict[int, dict[str, list[str]]]:
    if not rows:
        return {}

    schema = settings.local_schema
    record_ids = [int(row.resource_id) for row in rows]
    current_rows = {
        int(row.resource_id): {
            "authors": list(row.dc_authors) if row.dc_authors else [],
            "sources": list(row.source_arr) if getattr(row, "source_arr", None) else [],
        }
        for row in rows
    }

    history_map: dict[int, list[dict[str, Any]]] = {rid: [] for rid in record_ids}
    with engine.connect() as conn:
        try:
            history_rows = conn.execute(text(f"""
                SELECT
                    dedup_kept_resource_id,
                    "utb.source" AS source_arr,
                    "dc.contributor.author" AS authors_arr
                FROM "{schema}"."dedup_histoire"
                WHERE dedup_kept_resource_id = ANY(:ids)
            """), {"ids": record_ids}).fetchall()
        except Exception:
            history_rows = []

    for row in history_rows:
        kept_id = int(row.dedup_kept_resource_id)
        history_map.setdefault(kept_id, []).append({
            "sources": row.source_arr,
            "authors": row.authors_arr,
        })

    return {
        rid: split_source_author_lists(
            current_authors=current_rows[rid]["authors"],
            current_sources=current_rows[rid]["sources"],
            history_rows=history_map.get(rid, []),
        )
        for rid in record_ids
    }


def run_heuristics(
    engine: Engine | None = None,
    remote_engine: Engine | None = None,
    batch_size: int | None = None,
    limit: int = 0,
    reprocess_errors: bool = False,
    reprocess: bool = False,
    normalize: bool = False,
) -> None:
    engine = engine or get_local_engine()
    remote_engine = remote_engine or get_remote_engine()
    batch_size = batch_size or settings.heuristics_batch_size
    schema = settings.local_schema
    table = settings.local_table
    queue = QUEUE_TABLE
    statuses = [HeuristicStatus.NOT_PROCESSED]
    if reprocess_errors:
        statuses.append(HeuristicStatus.ERROR)
    if reprocess:
        statuses.append(HeuristicStatus.PROCESSED)

    registry = get_author_registry(remote_engine=remote_engine)
    print(f"[INFO] Nacitanych internych autorov z remote DB: {len(registry)}")

    with engine.connect() as conn:
        id_rows = conn.execute(
            text(f"""
                SELECT q.resource_id
                FROM "{schema}"."{queue}" q
                JOIN "{schema}"."{table}" m ON q.resource_id = m.resource_id
                WHERE q.author_heuristic_status = ANY(:s)
                ORDER BY q.resource_id
            """),
            {"s": statuses},
        ).fetchall()

    all_ids: list[int] = [int(row[0]) for row in id_rows]
    total = len(all_ids)

    if limit > 0:
        all_ids = all_ids[:limit]
        total = len(all_ids)
    if total == 0:
        print("[INFO] Ziadne zaznamy na spracovanie.")
        return

    print(f"[INFO] Zaznamov na spracovanie: {total}")

    processed = 0
    while processed < total:
        batch_ids = all_ids[processed: processed + batch_size]

        with engine.connect() as conn:
            rows = conn.execute(
                text(f"""
                    SELECT m.resource_id,
                           m."utb.wos.affiliation" AS wos_aff,
                           m."utb.scopus.affiliation" AS scopus_aff,
                           m."utb.fulltext.affiliation" AS fulltext_aff,
                           m."dc.contributor.author" AS dc_authors,
                           m."utb.source" AS source_arr
                    FROM "{schema}"."{table}" m
                    JOIN "{schema}"."{queue}" q ON m.resource_id = q.resource_id
                    WHERE q.resource_id = ANY(:ids)
                    ORDER BY m.resource_id
                """),
                {"ids": batch_ids},
            ).fetchall()

        if not rows:
            processed += len(batch_ids)
            print(f"  [WARN] Davka bez platnych zaznamov: {batch_ids}")
            continue

        source_author_map = _build_source_author_map(engine, rows)
        updates = process_batch(
            rows,
            registry,
            normalize=normalize,
            remote_engine=remote_engine,
            source_author_map=source_author_map,
        )

        update_sql = f"""
            UPDATE "{schema}"."{queue}"
            SET
                author_flags = %s::jsonb ||
                    CASE
                        WHEN author_flags ? 'duplicates'
                        THEN jsonb_build_object('duplicates', author_flags->'duplicates')
                        ELSE '{{}}'::jsonb
                    END,
                author_heuristic_status = %s,
                author_heuristic_version = %s,
                author_heuristic_processed_at = %s,
                author_needs_llm = %s,
                author_dc_names = %s,
                author_internal_names = %s,
                author_faculty = %s,
                author_ou = %s
            WHERE resource_id = %s
        """
        params = [
            (
                json.dumps(update["author_flags"], ensure_ascii=False),
                update["author_heuristic_status"],
                update["author_heuristic_version"],
                update["author_heuristic_processed_at"],
                update["author_needs_llm"],
                update["author_dc_names"],
                update["author_internal_names"],
                update["author_faculty"],
                update["author_ou"],
                update["resource_id"],
            )
            for update in updates
        ]

        raw = engine.raw_connection()
        try:
            with raw.cursor() as cur:
                cur.executemany(update_sql, params)
            raw.commit()
        finally:
            raw.close()

        processed += len(batch_ids)
        print(f"  Spracovane: {processed}/{total}")

    print(f"[OK] Heuristiky autorov hotove. Spracovanych: {processed}")


def _norm_name_set(names: list[str] | None) -> set[str]:
    from src.authors.registry import _normalize_name

    if not names:
        return set()
    return {_normalize_name(name) for name in names if name and name.strip()}


def compare_with_librarian(engine: Engine | None = None) -> None:
    """
    Compare author_internal_names (program) vs utb.contributor.internalauthor (librarian).
    """
    engine = engine or get_local_engine()
    schema = settings.local_schema
    table = settings.local_table
    queue = QUEUE_TABLE

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT q.resource_id,
                   q.author_internal_names AS prog,
                   m."utb.contributor.internalauthor" AS lib
            FROM "{schema}"."{queue}" q
            JOIN "{schema}"."{table}" m ON q.resource_id = m.resource_id
            WHERE q.author_heuristic_status = 'processed'
        """)).fetchall()

    cats: dict[str, int] = {
        "exact": 0,
        "partial": 0,
        "no_overlap": 0,
        "only_prog": 0,
        "only_lib": 0,
        "both_empty": 0,
    }
    total = len(rows)

    for row in rows:
        prog = _norm_name_set(row.prog)
        lib = _norm_name_set(row.lib)

        if not prog and not lib:
            cats["both_empty"] += 1
        elif prog and not lib:
            cats["only_prog"] += 1
        elif lib and not prog:
            cats["only_lib"] += 1
        elif prog == lib:
            cats["exact"] += 1
        elif prog & lib:
            cats["partial"] += 1
        else:
            cats["no_overlap"] += 1

    matched = cats["exact"] + cats["partial"]
    print(f"Spracovanych zaznamov (heuristic_status=processed): {total}")
    print()
    print(f"  Presna zhoda (exact):          {cats['exact']:>6}  ({100*cats['exact']/total:.1f}%)" if total else "")
    print(f"  Ciastocna zhoda (partial):     {cats['partial']:>6}  ({100*cats['partial']/total:.1f}%)" if total else "")
    print(f"  Bez prieniku (no_overlap):     {cats['no_overlap']:>6}  ({100*cats['no_overlap']/total:.1f}%)" if total else "")
    print(f"  Len program (only_prog):       {cats['only_prog']:>6}  ({100*cats['only_prog']/total:.1f}%)" if total else "")
    print(f"  Len knihovnik (only_lib):      {cats['only_lib']:>6}  ({100*cats['only_lib']/total:.1f}%)" if total else "")
    print(f"  Oba prazdne (both_empty):      {cats['both_empty']:>6}  ({100*cats['both_empty']/total:.1f}%)" if total else "")
    print()
    if total:
        print(f"  Celkom zhodnych (exact+partial): {matched} / {total}  ({100*matched/total:.1f}%)")
