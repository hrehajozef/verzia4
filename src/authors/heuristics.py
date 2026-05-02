"""Heuristicky runner pre mena autorov a afiliacie."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

from sqlalchemy.engine import Engine

from src.authors.attribution import AuthorAttribution, _serialize_attribution
from src.authors.parsers.scopus import parse_scopus_affiliation_array
from src.authors.parsers.wos import normalize_text, parse_wos_affiliation
from src.authors.registry import (
    InternalAuthor,
    MatchResult,
    _candidate_signatures,
    lookup_author_affiliations,
    match_author,
)
from src.authors.workplace_tree import (
    WorkplaceNode,
    find_workplace_by_name,
    walk_to_faculty,
)
from src.authors.source_authors import merge_author_lists
from src.common.constants import FlagKey, HeuristicStatus
from src.config.settings import settings

HEURISTIC_VERSION = "4.2.0"

_MIN_STRONG_FUZZY_SCORE = 0.90


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


def _resolve_authors_against_registry(
    author_candidates: list[str],
    registry: list[InternalAuthor],
    *,
    normalize: bool,
    low_confidence_matches: list[dict[str, Any]],
) -> tuple[list[tuple[str, MatchResult]], list[dict[str, Any]]]:
    resolved: list[tuple[str, MatchResult]] = []
    ambiguous_authors: list[dict[str, Any]] = []
    seen_source_authors: set[str] = set()
    seen_identities: set[str] = set()

    for author_str in author_candidates:
        if not author_str:
            continue
        norm_author = normalize_text(author_str)
        if norm_author in seen_source_authors:
            continue
        seen_source_authors.add(norm_author)

        match_obj = _resolve_author_candidate_match(
            author_str,
            registry,
            normalize=normalize,
            hint_faculties=[],
            low_confidence_matches=low_confidence_matches,
            require_surname_match=True,
        )
        if not match_obj:
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
            continue

        identity = _registry_identity(match_obj.author)
        if identity in seen_identities:
            continue
        seen_identities.add(identity)
        resolved.append((author_str, match_obj))

    return resolved, ambiguous_authors


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


def _build_attribution(
    match_obj: MatchResult,
    output_author: str,
    workplace_tree: dict[int, WorkplaceNode] | None,
    remote_engine: Engine | None,
    raw_scopus_affiliations: list[str],
    raw_wos_affiliations: list[str],
) -> AuthorAttribution:
    default_faculty, default_ou = _default_author_affiliation(
        match_obj.author,
        workplace_tree,
        remote_engine,
    )
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
    return attribution


def _cohort_pair_ratio(attributions: list[AuthorAttribution]) -> tuple[tuple[str, str] | None, float]:
    pair_counts: dict[tuple[str, str], int] = {}
    for attribution in attributions:
        if attribution.per_paper_source not in {"scopus_verified", "wos_verified"}:
            continue
        pair = (attribution.per_paper_faculty, attribution.per_paper_ou)
        if not pair[0] or not pair[1]:
            continue
        pair_counts[pair] = pair_counts.get(pair, 0) + 1
    if not pair_counts or not attributions:
        return None, 0.0
    dominant_pair, dominant_count = max(pair_counts.items(), key=lambda item: item[1])
    return dominant_pair, (dominant_count / len(attributions))


def _apply_cohort_inference(attributions: list[AuthorAttribution]) -> tuple[str, str] | None:
    dominant_pair, dominant_ratio = _cohort_pair_ratio(attributions)
    if not dominant_pair or dominant_ratio < 0.8:
        return None
    for attribution in attributions:
        if attribution.per_paper_source:
            continue
        if attribution.default_faculty and attribution.default_faculty == dominant_pair[0]:
            attribution.per_paper_faculty = dominant_pair[0]
            attribution.per_paper_ou = dominant_pair[1]
            attribution.per_paper_source = "cohort_inference"
            attribution.per_paper_confidence = round(0.85 * dominant_ratio, 4)
    return dominant_pair


def _apply_default_fallback(attributions: list[AuthorAttribution]) -> None:
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


def _build_author_flags(
    attributions: list[AuthorAttribution],
    has_wos: bool,
    warnings: list[str],
    unmatched_utb: list[str],
    low_confidence_matches: list[dict[str, Any]],
    ambiguous_authors: list[dict[str, Any]],
    cohort_pair: tuple[str, str] | None,
    cohort_ratio: float,
) -> dict[str, Any]:
    flags_b: dict[str, Any] = {
        FlagKey.MATCHED_UTB_AUTHORS: len([a for a in attributions if a.matched_author]),
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
    if cohort_pair and cohort_ratio >= 0.8:
        flags_b["cohort_inference"] = {
            "faculty": cohort_pair[0],
            "ou": cohort_pair[1],
            "ratio": round(cohort_ratio, 4),
        }
    return flags_b


def _build_result_payload(
    attributions: list[AuthorAttribution],
    *,
    needs_llm: bool,
    has_wos: bool,
    warnings: list[str],
    unmatched_utb: list[str],
    low_confidence_matches: list[dict[str, Any]],
    ambiguous_authors: list[dict[str, Any]],
    cohort_pair: tuple[str, str] | None,
    cohort_ratio: float,
) -> dict[str, Any]:
    return {
        "author_heuristic_status": HeuristicStatus.PROCESSED,
        "author_needs_llm": needs_llm,
        "author_internal_names": _author_output([a.display_name for a in attributions if a.matched_author]),
        "author_faculty": _author_output([a.per_paper_faculty for a in attributions]),
        "author_ou": _author_output([a.per_paper_ou for a in attributions]),
        "author_flags": _build_author_flags(
            attributions,
            has_wos,
            warnings,
            unmatched_utb,
            low_confidence_matches,
            ambiguous_authors,
            cohort_pair,
            cohort_ratio,
        ),
    }


def _parse_author_inputs(
    resource_id: int,
    dc_authors_arr: list[str] | None,
    wos_aff_arr: list[str] | None,
    scopus_aff_arr: list[str] | None,
    wos_author_arr: list[str] | None,
    scopus_author_arr: list[str] | None,
    registry: list[InternalAuthor],
    *,
    normalize: bool,
) -> tuple[list[Any], list[Any], list[str], list[dict[str, Any]], dict[str, str]]:
    scopus_results = parse_scopus_affiliation_array(scopus_aff_arr)
    parsed_wos_results = [
        parse_wos_affiliation(str(raw_aff), resource_id=resource_id)
        for raw_aff in (wos_aff_arr or [])
        if raw_aff
    ]
    scopus_aff_author_names = [
        block.author_name.strip()
        for parsed in scopus_results
        for block in parsed.utb_blocks
        if block.author_name and block.author_name.strip()
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
    return (
        scopus_results,
        parsed_wos_results,
        combined_authors,
        _collect_author_affiliation_texts(scopus_results, parsed_wos_results),
        _preferred_repo_author_names(combined_authors, registry, normalize=normalize),
    )


def _base_result(resource_id: int, combined_authors: list[str]) -> dict[str, Any]:
    return {
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


def _collect_wos_status(parsed_wos_results: list[Any]) -> tuple[list[str], bool]:
    warnings: list[str] = []
    needs_llm = False
    for parsed in parsed_wos_results:
        warnings.extend(parsed.warnings)
        if not parsed.ok:
            needs_llm = True
    return warnings, needs_llm


def _empty_author_flags(
    has_wos: bool,
    warnings: list[str],
    unmatched_utb: list[str],
) -> dict[str, Any]:
    flags_empty: dict[str, Any] = {}
    if not has_wos:
        flags_empty[FlagKey.NO_WOS_DATA] = True
    if unmatched_utb:
        flags_empty[FlagKey.UNMATCHED_UTB_AUTHORS] = unmatched_utb
    if warnings:
        flags_empty[FlagKey.PARSE_WARNINGS] = warnings
    if any("Viac UTB blokov" in warning for warning in warnings):
        flags_empty[FlagKey.MULTIPLE_UTB_BLOCKS] = True
    return flags_empty


def _build_attributions_for_matches(
    resolved_authors: list[tuple[str, MatchResult]],
    preferred_repo_names: dict[str, str],
    author_aff_texts: list[dict[str, Any]],
    workplace_tree: dict[int, WorkplaceNode] | None,
    remote_engine: Engine | None,
) -> list[AuthorAttribution]:
    attributions: list[AuthorAttribution] = []
    for author_str, match_obj in resolved_authors:
        identity = _registry_identity(match_obj.author)
        output_author = preferred_repo_names.get(identity, author_str.strip())
        candidate_affiliations = _author_affiliations_for_candidate(author_str, author_aff_texts)
        attributions.append(_build_attribution(
            match_obj,
            output_author,
            workplace_tree,
            remote_engine,
            candidate_affiliations.get("scopus", []),
            candidate_affiliations.get("wos", []),
        ))
    return attributions


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
    _, parsed_wos_results, combined_authors, author_aff_texts, preferred_repo_names = _parse_author_inputs(
        resource_id,
        dc_authors_arr,
        wos_aff_arr,
        scopus_aff_arr,
        wos_author_arr,
        scopus_author_arr,
        registry,
        normalize=normalize,
    )
    result = _base_result(resource_id, combined_authors)

    try:
        has_wos = bool(wos_aff_arr and any(value for value in wos_aff_arr if value))
        unmatched_utb: list[str] = []
        warnings, needs_llm = _collect_wos_status(parsed_wos_results)
        if not combined_authors:
            result["author_heuristic_status"] = HeuristicStatus.PROCESSED
            result["author_flags"] = _empty_author_flags(has_wos, warnings, unmatched_utb)
            return result

        low_confidence_matches: list[dict] = []
        resolved_authors, ambiguous_authors = _resolve_authors_against_registry(
            combined_authors,
            registry,
            normalize=normalize,
            low_confidence_matches=low_confidence_matches,
        )
        attributions = _build_attributions_for_matches(
            resolved_authors,
            preferred_repo_names,
            author_aff_texts,
            workplace_tree,
            remote_engine,
        )

        cohort_pair, cohort_ratio = _cohort_pair_ratio(attributions)
        _apply_cohort_inference(attributions)
        _apply_default_fallback(attributions)
        needs_llm = needs_llm or bool(ambiguous_authors) or any(
            attribution.per_paper_source == "unresolved"
            for attribution in attributions
        )
        result.update(_build_result_payload(
            attributions,
            needs_llm=needs_llm,
            has_wos=has_wos,
            warnings=warnings,
            unmatched_utb=unmatched_utb,
            low_confidence_matches=low_confidence_matches,
            ambiguous_authors=ambiguous_authors,
            cohort_pair=cohort_pair,
            cohort_ratio=cohort_ratio,
        ))

    except Exception as exc:
        result["author_needs_llm"] = True
        result["author_flags"] = {FlagKey.ERROR: str(exc)}

    return result
