"""Helpers for source-aware author merging (WoS / Scopus / repository)."""

from __future__ import annotations

import re
import unicodedata
from typing import Any, Iterable


def _normalize_name(name: str) -> str:
    if not name:
        return ""
    nfd = unicodedata.normalize("NFD", str(name))
    no_acc = "".join(ch for ch in nfd if unicodedata.category(ch) != "Mn")
    return re.sub(r"\s+", " ", no_acc.lower()).strip()


def _name_words(name: str) -> list[str]:
    return re.findall(r"[a-z]+", _normalize_name(name))


def _author_signature(name: str) -> tuple[str, str] | None:
    if not name:
        return None

    raw = str(name).strip()
    if not raw:
        return None

    if "," in raw:
        left, right = raw.split(",", 1)
        surname_words = _name_words(left)
        given_words = _name_words(right)
        if surname_words:
            return " ".join(surname_words), "".join(word[0] for word in given_words)

    words = _name_words(raw)
    if not words:
        return None
    if len(words) == 1:
        return words[0], ""
    if len(words[-1]) == 1 and len(words[0]) > 1:
        return words[0], "".join(word[0] for word in words[1:])
    if len(words[0]) == 1 and len(words[-1]) > 1:
        return words[-1], "".join(word[0] for word in words[:-1])
    return words[-1], "".join(word[0] for word in words[:-1])


def _author_key(name: str) -> str:
    signature = _author_signature(name)
    if signature:
        surname, initials = signature
        if surname:
            return f"{surname}|{initials}"
    return _normalize_name(name)


def _author_quality(name: str) -> tuple[int, int, int, int]:
    raw = str(name or "").strip()
    words = _name_words(raw)
    signature = _author_signature(raw)
    initials = signature[1] if signature else ""
    non_initial_words = sum(1 for word in words if len(word) > 1)
    non_ascii = sum(1 for ch in raw if ord(ch) > 127)
    return (
        1 if "," in raw else 0,
        non_initial_words,
        non_ascii,
        len(raw),
    )


def _to_list(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return [str(item).strip() for item in value if item is not None and str(item).strip()]
    text_value = str(value).strip()
    if not text_value:
        return []
    return [part.strip() for part in re.split(r"\s*\|\|\s*", text_value) if part.strip()]


def merge_author_lists(*author_lists: Iterable[str] | None) -> list[str]:
    """Merge source author lists while preferring the richer name variant."""
    order: list[str] = []
    best_by_key: dict[str, str] = {}

    for author_list in author_lists:
        for author in _to_list(author_list):
            key = _author_key(author)
            if not key:
                continue
            if key not in best_by_key:
                best_by_key[key] = author
                order.append(key)
                continue
            if _author_quality(author) > _author_quality(best_by_key[key]):
                best_by_key[key] = author

    return [best_by_key[key] for key in order]


def source_flags(source_arr: Iterable[str] | None) -> tuple[bool, bool]:
    values = [str(item).lower() for item in _to_list(source_arr)]
    has_wos = any(("wok" in value) or ("wos" in value) for value in values)
    has_scopus = any("scopus" in value for value in values)
    return has_wos, has_scopus


def split_source_author_lists(
    current_authors: Iterable[str] | None,
    current_sources: Iterable[str] | None,
    history_rows: Iterable[dict[str, Any]] | None = None,
) -> dict[str, list[str]]:
    """
    Build source-specific author lists from the kept record and dedup history rows.

    Returns {"repo": [...], "wos": [...], "scopus": [...]}.
    """
    wos_authors: list[str] = []
    scopus_authors: list[str] = []
    current_list = _to_list(current_authors)
    history_found = False

    for row in history_rows or []:
        history_found = True
        authors = _to_list(row.get("authors"))
        has_wos, has_scopus = source_flags(row.get("sources"))
        if has_wos:
            wos_authors = merge_author_lists(wos_authors, authors)
        if has_scopus:
            scopus_authors = merge_author_lists(scopus_authors, authors)

    if not history_found:
        has_wos, has_scopus = source_flags(current_sources)
        if has_wos and not has_scopus:
            wos_authors = merge_author_lists(wos_authors, current_list)
        elif has_scopus and not has_wos:
            scopus_authors = merge_author_lists(scopus_authors, current_list)

    repo_authors = merge_author_lists(wos_authors, scopus_authors, current_list)
    return {
        "repo": repo_authors,
        "wos": wos_authors,
        "scopus": scopus_authors,
    }
