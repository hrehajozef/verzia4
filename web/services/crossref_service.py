"""Load and format metadata from the Crossref works API."""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import quote

import httpx

_JATS_TAG_RE = re.compile(r"</?jats:[^>]*>", re.IGNORECASE)


def _strip_jats(text: str) -> str:
    """Remove JATS XML tags (e.g. <jats:p>, <jats:italic>) from abstract text."""
    cleaned = _JATS_TAG_RE.sub("", text)
    return re.sub(r" {2,}", " ", cleaned).strip()

_CROSSREF_WORKS_URL = "https://api.crossref.org/works/{doi}"
_CROSSREF_CITATION_URL = "https://citation.doi.org/metadata?doi={doi}"
_TIMEOUT = 10.0
_HEADERS = {"User-Agent": "UTB-Metadata-Pipeline/1.0", "Accept": "application/json"}

# Human labels for the optional "extra" Crossref section.
CROSSREF_FIELD_LABELS: dict[str, str] = {
    "title": "Názov",
    "author": "Autori",
    "container-title": "Journal",
    "short-container-title": "Skrátený journal",
    "publisher": "Vydavateľ",
    "volume": "Volume",
    "issue": "Issue",
    "page": "Strany",
    "ISSN": "ISSN",
    "issn-type": "ISSN typ",
    "ISBN": "ISBN",
    "DOI": "DOI",
    "URL": "URL",
    "type": "Typ",
    "subject": "Predmet",
    "abstract": "Abstrakt",
    "reference-count": "Počet referencií",
    "is-referenced-by-count": "Počet citácií",
    "language": "Jazyk",
    "license": "Licencia",
    "funder": "Financovanie",
    "link": "Fulltext linky",
}

# Simple 1:1 mappings from detail row keys to Crossref keys.
MAIN_TO_CROSSREF: dict[str, str] = {
    "dc.title": "title",
    "dc.contributor.author": "author",
    "dc.relation.ispartof": "container-title",
    "dc.publisher": "publisher",
    "utb.relation.volume": "volume",
    "utb.relation.issue": "issue",
    "dc.type": "type",
    "dc.identifier.issn": "ISSN",
    "dc.identifier.isbn": "ISBN",
    "dc.identifier.doi": "DOI",
    "dc.identifier.uri": "URL",
    "dc.description.abstract": "abstract",
    "dc.language.iso": "language",
}

# Date rows need explicit fallback rules.
DATE_FIELD_FALLBACKS: dict[str, tuple[str, ...]] = {
    "dc.date.issued": ("published", "issued", "published-online", "published-print"),
    "utb_date_published_online": ("published-online",),
    "utb_date_published": ("published-print",),
}

# These Crossref keys are already represented inline in the main table.
_MAPPED_CF_KEYS: set[str] = (
    set(MAIN_TO_CROSSREF.values())
    | set(key for keys in DATE_FIELD_FALLBACKS.values() for key in keys)
    | {"page"}
)

# Crossref technical timestamps are not relevant for librarian review.
_IGNORED_CF_KEYS: set[str] = {"created", "deposited", "indexed"}


def _format_authors(authors: list[dict[str, Any]]) -> str:
    parts: list[str] = []
    for author in authors:
        name = ", ".join(filter(None, [author.get("family"), author.get("given")]))
        if not name:
            name = author.get("name", "")
        if name:
            parts.append(name)
    return "||".join(parts)


def _format_date(date_obj: dict[str, Any]) -> str | None:
    parts = date_obj.get("date-parts", [[]])[0]
    if not parts:
        return None
    return "-".join(str(part).zfill(2) for part in parts if part)


def _format_string_list(values: list[Any]) -> str | None:
    joined = "||".join(str(value).strip() for value in values if value and str(value).strip())
    return joined or None


def _format_funders(funders: list[dict[str, Any]]) -> str | None:
    parts: list[str] = []
    for funder in funders:
        name = str(funder.get("name") or "").strip()
        awards = funder.get("award") or []
        award_text = _format_string_list(awards) if isinstance(awards, list) else None
        if name and award_text:
            parts.append(f"{name} ({award_text})")
        elif name:
            parts.append(name)
    return "||".join(parts) if parts else None


def _format_url_entries(entries: list[dict[str, Any]]) -> str | None:
    urls: list[str] = []
    for entry in entries:
        url = str(entry.get("URL") or "").strip()
        if url:
            urls.append(url)
    return "||".join(urls) if urls else None


def _format_issn_type(entries: list[dict[str, Any]]) -> str | None:
    parts: list[str] = []
    for entry in entries:
        value = str(entry.get("value") or "").strip()
        typ = str(entry.get("type") or "").strip()
        if value and typ:
            parts.append(f"{value} ({typ})")
        elif value:
            parts.append(value)
    return "||".join(parts) if parts else None


def _format_value(key: str, value: Any) -> str | None:
    if value is None:
        return None
    if key == "author" and isinstance(value, list):
        return _format_authors(value)
    if key == "funder" and isinstance(value, list):
        return _format_funders(value)
    if key in ("license", "link") and isinstance(value, list):
        return _format_url_entries(value)
    if key == "issn-type" and isinstance(value, list):
        return _format_issn_type(value)
    if isinstance(value, dict) and "date-parts" in value:
        return _format_date(value)
    if isinstance(value, list):
        if all(not isinstance(item, dict) for item in value):
            return _format_string_list(value)
        return None
    if isinstance(value, dict):
        return None
    text_value = str(value).strip()
    if key == "abstract":
        text_value = _strip_jats(text_value)
    return text_value or None


def _first_formatted(data: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        value = _format_value(key, data.get(key))
        if value:
            return value
    return None


def _build_by_field(data: dict[str, Any]) -> dict[str, str]:
    by_field: dict[str, str] = {}

    for main_key, crossref_key in MAIN_TO_CROSSREF.items():
        value = _format_value(crossref_key, data.get(crossref_key))
        if value:
            by_field[main_key] = value

    for main_key, fallback_keys in DATE_FIELD_FALLBACKS.items():
        value = _first_formatted(data, fallback_keys)
        if value:
            by_field[main_key] = value

    page_value = data.get("page")
    if page_value and isinstance(page_value, str):
        page_str = page_value.strip()
        if "-" in page_str:
            spage, epage = [part.strip() for part in page_str.split("-", 1)]
            if spage:
                by_field["dc.citation.spage"] = spage
            if epage:
                by_field["dc.citation.epage"] = epage
        elif page_str:
            by_field["dc.citation.spage"] = page_str

    return by_field


def _build_extra(data: dict[str, Any]) -> list[dict[str, str]]:
    extra: list[dict[str, str]] = []
    for crossref_key, label in CROSSREF_FIELD_LABELS.items():
        if crossref_key in _MAPPED_CF_KEYS or crossref_key in _IGNORED_CF_KEYS:
            continue
        value = _format_value(crossref_key, data.get(crossref_key))
        if value:
            extra.append({"label": label, "value": value})
    return extra


def fetch_crossref(doi: str) -> dict[str, Any]:
    """
    Fetch Crossref metadata for a DOI.

    Returns:
      - ok: bool
      - by_field: {detail_row_key: formatted_value}
      - extra: [{label, value}]
      - error: str | None
    """
    doi = doi.strip()
    for prefix in ("https://doi.org/", "http://doi.org/", "https://dx.doi.org/", "doi:"):
        if doi.startswith(prefix):
            doi = doi[len(prefix):]
            break

    try:
        response = httpx.get(
            _CROSSREF_WORKS_URL.format(doi=quote(doi, safe="")),
            headers=_HEADERS,
            timeout=_TIMEOUT,
            follow_redirects=True,
        )
        response.raise_for_status()
        payload = response.json()
    except httpx.HTTPStatusError as exc:
        code = exc.response.status_code
        message = f"DOI nenajdeny: {doi}" if code == 404 else f"HTTP {code}"
        return {"ok": False, "by_field": {}, "extra": [], "error": message}
    except Exception as exc:  # pragma: no cover - defensive network handling
        return {"ok": False, "by_field": {}, "extra": [], "error": str(exc)}

    if isinstance(payload, dict) and isinstance(payload.get("message"), dict):
        data = payload["message"]
    else:
        data = payload
        try:
            response = httpx.get(
                _CROSSREF_CITATION_URL.format(doi=doi),
                headers=_HEADERS,
                timeout=_TIMEOUT,
                follow_redirects=True,
            )
            response.raise_for_status()
            fallback_payload = response.json()
            if isinstance(fallback_payload, dict):
                data = fallback_payload
        except Exception:
            pass

    return {
        "ok": True,
        "by_field": _build_by_field(data),
        "extra": _build_extra(data),
        "error": None,
    }
