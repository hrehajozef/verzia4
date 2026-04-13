"""Načítanie metadát z Crossref API podľa DOI."""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

import httpx

_CROSSREF_WORKS_URL = "https://api.crossref.org/works/{doi}"
_CROSSREF_CITATION_URL = "https://citation.doi.org/metadata?doi={doi}"
_TIMEOUT = 10.0
_HEADERS = {"User-Agent": "UTB-Metadata-Pipeline/1.0", "Accept": "application/json"}

# Mapovanie Crossref kľúčov na labely (pre "extra" sekciu)
CROSSREF_FIELD_LABELS: dict[str, str] = {
    "title":                  "Názov",
    "author":                 "Autori",
    "published":              "Dátum vydania",
    "published-print":        "Dátum vydania tlačou",
    "published-online":       "Dátum vydania online",
    "issued":                 "Dátum vydania",
    "created":                "Vytvorené v Crossref",
    "deposited":              "Aktualizované v Crossref",
    "container-title":        "Journal",
    "short-container-title":  "Skrátený journal",
    "publisher":              "Vydavateľ",
    "volume":                 "Volume",
    "issue":                  "Issue",
    "page":                   "Strany",
    "ISSN":                   "ISSN",
    "issn-type":              "ISSN typ",
    "ISBN":                   "ISBN",
    "DOI":                    "DOI",
    "URL":                    "URL",
    "type":                   "Typ",
    "subject":                "Predmet",
    "abstract":               "Abstrakt",
    "reference-count":        "Počet referencií",
    "is-referenced-by-count": "Počet citovaní",
    "language":               "Jazyk",
    "license":                "Licencia",
    "funder":                 "Financovanie",
    "link":                   "Fulltext linky",
}

# Mapovanie: hlavný field kľúč tabuľky → Crossref kľúč
# Crossref hodnota sa zobrazí v rovnakom riadku ako zodpovedajúci field
MAIN_TO_CROSSREF: dict[str, str] = {
    "dc.title":                 "title",
    "dc.contributor.author":    "author",
    "dc.date.issued":           "published",
    "dc.relation.ispartof":     "container-title",
    "dc.publisher":             "publisher",
    "utb.relation.volume":      "volume",
    "utb.relation.issue":       "issue",
    "dc.type":                  "type",
    "dc.identifier.issn":       "ISSN",
    "dc.identifier.isbn":       "ISBN",
    "dc.identifier.doi":        "DOI",
    "dc.identifier.uri":        "URL",
    "dc.description.abstract":  "abstract",
    "dc.language.iso":          "language",
}

# Crossref kľúče, ktoré sú namapované na hlavné fieldy (zvyšok pôjde do "extra")
# "page" je tu tiež – spracúva sa osobitne na dc.citation.spage / dc.citation.epage
_MAPPED_CF_KEYS: set[str] = set(MAIN_TO_CROSSREF.values()) | {"page"}


def _format_authors(authors: list[dict]) -> str:
    parts = []
    for a in authors:
        name = ", ".join(filter(None, [a.get("family"), a.get("given")]))
        if not name:
            name = a.get("name", "")
        if name:
            parts.append(name)
    return " || ".join(parts)


def _format_date(date_obj: dict) -> str | None:
    parts = date_obj.get("date-parts", [[]])[0]
    if not parts:
        return None
    return "-".join(str(p).zfill(2) for p in parts if p)


def _format_string_list(val: list) -> str | None:
    joined = " || ".join(str(v).strip() for v in val if v and str(v).strip())
    return joined or None


def _format_funders(funders: list[dict]) -> str | None:
    parts = []
    for funder in funders:
        name = str(funder.get("name") or "").strip()
        awards = funder.get("award") or []
        award_text = _format_string_list(awards) if isinstance(awards, list) else None
        if name and award_text:
            parts.append(f"{name} ({award_text})")
        elif name:
            parts.append(name)
    return " || ".join(parts) if parts else None


def _format_url_entries(entries: list[dict]) -> str | None:
    urls = []
    for entry in entries:
        url = str(entry.get("URL") or "").strip()
        if url:
            urls.append(url)
    return " || ".join(urls) if urls else None


def _format_issn_type(entries: list[dict]) -> str | None:
    parts = []
    for entry in entries:
        value = str(entry.get("value") or "").strip()
        typ = str(entry.get("type") or "").strip()
        if value and typ:
            parts.append(f"{value} ({typ})")
        elif value:
            parts.append(value)
    return " || ".join(parts) if parts else None


def _format_value(key: str, val: Any) -> str | None:
    if val is None:
        return None
    if key == "author" and isinstance(val, list):
        return _format_authors(val)
    if key == "funder" and isinstance(val, list):
        return _format_funders(val)
    if key in ("license", "link") and isinstance(val, list):
        return _format_url_entries(val)
    if key == "issn-type" and isinstance(val, list):
        return _format_issn_type(val)
    if isinstance(val, dict) and "date-parts" in val:
        return _format_date(val)
    if isinstance(val, list):
        if all(not isinstance(v, dict) for v in val):
            return _format_string_list(val)
        return None
    if isinstance(val, dict):
        return None  # preskočíme vnorené objekty
    s = str(val).strip()
    return s if s else None


def _first_formatted(data: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        val = _format_value(key, data.get(key))
        if val:
            return val
    return None


def fetch_crossref(doi: str) -> dict[str, Any]:
    """
    Načíta metadáta z Crossref API pre daný DOI.

    Vráti dict:
      - ok:       bool
      - by_field: {main_field_key: formatted_value}  – pre inline zobrazenie v tabuľke
      - extra:    [{label, value}]                    – ostatné Crossref polia bez mapovania
      - error:    str | None
    """
    doi = doi.strip()
    for prefix in ("https://doi.org/", "http://doi.org/", "https://dx.doi.org/", "doi:"):
        if doi.startswith(prefix):
            doi = doi[len(prefix):]
            break

    try:
        resp = httpx.get(
            _CROSSREF_WORKS_URL.format(doi=quote(doi, safe="")),
            headers=_HEADERS,
            timeout=_TIMEOUT,
            follow_redirects=True,
        )
        resp.raise_for_status()
        data = resp.json()
    except httpx.HTTPStatusError as exc:
        code = exc.response.status_code
        msg = f"DOI nenájdený: {doi}" if code == 404 else f"HTTP {code}"
        return {"ok": False, "by_field": {}, "extra": [], "error": msg}
    except Exception as exc:
        return {"ok": False, "by_field": {}, "extra": [], "error": str(exc)}

    # 1. Inline hodnoty (namapované na riadky tabuľky)
    if isinstance(data, dict) and isinstance(data.get("message"), dict):
        data = data["message"]
    else:
        try:
            resp = httpx.get(
                _CROSSREF_CITATION_URL.format(doi=doi),
                headers=_HEADERS,
                timeout=_TIMEOUT,
                follow_redirects=True,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception:
            pass

    by_field: dict[str, str] = {}
    for main_key, cf_key in MAIN_TO_CROSSREF.items():
        if main_key == "dc.date.issued":
            val = _first_formatted(data, ("published-print", "published-online", "published", "issued"))
        else:
            val = _format_value(cf_key, data.get(cf_key))
        if val:
            by_field[main_key] = val

    # Špeciálne: page → dc.citation.spage + dc.citation.epage
    page_val = data.get("page")
    if page_val and isinstance(page_val, str):
        page_str = page_val.strip()
        if "-" in page_str:
            parts = page_str.split("-", 1)
            spage = parts[0].strip()
            epage = parts[1].strip()
            if spage:
                by_field["dc.citation.spage"] = spage
            if epage:
                by_field["dc.citation.epage"] = epage
        elif page_str:
            by_field["dc.citation.spage"] = page_str

    # 2. Extra hodnoty (nemajú zodpovedajúci riadok)
    extra: list[dict] = []
    for cf_key, label in CROSSREF_FIELD_LABELS.items():
        if cf_key in _MAPPED_CF_KEYS:
            continue
        val = _format_value(cf_key, data.get(cf_key))
        if val:
            extra.append({"label": label, "value": val})

    return {"ok": True, "by_field": by_field, "extra": extra, "error": None}
