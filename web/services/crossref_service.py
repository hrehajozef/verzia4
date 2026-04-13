"""Načítanie metadát z Crossref API podľa DOI."""

from __future__ import annotations

from typing import Any

import httpx

_CROSSREF_URL = "https://citation.doi.org/metadata?doi={doi}"
_TIMEOUT = 10.0
_HEADERS = {"User-Agent": "UTB-Metadata-Pipeline/1.0", "Accept": "application/json"}

# Mapovanie Crossref kľúčov na labely (pre "extra" sekciu)
CROSSREF_FIELD_LABELS: dict[str, str] = {
    "title":                  "Názov",
    "author":                 "Autori",
    "published":              "Dátum vydania",
    "container-title":        "Journal",
    "publisher":              "Vydavateľ",
    "volume":                 "Volume",
    "issue":                  "Issue",
    "page":                   "Strany",
    "ISSN":                   "ISSN",
    "ISBN":                   "ISBN",
    "DOI":                    "DOI",
    "URL":                    "URL",
    "type":                   "Typ",
    "subject":                "Predmet",
    "abstract":               "Abstrakt",
    "reference-count":        "Počet citácií",
    "is-referenced-by-count": "Citovaný",
    "language":               "Jazyk",
    "license":                "Licencia",
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


def _format_value(key: str, val: Any) -> str | None:
    if val is None:
        return None
    if key == "author" and isinstance(val, list):
        return _format_authors(val)
    if key in ("published", "published-print", "published-online") and isinstance(val, dict):
        return _format_date(val)
    if isinstance(val, list):
        joined = " || ".join(str(v) for v in val if v)
        return joined if joined else None
    if isinstance(val, dict):
        return None  # preskočíme vnorené objekty
    s = str(val).strip()
    return s if s else None


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
            _CROSSREF_URL.format(doi=doi),
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
    by_field: dict[str, str] = {}
    for main_key, cf_key in MAIN_TO_CROSSREF.items():
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
