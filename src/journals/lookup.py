"""
API klienti pre vyhľadávanie kanonických hodnôt publisher a title (relation.ispartof).

Hierarchia pre ISSN:
  1. Crossref  – https://api.crossref.org/journals/{issn}
  2. OpenAlex  – https://api.openalex.org/sources?filter=issn:{issn}

Hierarchia pre ISBN:
  1. Google Books  – https://www.googleapis.com/books/v1/volumes?q=isbn:{isbn}
  2. OpenLibrary   – https://openlibrary.org/api/books?bibkeys=ISBN:{isbn}&format=json&jscmd=data
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from urllib.parse import quote

import httpx


_TIMEOUT    = 12
_RATE_DELAY = 0.25   # polite delay medzi requestmi na rovnaké API


@dataclass
class LookupResult:
    publisher: str | None
    title:     str | None   # → dc.relation.ispartof
    source:    str          # 'crossref' | 'openalex' | 'google_books' | 'openlibrary'


# ═══════════════════════════════════════════════════════════════════════
# Interné HTTP
# ═══════════════════════════════════════════════════════════════════════

def _get(url: str, *, params: dict | None = None, headers: dict | None = None) -> dict | None:
    """GET s timeoutom; vráti parsovaný JSON alebo None pri chybe."""
    try:
        with httpx.Client(timeout=_TIMEOUT) as client:
            r = client.get(url, params=params, headers=headers or {})
            r.raise_for_status()
            return r.json()
    except Exception:
        return None


def _clean(s: str | None) -> str | None:
    """Trim + vráti None ak prázdny."""
    if not s:
        return None
    v = str(s).strip()
    return v or None


# ═══════════════════════════════════════════════════════════════════════
# ISSN
# ═══════════════════════════════════════════════════════════════════════

def _crossref_issn(issn: str) -> LookupResult | None:
    data = _get(
        f"https://api.crossref.org/journals/{issn}",
        headers={"User-Agent": "UTBMetadataPipeline/1.0 (mailto:library@utb.cz)"},
    )
    if not data:
        return None
    msg   = data.get("message", {})
    pub   = _clean(msg.get("publisher"))
    title = _clean(msg.get("title"))
    if pub or title:
        return LookupResult(publisher=pub, title=title, source="crossref")
    return None


def _openalex_issn(issn: str) -> LookupResult | None:
    data = _get(
        "https://api.openalex.org/sources",
        params={"filter": f"issn:{issn}", "per-page": "1"},
    )
    if not data:
        return None
    results = data.get("results") or []
    if not results:
        return None
    src   = results[0]
    pub   = _clean(src.get("publisher"))
    title = _clean(src.get("display_name"))
    if title:
        return LookupResult(publisher=pub, title=title, source="openalex")
    return None


def lookup_by_issn(issn: str) -> LookupResult | None:
    """Crossref → OpenAlex fallback. issn vo formáte '0001-1541'."""
    result = _crossref_issn(issn)
    if result:
        return result
    time.sleep(_RATE_DELAY)
    return _openalex_issn(issn)


def lookup_by_doi(doi: str) -> LookupResult | None:
    """Crossref Works lookup pre konkretne DOI; vracia publisher a container-title."""
    clean = (doi or "").strip()
    for prefix in ("https://doi.org/", "http://doi.org/", "https://dx.doi.org/", "doi:"):
        if clean.lower().startswith(prefix):
            clean = clean[len(prefix):]
            break
    if not clean:
        return None

    data = _get(
        f"https://api.crossref.org/works/{quote(clean, safe='')}",
        headers={"User-Agent": "UTBMetadataPipeline/1.0 (mailto:library@utb.cz)"},
    )
    if not data:
        return None

    msg = data.get("message", {})
    pub = _clean(msg.get("publisher"))
    titles = msg.get("container-title") or msg.get("short-container-title") or []
    title = _clean(titles[0] if isinstance(titles, list) and titles else titles)
    if pub or title:
        return LookupResult(publisher=pub, title=title, source="crossref_work")
    return None


# ═══════════════════════════════════════════════════════════════════════
# ISBN
# ═══════════════════════════════════════════════════════════════════════

def _google_books(isbn: str) -> LookupResult | None:
    data = _get(
        "https://www.googleapis.com/books/v1/volumes",
        params={"q": f"isbn:{isbn}"},
    )
    if not data or data.get("totalItems", 0) == 0:
        return None
    items = data.get("items") or []
    if not items:
        return None
    vi    = items[0].get("volumeInfo", {})
    pub   = _clean(vi.get("publisher"))
    title = _clean(vi.get("title"))
    if title:
        return LookupResult(publisher=pub, title=title, source="google_books")
    return None


def _openlibrary(isbn: str) -> LookupResult | None:
    data = _get(
        "https://openlibrary.org/api/books",
        params={"bibkeys": f"ISBN:{isbn}", "format": "json", "jscmd": "data"},
    )
    if not data:
        return None
    book = data.get(f"ISBN:{isbn}")
    if not book:
        return None
    publishers = book.get("publishers") or []
    pub   = _clean(publishers[0].get("name") if publishers else None)
    title = _clean(book.get("title"))
    if title:
        return LookupResult(publisher=pub, title=title, source="openlibrary")
    return None


def lookup_by_isbn(isbn: str) -> LookupResult | None:
    """Google Books → OpenLibrary fallback. Akceptuje ISBN s pomlčkami aj bez."""
    clean = isbn.replace("-", "").replace(" ", "")
    result = _google_books(clean)
    if result:
        return result
    time.sleep(_RATE_DELAY)
    return _openlibrary(clean)
