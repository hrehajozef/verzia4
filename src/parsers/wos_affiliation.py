"""Parser pre stĺpec ``utb.wos.affiliation``.

Formát vstupu (jeden prvok VARCHAR[] poľa):
  [Priezvisko1, Meno1; Priezvisko2, Meno2] Inštitúcia, Oddelenie, Adresa;
  [Priezvisko3, Meno3] Iná inštitúcia, Adresa

Každý nájdený UTB blok sa vypíše do konzoly pre audit/debugging.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field

from src.config.settings import settings

_BLOCK_RE    = re.compile(r"\[([^\]]+)\]([^[]*)", re.DOTALL)
_CLEANUP_RE  = re.compile(r"^[;\s]+|[;\s]+$")


# -----------------------------------------------------------------------
# Normalizácia
# -----------------------------------------------------------------------

def _remove_diacritics(value: str) -> str:
    nfd = unicodedata.normalize("NFD", value)
    return "".join(ch for ch in nfd if unicodedata.category(ch) != "Mn")


def normalize_text(value: str) -> str:
    """Lowercase + bez diakritiky + komprimované medzery."""
    return re.sub(r"\s+", " ", _remove_diacritics(value).lower()).strip()


# -----------------------------------------------------------------------
# Detekcia UTB afiliácie
# -----------------------------------------------------------------------

def detect_utb_affiliation(text: str) -> tuple[bool, str | None]:
    """
    Vráti (True, matched_keyword) ak text obsahuje UTB afiliáciu,
    inak (False, None).
    """
    normalized = normalize_text(text)
    for keyword in settings.utb_keywords:
        kw_norm = normalize_text(keyword)
        if kw_norm and kw_norm in normalized:
            return True, keyword
    return False, None


# Alias pre spätnú kompatibilitu
_is_utb_affiliation = detect_utb_affiliation


# -----------------------------------------------------------------------
# Dátové štruktúry
# -----------------------------------------------------------------------

@dataclass
class AffiliationBlock:
    authors_raw:      str
    authors:          list[str]
    affiliation_raw:  str
    is_utb:           bool        = False
    matched_keyword:  str | None  = None


@dataclass
class ParseResult:
    raw_text:   str
    blocks:     list[AffiliationBlock] = field(default_factory=list)
    utb_blocks: list[AffiliationBlock] = field(default_factory=list)
    ok:         bool                   = True
    error:      str | None             = None
    warnings:   list[str]             = field(default_factory=list)

    @property
    def utb_authors(self) -> list[str]:
        seen: set[str] = set()
        result: list[str] = []
        for block in self.utb_blocks:
            for author in block.authors:
                if author not in seen:
                    seen.add(author)
                    result.append(author)
        return result

    @property
    def has_utb_affiliation(self) -> bool:
        return bool(self.utb_blocks)

    @property
    def multiple_utb_blocks(self) -> bool:
        return len(self.utb_blocks) > 1


# -----------------------------------------------------------------------
# Interné pomocné funkcie
# -----------------------------------------------------------------------

def _parse_authors(authors_raw: str) -> list[str]:
    return [a.strip() for a in authors_raw.split(";") if a.strip()]


# -----------------------------------------------------------------------
# Hlavné parsovanie
# -----------------------------------------------------------------------

def parse_wos_affiliation(raw_text: str, resource_id: int | None = None) -> ParseResult:
    """
    Parsuje jeden textový prvok zo stĺpca utb.wos.affiliation.

    Každý blok rozpoznaný ako UTB afiliácia sa vypíše do konzoly.
    resource_id je voliteľný identifikátor záznamu pre audit výpis.
    """
    result = ParseResult(raw_text=raw_text)

    if not raw_text or not raw_text.strip():
        result.ok    = False
        result.error = "Prázdny vstup"
        return result

    matches = list(_BLOCK_RE.finditer(raw_text))

    # Fallback: žiadne [bloky] – celý text je jedna afiliácia
    if not matches:
        result.warnings.append("Nenašli sa bloky [autori] – fallback mód")
        is_utb, keyword = detect_utb_affiliation(raw_text)
        block = AffiliationBlock("", [], raw_text.strip(), is_utb, keyword)
        result.blocks.append(block)
        if is_utb:
            result.utb_blocks.append(block)
        return result

    for m in matches:
        authors_raw     = m.group(1).strip()
        affiliation_raw = _CLEANUP_RE.sub("", m.group(2)).strip()
        authors         = _parse_authors(authors_raw)
        is_utb, keyword = detect_utb_affiliation(affiliation_raw)
        block = AffiliationBlock(authors_raw, authors, affiliation_raw, is_utb, keyword)
        result.blocks.append(block)
        if is_utb:
            result.utb_blocks.append(block)

    if result.multiple_utb_blocks:
        result.warnings.append(f"Viac UTB blokov ({len(result.utb_blocks)})")

    return result


def parse_wos_affiliation_array(
    values: list[str] | None,
    resource_id: int | None = None,
) -> list[ParseResult]:
    """Parsuje celé VARCHAR[] pole zo stĺpca utb.wos.affiliation."""
    if not values:
        return []
    return [parse_wos_affiliation(item, resource_id) for item in values if item]


# -----------------------------------------------------------------------
# Extrakcia oddelenia/ústavu z textu afiliácie
# -----------------------------------------------------------------------

_OU_RE = re.compile(
    r"\b(Dept\.?|Department|Inst\.?|Institute|Ctr\.?|Center|Centre|"
    r"Lab\.?|Laboratory|Grp\.?|Group|Div\.?|Division|Sch\.?|School|"
    r"Unit|Faculty)\b[^,;]{3,60}",
    re.IGNORECASE,
)


def extract_ou_candidates(affiliation_text: str) -> list[str]:
    """
    Vráti všetky kandidátne reťazce oddelení/ústavov nájdené v texte.
    Výsledky sú ďalej spresňované cez DEPT_KEYWORD_MAP v heuristics/runner.py.
    """
    return [m.group(0).strip() for m in _OU_RE.finditer(affiliation_text)]


def extract_ou(affiliation_text: str) -> str:
    """Vráti prvého kandidáta alebo prázdny reťazec (spätná kompatibilita)."""
    candidates = extract_ou_candidates(affiliation_text)
    return candidates[0] if candidates else ""