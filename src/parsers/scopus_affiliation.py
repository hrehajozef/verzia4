"""Parser pre stĺpec ``utb.scopus.affiliation``.

Formát vstupu (jeden prvok VARCHAR[] poľa):
  "niekedy oddelenie/ústav1, niekedy fakulta1, Inštitúcia1, prípadne adresa1;
   niekedy oddelenie/ústav2, niekedy fakulta2, Inštitúcia2, prípadne adresa2; ..."

Scopus afiliácia neobsahuje mená autorov – obsahuje iba inštitucionálne záznamy.
Tento parser ich rozseká na časti a identifikuje UTB záznamy.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from src.parsers.wos_affiliation import detect_utb_affiliation, normalize_text


@dataclass
class ScopusAffBlock:
    raw:        str
    parts:      list[str]   # časti oddelené čiarkou: [oddelenie, fakulta, inštitúcia, adresa]
    is_utb:     bool        = False
    keyword:    str | None  = None

    @property
    def institution(self) -> str:
        """Heuristicky určí inštitúciu – zvyčajne predposledná alebo posledná časť."""
        if not self.parts:
            return self.raw
        # Inštitúcia je zvyčajne dlhší text obsahujúci "Univ", "Acad", "Inst"...
        for part in self.parts:
            if re.search(r"\b(univ|acad|inst|college|school)\b", part, re.IGNORECASE):
                return part.strip()
        return self.parts[-1].strip() if self.parts else self.raw

    @property
    def department(self) -> str:
        """Prvá časť je zvyčajne oddelenie/ústav."""
        return self.parts[0].strip() if self.parts else ""

    @property
    def faculty(self) -> str:
        """Druhá časť (ak existuje) je zvyčajne fakulta."""
        return self.parts[1].strip() if len(self.parts) > 1 else ""


@dataclass
class ScopusParseResult:
    raw_text:   str
    blocks:     list[ScopusAffBlock]  = field(default_factory=list)
    utb_blocks: list[ScopusAffBlock]  = field(default_factory=list)

    @property
    def utb_departments(self) -> list[str]:
        return [b.department for b in self.utb_blocks if b.department]

    @property
    def utb_faculties(self) -> list[str]:
        return [b.faculty for b in self.utb_blocks if b.faculty]


def parse_scopus_affiliation(raw_text: str) -> ScopusParseResult:
    """
    Parsuje jeden textový prvok zo stĺpca utb.scopus.affiliation.

    Záznamy sú oddelené bodkočiarkou. Každý záznam obsahuje čiarkami
    oddelené časti: oddelenie, fakulta, inštitúcia, adresa.
    """
    result = ScopusParseResult(raw_text=raw_text)
    if not raw_text or not raw_text.strip():
        return result

    entries = [e.strip() for e in raw_text.split(";") if e.strip()]
    for entry in entries:
        parts   = [p.strip() for p in entry.split(",") if p.strip()]
        is_utb, kw = detect_utb_affiliation(entry)
        block = ScopusAffBlock(raw=entry, parts=parts, is_utb=is_utb, keyword=kw)
        result.blocks.append(block)
        if is_utb:
            result.utb_blocks.append(block)

    return result


def parse_scopus_affiliation_array(
    values: list[str] | None,
) -> list[ScopusParseResult]:
    """Parsuje celé VARCHAR[] pole zo stĺpca utb.scopus.affiliation."""
    if not values:
        return []
    return [parse_scopus_affiliation(item) for item in values if item]