"""Parser pre stĺpec ``utb.scopus.affiliation``.

Podporované vstupy:
  1. starý formát bez mien:
     "Department ..., Tomas Bata University in Zlin, ...; ..."
  2. nový formát "Authors with affiliations":
     "Belas J., Tomas Bata University in Zlin, ...; Dvorsky J., ..."
  3. nový formát s VIACERÝMI afiliáciami pre jedného autora (Scopus to robí
     aj keď autor patrí pod 2+ inštitúcie – afiliácie sú zlúčené čiarkami,
     bez bodkočiarky medzi nimi):
     "Di Martino A., Research School ..., Tomsk Polytechnic University, ...,
      Russian Federation, Centre of Polymer Systems, ..., Czech Republic;
      Drannikov A., Research School ..., Russian Federation"

Parser je spätne kompatibilný. Pri novom formáte ukladá meno autora
do ``author_name`` a zvyšok textu do ``affiliation``. Ak má autor viac
afiliácií, rozdelí ich na hranici krajiny a emituje pre každú samostatný
``ScopusAffBlock`` (s rovnakým ``author_name``).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from src.authors.parsers.wos import detect_utb_affiliation, normalize_text  # noqa: F401  (re-export)


# Posledný token mena vyzerá ako iniciály: "J", "J.", "J.A.", "P.S.", "T.E."
_AUTHOR_HINT_RE = re.compile(r"\b[A-Z](?:\.?[A-Z])*\.?$")
_INSTITUTION_HINT_RE = re.compile(
    r"\b(univ|university|faculty|department|dept|institute|inst|academy|"
    r"centre|center|college|school|hospital|laborator|laboratory)\b",
    re.IGNORECASE,
)

# Zoznam najčastejších krajín v Scopus afiliáciách. Slúži na rozdelenie
# zlúčených afiliácií jedného autora. Dlhšie názvy sú zoradené ako prvé,
# aby "Russia" nezachytilo skôr ako "Russian Federation".
_COUNTRIES: tuple[str, ...] = tuple(sorted({
    "Russian Federation", "Czech Republic", "United States", "United Kingdom",
    "South Korea", "South Africa", "New Zealand", "Saudi Arabia",
    "United Arab Emirates", "Hong Kong", "Sri Lanka",
    "Slovakia", "Russia", "Germany", "France", "USA", "U.S.A.", "U.K.",
    "Spain", "Italy", "Poland", "Hungary", "Austria", "Slovenia", "Croatia",
    "Romania", "Greece", "Sweden", "Norway", "Finland", "Denmark",
    "Netherlands", "Belgium", "Switzerland", "Portugal", "Ireland",
    "Ukraine", "Belarus", "Bulgaria", "Estonia", "Latvia", "Lithuania",
    "Serbia", "Turkey", "Israel", "Egypt", "Morocco", "Tunisia", "Algeria",
    "Jordan", "Lebanon", "Cyprus", "Iceland", "Luxembourg", "Malta",
    "China", "Japan", "India", "Pakistan", "Iran", "Iraq", "Bangladesh",
    "Brazil", "Argentina", "Chile", "Colombia", "Peru", "Venezuela",
    "Mexico", "Canada", "Australia", "Singapore", "Vietnam", "Thailand",
    "Malaysia", "Indonesia", "Philippines", "Taiwan", "Kazakhstan",
    "Uzbekistan", "Mongolia", "Nigeria", "Kenya", "Ethiopia", "Ghana",
    "North Macedonia", "Bosnia and Herzegovina", "Montenegro", "Albania",
    "Moldova", "Georgia", "Armenia", "Azerbaijan",
}, key=len, reverse=True))

_COUNTRY_BOUNDARY_RE = re.compile(
    r"(?<=[\s,])(" + "|".join(re.escape(c) for c in _COUNTRIES) + r")(?=\s*(?:,|;|$))",
    re.IGNORECASE,
)


@dataclass
class ScopusAffBlock:
    """Jedna (autor, jedna afiliácia) dvojica zo Scopus stĺpca.

    Pri starom formáte ``author_name`` je ``None`` a ``affiliation``
    obsahuje celú vetvu pôvodnej Scopus afiliácie (bez mena).
    """

    raw: str                       # pôvodný `;`-delimited záznam (informácia, nikdy sa neparsuje znova)
    parts: list[str]               # afiliácia rozsekaná po čiarkach (bez mena autora)
    affiliation: str               # text afiliácie tohto bloku (bez mena autora)
    author_name: str | None = None
    is_utb: bool = False
    keyword: str | None = None

    @property
    def institution(self) -> str:
        """Heuristicky určí inštitúciu."""
        if not self.parts:
            return self.affiliation
        for part in self.parts:
            if re.search(r"\b(univ|acad|inst|college|school)\b", part, re.IGNORECASE):
                return part.strip()
        return self.parts[-1].strip() if self.parts else self.affiliation

    @property
    def department(self) -> str:
        return self.parts[0].strip() if self.parts else ""

    @property
    def faculty(self) -> str:
        return self.parts[1].strip() if len(self.parts) > 1 else ""


@dataclass
class ScopusParseResult:
    raw_text: str
    blocks: list[ScopusAffBlock] = field(default_factory=list)
    utb_blocks: list[ScopusAffBlock] = field(default_factory=list)
    has_authors: bool = False  # True ak aspoň 1 záznam mal detegované meno

    @property
    def utb_departments(self) -> list[str]:
        return [b.department for b in self.utb_blocks if b.department]

    @property
    def utb_faculties(self) -> list[str]:
        return [b.faculty for b in self.utb_blocks if b.faculty]

    @property
    def utb_authors(self) -> list[str]:
        """Mená UTB autorov (deduplikované, zachované poradie)."""
        seen: set[str] = set()
        out: list[str] = []
        for b in self.utb_blocks:
            if b.author_name and b.author_name not in seen:
                seen.add(b.author_name)
                out.append(b.author_name)
        return out

    @property
    def all_authors(self) -> list[str]:
        """Všetky mená autorov (UTB aj externých), deduplikované."""
        seen: set[str] = set()
        out: list[str] = []
        for b in self.blocks:
            if b.author_name and b.author_name not in seen:
                seen.add(b.author_name)
                out.append(b.author_name)
        return out


# -----------------------------------------------------------------------
# Detekcia mena autora v zázname
# -----------------------------------------------------------------------

def _split_author_prefix(entry: str) -> tuple[str | None, str]:
    """Rozdelí Scopus záznam na ``(meno_alebo_None, afiliácia_text)``.

    V Scopus formáte je meno autora pred PRVOU čiarkou. Posledný token mena
    musí vyzerať ako iniciály (``[A-Z](\\.?[A-Z])*\\.?``). Ak prvá časť
    obsahuje inštitucionálne kľúčové slovo (univ, faculty, …), ide o starý
    formát bez mien.
    """
    left, sep, right = entry.partition(",")
    if not sep:
        return None, entry.strip()

    candidate = left.strip()
    affiliation = right.strip()
    if not candidate or not affiliation:
        return None, entry.strip()

    # 5+ slov v "mene" je takmer určite afiliácia, nie meno
    tokens = candidate.split()
    if not tokens or len(tokens) > 5:
        return None, entry.strip()

    # Inštitucionálne kľúčové slová → starý formát
    if _INSTITUTION_HINT_RE.search(candidate):
        return None, entry.strip()

    # Posledný token musí vyzerať ako iniciály
    if not _AUTHOR_HINT_RE.fullmatch(tokens[-1]):
        return None, entry.strip()

    return candidate, affiliation


# -----------------------------------------------------------------------
# Rozdelenie viacerých afiliácií jedného autora podľa hranice krajiny
# -----------------------------------------------------------------------

def _split_multi_affiliation(text: str) -> list[str]:
    """Rozseká text afiliácií, ktoré patria jednému autorovi, na samostatné
    afiliácie podľa hranice krajiny (čiarka po názve štátu = nová afiliácia).

    Ak v texte nie je rozpoznaná žiadna krajina alebo iba jedna, vráti
    pôvodný text v jednoprvkovom zozname.
    """
    if not text or not text.strip():
        return []

    matches = list(_COUNTRY_BOUNDARY_RE.finditer(text))
    if len(matches) <= 1:
        return [text.strip()]

    affiliations: list[str] = []
    cursor = 0
    for m in matches:
        end = m.end()
        chunk = text[cursor:end].strip().lstrip(",").strip()
        if chunk:
            affiliations.append(chunk)
        cursor = end

    rest = text[cursor:].strip().lstrip(",").strip()
    if rest:
        affiliations.append(rest)

    return affiliations or [text.strip()]


# -----------------------------------------------------------------------
# Hlavné API
# -----------------------------------------------------------------------

def parse_scopus_affiliation(raw_text: str) -> ScopusParseResult:
    """Parsuje jeden textový prvok zo stĺpca utb.scopus.affiliation.

    Záznamy sú oddelené ``;``. Pre nový formát z každého záznamu vyextrahuje
    meno autora a afiliácie (môže ich byť viac – rozdelí ich podľa hranice
    krajiny). Pre každú (autor, afiliácia) dvojicu vytvorí ``ScopusAffBlock``.
    """
    result = ScopusParseResult(raw_text=raw_text)
    if not raw_text or not raw_text.strip():
        return result

    entries = [entry.strip() for entry in raw_text.split(";") if entry.strip()]
    for entry in entries:
        author_name, affiliation_text = _split_author_prefix(entry)

        if author_name:
            aff_strings = _split_multi_affiliation(affiliation_text)
        else:
            aff_strings = [affiliation_text] if affiliation_text else []

        for aff in aff_strings:
            parts = [p.strip() for p in aff.split(",") if p.strip()]
            is_utb, keyword = detect_utb_affiliation(aff)
            block = ScopusAffBlock(
                raw=entry,
                parts=parts,
                affiliation=aff,
                author_name=author_name,
                is_utb=is_utb,
                keyword=keyword,
            )
            result.blocks.append(block)
            if author_name:
                result.has_authors = True
            if is_utb:
                result.utb_blocks.append(block)

    return result


def parse_scopus_affiliation_array(values: list[str] | None) -> list[ScopusParseResult]:
    """Parsuje celé VARCHAR[] pole zo stĺpca utb.scopus.affiliation."""
    if not values:
        return []
    return [parse_scopus_affiliation(item) for item in values if item]


__all__ = [
    "ScopusAffBlock",
    "ScopusParseResult",
    "parse_scopus_affiliation",
    "parse_scopus_affiliation_array",
    "normalize_text",
]
