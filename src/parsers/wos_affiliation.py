"""Parser a pomocné funkcie pre stĺpec ``utb.wos.affiliation``."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field

from src.config.settings import settings

_BLOCK_PATTERN = re.compile(r"\[([^\]]+)\]([^[]*)", re.DOTALL)
_CLEANUP_PATTERN = re.compile(r"^[;\s]+|[;\s]+$")


def _remove_diacritics(value: str) -> str:
    normalized = unicodedata.normalize("NFD", value)
    return "".join(ch for ch in normalized if unicodedata.category(ch) != "Mn")


def normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", _remove_diacritics(value).lower()).strip()


def detect_utb_affiliation(affiliation_text: str) -> tuple[bool, str | None]:
    normalized = normalize_text(affiliation_text)
    for keyword in settings.utb_keywords:
        normalized_keyword = normalize_text(keyword)
        if normalized_keyword and normalized_keyword in normalized:
            return True, keyword
    return False, None


# Spätná kompatibilita pre staré testy/importy
_is_utb_affiliation = detect_utb_affiliation


@dataclass
class AffiliationBlock:
    authors_raw: str
    authors: list[str]
    affiliation_raw: str
    is_utb: bool = False
    matched_keyword: str | None = None


@dataclass
class ParseResult:
    raw_text: str
    blocks: list[AffiliationBlock] = field(default_factory=list)
    utb_blocks: list[AffiliationBlock] = field(default_factory=list)
    ok: bool = True
    error: str | None = None
    warnings: list[str] = field(default_factory=list)

    @property
    def utb_authors(self) -> list[str]:
        seen: set[str] = set()
        output: list[str] = []
        for block in self.utb_blocks:
            for author in block.authors:
                if author not in seen:
                    seen.add(author)
                    output.append(author)
        return output

    @property
    def has_utb_affiliation(self) -> bool:
        return bool(self.utb_blocks)

    @property
    def multiple_utb_blocks(self) -> bool:
        return len(self.utb_blocks) > 1


def _parse_authors(authors_raw: str) -> list[str]:
    return [author.strip() for author in authors_raw.split(";") if author.strip()]


def parse_wos_affiliation(raw_text: str) -> ParseResult:
    result = ParseResult(raw_text=raw_text)
    if not raw_text or not raw_text.strip():
        result.ok = False
        result.error = "Prázdny vstupný text"
        return result

    matches = list(_BLOCK_PATTERN.finditer(raw_text))
    if not matches:
        result.warnings.append("Nenašli sa bloky [autori], použitý fallback mód")
        is_utb, keyword = detect_utb_affiliation(raw_text)
        block = AffiliationBlock("", [], raw_text.strip(), is_utb, keyword)
        result.blocks.append(block)
        if is_utb:
            result.utb_blocks.append(block)
        return result

    for match in matches:
        authors_raw = match.group(1).strip()
        affiliation_raw = _CLEANUP_PATTERN.sub("", match.group(2)).strip()
        authors = _parse_authors(authors_raw)
        is_utb, keyword = detect_utb_affiliation(affiliation_raw)
        block = AffiliationBlock(authors_raw, authors, affiliation_raw, is_utb, keyword)
        result.blocks.append(block)
        if is_utb:
            result.utb_blocks.append(block)

    if result.multiple_utb_blocks:
        result.warnings.append(
            f"Nájdených viac UTB blokov ({len(result.utb_blocks)})"
        )

    return result


def parse_wos_affiliation_array(values: list[str] | None) -> list[ParseResult]:
    if not values:
        return []
    return [parse_wos_affiliation(item) for item in values if item]


def extract_ou(affiliation_text: str) -> str:
    match = re.search(
        r"\b(Dept|Department|Inst|Institute|Ctr|Center|Centre)\b[^,;]+",
        affiliation_text,
        re.IGNORECASE,
    )
    return match.group(0).strip() if match else ""
