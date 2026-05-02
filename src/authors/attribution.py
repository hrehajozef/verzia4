"""Datove struktury pre per-paper atribuciu autorov."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from src.authors.registry import InternalAuthor


@dataclass
class AuthorAttribution:
    matched_author: InternalAuthor | None
    display_name: str
    per_paper_faculty: str = ""
    per_paper_ou: str = ""
    per_paper_source: str = ""
    per_paper_confidence: float = 0.0
    default_faculty: str = ""
    default_ou: str = ""
    scopus_raw_affiliation: str = ""
    wos_raw_affiliation: str = ""
    flags: dict[str, Any] = field(default_factory=dict)


def _serialize_attribution(attribution: AuthorAttribution) -> dict[str, Any]:
    data = asdict(attribution)
    data["matched_author"] = (
        {
            "author_id": attribution.matched_author.limited_author_id,
            "utbid": attribution.matched_author.utb_id,
            "display_name": attribution.matched_author.display_name,
            "canonical_name": attribution.matched_author.canonical_name,
            "organization_id": attribution.matched_author.organization_id,
            "faculty": attribution.matched_author.faculty,
        }
        if attribution.matched_author
        else None
    )
    return data
