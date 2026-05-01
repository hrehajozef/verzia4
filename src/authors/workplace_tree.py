"""Praca so stromom pracovisk UTB z remote tabulky veda.obd_prac."""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass

import jellyfish
from sqlalchemy import text

from src.common.constants import (
    CZECH_DEPARTMENT_MAP_NORM,
    CZECH_FACULTY_MAP_NORM,
    FACULTIES,
)
from src.config.settings import settings
from src.db.engines import get_remote_engine


@dataclass(frozen=True)
class WorkplaceNode:
    id: int
    code: str
    name_cs: str
    name_en: str
    abbreviations: tuple[str, ...]
    parent_id: int | None
    is_department: bool


_FACULTY_LEVEL_NAMES: frozenset[str] = frozenset({
    "Faculty of Technology",
    "Faculty of Management and Economics",
    "Faculty of Applied Informatics",
    "Faculty of Multimedia Communications",
    "Faculty of Logistics and Crisis Management",
    "Faculty of Humanities",
    "University Institute",
})


def _norm(value: str) -> str:
    if not value:
        return ""
    decomposed = unicodedata.normalize("NFD", value)
    without_marks = "".join(ch for ch in decomposed if unicodedata.category(ch) != "Mn")
    lowered = without_marks.lower()
    lowered = lowered.replace("&", " and ")
    lowered = lowered.replace("center", "centre")
    lowered = re.sub(r"[^\w\s]", " ", lowered, flags=re.UNICODE)
    return re.sub(r"\s+", " ", lowered).strip()


def _abbrs(*values: object) -> tuple[str, ...]:
    result: list[str] = []
    seen: set[str] = set()
    for raw in values:
        value = str(raw or "").strip()
        if not value:
            continue
        key = _norm(value)
        if key in seen:
            continue
        seen.add(key)
        result.append(value)
    return tuple(result)


def _english_name(name_cs: str, name_en: str) -> str:
    normalized_cs = _norm(name_cs)
    if normalized_cs in CZECH_DEPARTMENT_MAP_NORM:
        return CZECH_DEPARTMENT_MAP_NORM[normalized_cs]
    if normalized_cs in CZECH_FACULTY_MAP_NORM:
        faculty_id = CZECH_FACULTY_MAP_NORM[normalized_cs]
        return FACULTIES.get(faculty_id, name_cs)

    cleaned_en = str(name_en or "").strip()
    if cleaned_en and not cleaned_en.startswith("EN_"):
        return cleaned_en
    return name_cs


def _is_faculty_node(node: WorkplaceNode) -> bool:
    if node.name_en.startswith("Faculty of "):
        return True
    return node.name_en in _FACULTY_LEVEL_NAMES


def load_workplace_tree(remote_engine=None) -> dict[int, WorkplaceNode]:
    """Nacita cely platny strom pracovisk UTB z obd_prac."""
    engine = remote_engine or get_remote_engine()
    schema = settings.remote_schema
    sql = text(f"""
        SELECT
            id,
            kodprac,
            nazev,
            nazev_eng,
            zkratka,
            zkr,
            id_nadrizene,
            je_katedra
        FROM "{schema}".obd_prac
        WHERE platnost_do IS NULL OR platnost_do > now()
    """)

    with engine.connect() as conn:
        rows = conn.execute(sql).fetchall()

    tree: dict[int, WorkplaceNode] = {}
    for row in rows:
        node_id = int(row.id)
        parent_id = int(row.id_nadrizene) if row.id_nadrizene is not None else None
        name_cs = str(row.nazev or "").strip()
        name_en = _english_name(name_cs, str(row.nazev_eng or "").strip())
        tree[node_id] = WorkplaceNode(
            id=node_id,
            code=str(row.kodprac or "").strip(),
            name_cs=name_cs,
            name_en=name_en,
            abbreviations=_abbrs(row.zkratka, row.zkr),
            parent_id=parent_id,
            is_department=str(row.je_katedra or "").strip().upper() == "A",
        )
    return tree


def walk_to_faculty(node_id: int, tree: dict[int, WorkplaceNode]) -> WorkplaceNode | None:
    """Vrati prvy faculty-level uzol pri stupe nahor stromom."""
    visited: set[int] = set()
    current_id = node_id
    while current_id is not None and current_id not in visited:
        visited.add(current_id)
        node = tree.get(current_id)
        if node is None:
            return None
        if _is_faculty_node(node):
            return node
        current_id = node.parent_id
    return None


def find_workplace_by_name(
    candidate: str,
    tree: dict[int, WorkplaceNode],
    threshold: float = 0.92,
) -> tuple[WorkplaceNode | None, float]:
    """Fuzzy match kandidata na pracovisko podla EN/CS nazvu a skratiek."""
    normalized_candidate = _norm(candidate)
    if not normalized_candidate:
        return None, 0.0

    best_node: WorkplaceNode | None = None
    best_score = 0.0
    best_weight = -1

    for node in tree.values():
        variants = [node.name_en, node.name_cs, *node.abbreviations]
        local_best = 0.0
        for variant in variants:
            normalized_variant = _norm(variant)
            if not normalized_variant:
                continue
            if normalized_candidate == normalized_variant:
                score = 1.0
            elif normalized_candidate in normalized_variant or normalized_variant in normalized_candidate:
                score = max(
                    jellyfish.jaro_winkler_similarity(normalized_candidate, normalized_variant),
                    0.92,
                )
            else:
                score = jellyfish.jaro_winkler_similarity(normalized_candidate, normalized_variant)
            if score > local_best:
                local_best = score

        if local_best < threshold:
            continue

        weight = len(_norm(node.name_en or node.name_cs))
        if local_best > best_score or (local_best == best_score and weight > best_weight):
            best_node = node
            best_score = local_best
            best_weight = weight

    return best_node, best_score
