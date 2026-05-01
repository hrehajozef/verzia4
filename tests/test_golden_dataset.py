import unicodedata

import pytest
from sqlalchemy import text

from src.authors.heuristics import process_record
from src.authors.registry import get_author_registry
from src.authors.workplace_tree import load_workplace_tree
from src.config.settings import settings
from src.db.engines import get_local_engine, get_remote_engine


def _norm(value: str) -> str:
    decomposed = unicodedata.normalize("NFD", value or "")
    without_marks = "".join(ch for ch in decomposed if unicodedata.category(ch) != "Mn")
    return " ".join(without_marks.lower().replace(",", " ").split())


def _ensure_dataset_available() -> None:
    if not settings.local_table:
        pytest.skip("LOCAL_TABLE nie je nastavená.")
    try:
        with get_local_engine().connect() as conn:
            conn.execute(text(f'SELECT 1 FROM "{settings.local_schema}"."{settings.local_table}" LIMIT 1')).scalar()
        with get_remote_engine().connect() as conn:
            conn.execute(text(f'SELECT 1 FROM "{settings.remote_schema}"."utb_authors" LIMIT 1')).scalar()
    except Exception as exc:
        pytest.skip(f"Golden dataset test vyžaduje dostupnú local aj remote DB: {exc}")


def _fetch_record(resource_id: int) -> dict | None:
    with get_local_engine().connect() as conn:
        row = conn.execute(text(f"""
            SELECT
                resource_id,
                "utb.wos.affiliation" AS wos_aff,
                "utb.scopus.affiliation" AS scopus_aff,
                "utb.fulltext.affiliation" AS fulltext_aff,
                "dc.contributor.author" AS dc_authors
            FROM "{settings.local_schema}"."{settings.local_table}"
            WHERE resource_id = :resource_id
        """), {"resource_id": resource_id}).mappings().fetchone()
    return dict(row) if row else None


def _result_faculty_by_author(result: dict) -> dict[str, str]:
    names = result.get("author_internal_names") or []
    faculties = result.get("author_faculty") or []
    return {
        _norm(name): faculty
        for name, faculty in zip(names, faculties)
        if name and faculty
    }


def _matched_faculty_count(actual: dict[str, str], expected: dict[str, str]) -> tuple[int, int]:
    matched = 0
    total = 0
    for expected_name, expected_faculty in expected.items():
        total += 1
        expected_key = _norm(expected_name)
        for actual_name, actual_faculty in actual.items():
            if expected_key in actual_name or actual_name in expected_key:
                if actual_faculty == expected_faculty:
                    matched += 1
                break
    return matched, total


def test_golden_dataset_faculty_accuracy():
    _ensure_dataset_available()

    golden = {
        5326: {
            "Krasny": "Faculty of Technology",
            "Lapcik": "Faculty of Technology",
            "Lapcikova": "Faculty of Technology",
        },
        5310: {
            "Haburajova Ilavska": "Faculty of Humanities",
        },
        5305: {
            "Burita": "Faculty of Management and Economics",
        },
        5301: {
            "Hruska": "Faculty of Applied Informatics",
        },
        5303: {
            "Macku": "Faculty of Applied Informatics",
            "Novosad": "Faculty of Technology",
            "Samek": "Faculty of Technology",
        },
        5300: {
            "Ovsik": "Faculty of Technology",
            "Manas": "Faculty of Technology",
            "Stanek": "Faculty of Technology",
            "Bednarik": "Faculty of Technology",
            "Kratky": "Faculty of Technology",
        },
        11033: {
            "Patikova": "Faculty of Applied Informatics",
        },
        10052: {
            "Bednar": "Faculty of Management and Economics",
        },
        12856: {
            "Monkova": "Faculty of Technology",
        },
        10032: {
            "Skrovankova": "Faculty of Technology",
            "Mlcek": "Faculty of Technology",
            "Snopek": "Faculty of Technology",
        },
    }

    registry = get_author_registry(remote_engine=get_remote_engine())
    workplace_tree = load_workplace_tree(remote_engine=get_remote_engine())

    matched_total = 0
    expected_total = 0

    for resource_id, expected in golden.items():
        row = _fetch_record(resource_id)
        if not row:
            pytest.skip(f"V lokálnej DB chýba golden záznam {resource_id}.")

        result = process_record(
            resource_id=resource_id,
            wos_aff_arr=row.get("wos_aff"),
            scopus_aff_arr=row.get("scopus_aff"),
            fulltext_aff_arr=row.get("fulltext_aff"),
            dc_authors_arr=row.get("dc_authors"),
            registry=registry,
            remote_engine=get_remote_engine(),
            workplace_tree=workplace_tree,
        )

        actual = _result_faculty_by_author(result)
        matched, total = _matched_faculty_count(actual, expected)
        matched_total += matched
        expected_total += total

    assert expected_total > 0
    assert matched_total / expected_total >= 0.80
