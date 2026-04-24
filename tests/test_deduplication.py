"""Testy pre src/quality/dedup.py"""

import pytest
from src.quality.dedup import (
    CONTENT_TITLE_THRESHOLD,
    _normalize_text,
    _norm_column_value,
    _extract_year,
    _normalize_issn,
    _normalize_merge_pairs,
    find_content_duplicates,
    find_duplicates_by_column,
    find_duplicates_fuzzy,
)


# -----------------------------------------------------------------------
# Pomocné funkcie
# -----------------------------------------------------------------------

class TestNormalizeText:
    def test_lowercase(self):
        assert _normalize_text("Hello World") == "hello world"

    def test_removes_diacritics(self):
        assert _normalize_text("Schrödinger") == "schrodinger"
        assert _normalize_text("Česká republika") == "ceska republika"

    def test_collapses_spaces(self):
        assert _normalize_text("hello   world") == "hello world"

    def test_empty(self):
        assert _normalize_text("") == ""
        assert _normalize_text(None) == ""


class TestNormColumnValue:
    def test_string(self):
        assert _norm_column_value("10.1234/TEST") == "10.1234/test"

    def test_list_takes_first(self):
        assert _norm_column_value(["10.1234/abc", "other"]) == "10.1234/abc"

    def test_empty_list(self):
        assert _norm_column_value([]) == ""

    def test_none(self):
        assert _norm_column_value(None) == ""

    def test_strips_whitespace(self):
        assert _norm_column_value("  value  ") == "value"


class TestExtractYear:
    def test_iso_date(self):
        assert _extract_year("2018-06-15") == 2018

    def test_year_only(self):
        assert _extract_year("2021") == 2021

    def test_in_text(self):
        assert _extract_year("Published in 2019") == 2019

    def test_list_input(self):
        assert _extract_year(["2020-01-01"]) == 2020

    def test_none(self):
        assert _extract_year(None) is None

    def test_invalid(self):
        assert _extract_year("no year here") is None


class TestNormalizeIssn:
    def test_standard_issn(self):
        assert _normalize_issn("1234-5678") == "12345678"

    def test_with_x(self):
        assert _normalize_issn("1234-567X") == "1234567x"

    def test_none(self):
        assert _normalize_issn(None) == ""

    def test_list(self):
        assert _normalize_issn(["1234-5678"]) == "12345678"


class TestExactDeduplication:
    def _make_engine(self, records: list[dict]):
        from unittest.mock import MagicMock

        row_objects = []
        for r in records:
            row_objects.append((r["resource_id"], r.get("value")))

        conn = MagicMock()
        conn.__enter__ = lambda s: conn
        conn.__exit__ = MagicMock(return_value=False)
        conn.execute.return_value.fetchall.return_value = row_objects

        engine = MagicMock()
        engine.connect.return_value = conn
        return engine

    def test_find_duplicates_by_column_groups_equal_values(self):
        engine = self._make_engine([
            {"resource_id": 1, "value": ["10.1234/ABC"]},
            {"resource_id": 2, "value": ["10.1234/abc"]},
            {"resource_id": 3, "value": ["10.9999/xyz"]},
        ])
        result = find_duplicates_by_column(engine)
        assert len(result) == 1
        ids, column, matched_value, score, details = result[0]
        assert ids == [1, 2]
        assert column == "dc.identifier.doi"
        assert matched_value == "10.1234/abc"
        assert score == 1.0
        assert details["matched_value"] == "10.1234/abc"

    def test_find_duplicates_by_column_rejects_unknown_column(self):
        engine = self._make_engine([])
        with pytest.raises(ValueError):
            find_duplicates_by_column(engine, by_column="author_flags")


class TestContentDeduplication:
    def _make_engine(self, records: list[dict]):
        from unittest.mock import MagicMock

        row_objects = []
        for r in records:
            row = MagicMock()
            row.resource_id = r["resource_id"]
            row.title = r.get("title")
            row.authors = r.get("authors")
            row.abstract = r.get("abstract")
            row.issn = r.get("issn")
            row.doctype = r.get("doctype")
            row._mapping = {
                "utb.relation.volume": r.get("utb.relation.volume"),
                "utb.relation.issue": r.get("utb.relation.issue"),
            }
            row_objects.append(row)

        conn = MagicMock()
        conn.__enter__ = lambda s: conn
        conn.__exit__ = MagicMock(return_value=False)
        conn.execute.return_value.fetchall.return_value = row_objects

        engine = MagicMock()
        engine.connect.return_value = conn
        return engine

    def test_content_duplicates_accepts_almost_equal_titles(self):
        engine = self._make_engine([
            {
                "resource_id": 1,
                "title": "Machine Learning in Polymer Science",
                "authors": ["Smith, John"],
                "abstract": ["A short abstract."],
                "issn": "1234-5678",
                "doctype": "article",
                "utb.relation.volume": "12",
            },
            {
                "resource_id": 2,
                "title": "Machine Learning in Polymer Sciences",
                "authors": ["Smith, John"],
                "abstract": ["A short abstract."],
                "issn": "1234-5678",
                "doctype": "article",
                "utb.relation.volume": "12",
            },
        ])
        result = find_content_duplicates(engine)
        assert len(result) == 1
        _, _, match_type, score, details = result[0]
        assert match_type == "exact:content"
        assert score >= CONTENT_TITLE_THRESHOLD
        assert details["authors_match"] is True
        assert details["abstract_match"] is True

    def test_content_duplicates_requires_more_than_title_only(self):
        engine = self._make_engine([
            {
                "resource_id": 1,
                "title": "Machine Learning in Polymer Science",
                "authors": None,
                "abstract": None,
                "issn": None,
                "doctype": "article",
            },
            {
                "resource_id": 2,
                "title": "Machine Learning in Polymer Science",
                "authors": None,
                "abstract": None,
                "issn": None,
                "doctype": "article",
            },
        ])
        assert find_content_duplicates(engine) == []


class TestMergePlanning:
    def test_normalize_merge_pairs_resolves_chain_duplicates(self):
        planned = _normalize_merge_pairs([
            (1, 2, "exact:dc.identifier.doi", 1.0, {"basis": "exact_column"}, None),
            (2, 3, "exact:dc.identifier.doi", 1.0, {"basis": "exact_column"}, None),
        ])
        assert planned == [
            (1, 2, "exact:dc.identifier.doi", 1.0, {"basis": "exact_column"}, None),
            (1, 3, "exact:dc.identifier.doi", 1.0, {"basis": "exact_column"}, None),
        ]

    def test_normalize_merge_pairs_keeps_existing_survivor(self):
        planned = _normalize_merge_pairs([
            (10, 20, "exact:dc.identifier.doi", 1.0, {"basis": "exact_column"}, None),
            (5, 20, "exact:dc.identifier.doi", 1.0, {"basis": "exact_column"}, None),
        ])
        assert planned == [
            (10, 20, "exact:dc.identifier.doi", 1.0, {"basis": "exact_column"}, None),
            (10, 5, "exact:dc.identifier.doi", 1.0, {"basis": "exact_column"}, None),
        ]


# -----------------------------------------------------------------------
# find_duplicates_fuzzy – test s mock dátami
# -----------------------------------------------------------------------

class TestFindDuplicatesFuzzy:
    """Testy fuzzy deduplikácie pomocou mock SQLAlchemy engine."""

    def _make_engine(self, records: list[dict]):
        """Vytvorí fake engine, ktorý vracia zadané záznamy."""
        from unittest.mock import MagicMock

        row_objects = []
        for r in records:
            row = MagicMock()
            row.resource_id = r["resource_id"]
            row.title       = r.get("title")
            row.issued      = r.get("issued")
            row.issn        = r.get("issn")
            row.isbn        = r.get("isbn")
            row_objects.append(row)

        conn = MagicMock()
        conn.__enter__ = lambda s: conn
        conn.__exit__  = MagicMock(return_value=False)
        conn.execute.return_value.fetchall.return_value = row_objects

        engine = MagicMock()
        engine.connect.return_value = conn
        return engine

    def test_identical_titles_same_year(self):
        records = [
            {"resource_id": 1, "title": "Machine Learning in Polymer Science", "issued": "2020"},
            {"resource_id": 2, "title": "Machine Learning in Polymer Science", "issued": "2020"},
        ]
        engine = self._make_engine(records)
        result = find_duplicates_fuzzy(engine, title_threshold=0.85)
        assert len(result) == 1
        assert result[0][0] == 1
        assert result[0][1] == 2
        assert result[0][3] == 1.0  # perfect score

    def test_similar_titles_different_year(self):
        records = [
            {"resource_id": 1, "title": "Deep Learning Applications", "issued": "2018"},
            {"resource_id": 2, "title": "Deep Learning Applications", "issued": "2020"},  # year diff > 1
        ]
        engine = self._make_engine(records)
        result = find_duplicates_fuzzy(engine, title_threshold=0.85)
        # Rok rozdiel > 1 → nesmie byť nájdený
        assert result == []

    def test_similar_titles_adjacent_year(self):
        records = [
            {"resource_id": 1, "title": "Deep Learning Applications", "issued": "2019"},
            {"resource_id": 2, "title": "Deep Learning Applications", "issued": "2020"},  # year diff = 1
        ]
        engine = self._make_engine(records)
        result = find_duplicates_fuzzy(engine, title_threshold=0.85)
        assert len(result) == 1

    def test_different_titles(self):
        records = [
            {"resource_id": 1, "title": "Polymer Engineering Methods", "issued": "2019"},
            {"resource_id": 2, "title": "Crisis Management in Organizations", "issued": "2019"},
        ]
        engine = self._make_engine(records)
        result = find_duplicates_fuzzy(engine, title_threshold=0.85)
        assert result == []

    def test_no_self_comparison(self):
        records = [
            {"resource_id": 1, "title": "Single Record", "issued": "2020"},
        ]
        engine = self._make_engine(records)
        result = find_duplicates_fuzzy(engine, title_threshold=0.85)
        assert result == []

    def test_match_type_with_issn(self):
        records = [
            {"resource_id": 1, "title": "Food Chemistry Research", "issued": "2020", "issn": "1234-5678"},
            {"resource_id": 2, "title": "Food Chemistry Research", "issued": "2020", "issn": "1234-5678"},
        ]
        engine = self._make_engine(records)
        result = find_duplicates_fuzzy(engine, title_threshold=0.85)
        assert len(result) == 1
        assert "issn" in result[0][2]  # match_type contains "issn"

    def test_no_duplicate_pairs(self):
        """Každý pár sa smie objaviť iba raz."""
        records = [
            {"resource_id": 1, "title": "Polymer Science Review", "issued": "2020"},
            {"resource_id": 2, "title": "Polymer Science Review", "issued": "2020"},
            {"resource_id": 3, "title": "Polymer Science Review", "issued": "2021"},
        ]
        engine = self._make_engine(records)
        result = find_duplicates_fuzzy(engine, title_threshold=0.85)
        pairs = {(min(a, b), max(a, b)) for a, b, *_ in result}
        assert len(pairs) == len(result), "Duplicate pairs detected"
