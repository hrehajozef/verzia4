"""Testy pre src/quality/checks.py"""

import pytest
from src.quality.checks import (
    check_trailing_spaces,
    check_mojibake,
    check_doi_format,
    validate_record,
)


# -----------------------------------------------------------------------
# check_trailing_spaces
# -----------------------------------------------------------------------

class TestTrailingSpaces:
    def test_leading_space(self):
        assert check_trailing_spaces("  hello") is True

    def test_trailing_space(self):
        assert check_trailing_spaces("hello  ") is True

    def test_both_sides(self):
        assert check_trailing_spaces("  hello  ") is True

    def test_clean_string(self):
        assert check_trailing_spaces("hello world") is False

    def test_empty_string(self):
        assert check_trailing_spaces("") is False

    def test_internal_spaces_only(self):
        assert check_trailing_spaces("hello world") is False

    def test_newline_trailing(self):
        assert check_trailing_spaces("hello\n") is True

    def test_tab_leading(self):
        assert check_trailing_spaces("\thello") is True


# -----------------------------------------------------------------------
# check_mojibake
# -----------------------------------------------------------------------

class TestMojibake:
    def test_clean_ascii(self):
        assert check_mojibake("Hello world") is False

    def test_clean_diacritics(self):
        assert check_mojibake("Schrödinger, česky, slovensky") is False

    def test_replacement_char(self):
        assert check_mojibake("Hello \ufffd world") is True

    def test_utf8_as_latin1_e_acute(self):
        # "é" encoded in UTF-8 (0xC3 0xA9) read as Latin-1 → "Ã©"
        assert check_mojibake("\u00c3\u00a9") is True

    def test_utf8_as_latin1_o_umlaut(self):
        # "ö" → "\u00c3\u00b6" (Ã¶)
        assert check_mojibake("\u00c3\u00b6") is True

    def test_utf8_3byte_as_latin1(self):
        # U+2019 (right single quote) in UTF-8 = E2 80 99 → as Latin-1: â€™
        assert check_mojibake("\u00e2\u0080\u0099") is True

    def test_empty_string(self):
        assert check_mojibake("") is False


# -----------------------------------------------------------------------
# check_doi_format
# -----------------------------------------------------------------------

class TestDoiFormat:
    def test_valid_doi(self):
        assert check_doi_format("10.1234/test-doi") is True

    def test_valid_doi_complex(self):
        assert check_doi_format("10.1016/j.polymer.2018.06.030") is True

    def test_valid_doi_long_registrant(self):
        assert check_doi_format("10.123456789/suffix") is True

    def test_invalid_with_https_prefix(self):
        assert check_doi_format("https://doi.org/10.1234/x") is False

    def test_invalid_with_http_prefix(self):
        assert check_doi_format("http://dx.doi.org/10.1234/x") is False

    def test_invalid_no_10_prefix(self):
        assert check_doi_format("11.1234/test") is False

    def test_invalid_short_registrant(self):
        assert check_doi_format("10.12/test") is False

    def test_invalid_random_string(self):
        assert check_doi_format("not-a-doi") is False

    def test_empty_string_is_ok(self):
        # Chýbajúce DOI nie je chyba formátu
        assert check_doi_format("") is True

    def test_whitespace_stripped(self):
        assert check_doi_format("  10.1234/test  ") is True


# -----------------------------------------------------------------------
# validate_record
# -----------------------------------------------------------------------

class TestValidateRecord:
    def _base_row(self, **overrides) -> dict:
        row = {
            "dc.title": "Normal title",
            "dc.contributor.author": ["Author A", "Author B"],
            "dc.description.abstract": None,
            "dc.identifier.doi": ["10.1234/valid"],
            "utb.wos.affiliation": None,
            "utb.scopus.affiliation": None,
            "dc_contributor_author": None,
            "utb_contributor_internalauthor": None,
        }
        row.update(overrides)
        return row

    def test_clean_record(self):
        status, issues = validate_record(1, self._base_row(), set())
        assert status == "ok"
        assert issues == {}

    def test_trailing_space_in_title(self):
        status, issues = validate_record(1, self._base_row(**{"dc.title": "Title  "}), set())
        assert status == "has_issues"
        assert "trailing_spaces" in issues
        assert "dc.title" in issues["trailing_spaces"]

    def test_mojibake_in_title(self):
        # "Ã©" = é mojibake
        status, issues = validate_record(1, self._base_row(**{"dc.title": "\u00c3\u00a9test"}), set())
        assert status == "has_issues"
        assert "mojibake" in issues

    def test_invalid_doi(self):
        status, issues = validate_record(
            1,
            self._base_row(**{"dc.identifier.doi": ["https://doi.org/10.1234/x"]}),
            set(),
        )
        assert status == "has_issues"
        assert "invalid_doi" in issues

    def test_internal_author_not_in_registry(self):
        row = self._base_row(**{
            "utb_contributor_internalauthor": ["Novák, Jan"],
            "dc_contributor_author": ["Novak, Jan", "Smith, Bob"],
        })
        status, issues = validate_record(1, row, {"Smith, Bob"})
        # Novák, Jan nie je v registry (len Smith je)
        assert status == "has_issues"
        assert "authors_not_in_registry" in issues

    def test_internal_author_in_registry(self):
        row = self._base_row(**{
            "utb_contributor_internalauthor": ["Novák, Jan"],
            "dc_contributor_author": ["Novak, Jan"],
        })
        registry = {"Novák, Jan"}
        status, issues = validate_record(1, row, registry)
        assert "authors_not_in_registry" not in issues

    def test_multiple_issues(self):
        row = self._base_row(**{
            "dc.title": "  Title  ",
            "dc.identifier.doi": ["https://bad-doi"],
        })
        status, issues = validate_record(1, row, set())
        assert status == "has_issues"
        assert "trailing_spaces" in issues
        assert "invalid_doi" in issues

    def test_doi_as_string(self):
        # DOI môže prísť ako string aj ako list
        row = self._base_row(**{"dc.identifier.doi": "10.1234/string-doi"})
        status, issues = validate_record(1, row, set())
        assert "invalid_doi" not in issues
