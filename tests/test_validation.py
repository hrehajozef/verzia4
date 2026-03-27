"""Testy pre src/quality/checks.py"""

import pytest
from src.quality.checks import (
    check_trailing_spaces,
    check_mojibake,
    check_doi_format,
    check_encoding_chars,
    check_nbsp,
    check_double_space,
    validate_record,
    fix_mojibake,
    _fix_text_str,
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
        assert check_mojibake("\u00c3\u00a9") is True

    def test_utf8_as_latin1_o_umlaut(self):
        assert check_mojibake("\u00c3\u00b6") is True

    def test_utf8_3byte_as_latin1(self):
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
        assert check_doi_format("") is True

    def test_whitespace_stripped(self):
        assert check_doi_format("  10.1234/test  ") is True


# -----------------------------------------------------------------------
# check_encoding_chars
# -----------------------------------------------------------------------

class TestEncodingChars:
    def test_dotless_i(self):
        assert check_encoding_chars("Lapı") is True        # ı U+0131

    def test_g_circumflex(self):
        assert check_encoding_chars("Ĝeho") is True        # Ĝ U+011C → ř

    def test_h_circumflex(self):
        assert check_encoding_chars("Ĥ") is True           # Ĥ U+0124 → ů

    def test_pua_ft(self):
        assert check_encoding_chars("\ue09digure") is True  # PUA ft ligature

    def test_standalone_caron(self):
        assert check_encoding_chars("Venˇec") is True      # ˇ U+02C7

    def test_standalone_double_acute(self):
        assert check_encoding_chars("kl˝uc") is True       # ˝ U+02DD

    def test_standalone_combining_acute(self):
        assert check_encoding_chars("a\u0301b") is True    # U+0301 combining acute

    def test_clean_text(self):
        assert check_encoding_chars("Normálny text") is False

    def test_empty(self):
        assert check_encoding_chars("") is False


# -----------------------------------------------------------------------
# check_nbsp
# -----------------------------------------------------------------------

class TestNbsp:
    def test_nbsp_detected(self):
        assert check_nbsp("hello\u00a0world") is True

    def test_narrow_nbsp(self):
        assert check_nbsp("hello\u202fworld") is True

    def test_clean_text(self):
        assert check_nbsp("hello world") is False

    def test_empty(self):
        assert check_nbsp("") is False


# -----------------------------------------------------------------------
# check_double_space
# -----------------------------------------------------------------------

class TestDoubleSpace:
    def test_double_space(self):
        assert check_double_space("hello  world") is True

    def test_triple_space(self):
        assert check_double_space("hello   world") is True

    def test_single_space(self):
        assert check_double_space("hello world") is False

    def test_empty(self):
        assert check_double_space("") is False


# -----------------------------------------------------------------------
# _fix_text_str (opravná pipeline)
# -----------------------------------------------------------------------

class TestFixTextStr:
    def test_trailing_spaces_fixed(self):
        fixed, types = _fix_text_str("  hello  ")
        assert fixed == "hello"
        assert "trailing_spaces" in types

    def test_nbsp_replaced(self):
        fixed, types = _fix_text_str("hello\u00a0world")
        assert fixed == "hello world"
        assert "nbsp" in types

    def test_double_space_fixed(self):
        fixed, types = _fix_text_str("hello  world")
        assert fixed == "hello world"
        assert "double_space" in types

    def test_standalone_caron_removed(self):
        fixed, types = _fix_text_str("Venˇec")
        assert "ˇ" not in fixed
        assert "standalone_diacritics" in types

    def test_dotless_i_fixed(self):
        fixed, types = _fix_text_str("Lapı")
        assert "ı" not in fixed
        assert fixed == "Lapí"
        assert "encoding_chars" in types

    def test_g_circumflex_fixed(self):
        fixed, types = _fix_text_str("Ĝeho")
        assert fixed == "řeho"
        assert "encoding_chars" in types

    def test_h_circumflex_fixed(self):
        fixed, types = _fix_text_str("Ĥ")
        assert fixed == "ů"
        assert "encoding_chars" in types

    def test_pua_ft_fixed(self):
        fixed, types = _fix_text_str("\ue09digure")
        assert fixed == "ftigure"
        assert "encoding_chars" in types

    def test_en_dash_fix(self):
        fixed, types = _fix_text_str("a\u0bc5b")  # ௅ → –
        assert fixed == "a\u2013b"
        assert "encoding_chars" in types

    def test_nbsp_then_double_space(self):
        # nbsp + regular space → double space → single space
        fixed, types = _fix_text_str("a\u00a0 b")
        assert fixed == "a b"
        assert "nbsp" in types
        assert "double_space" in types

    def test_no_fix_needed(self):
        fixed, types = _fix_text_str("Normálny text")
        assert types == []
        assert fixed == "Normálny text"

    def test_via_query_param_not_in_text_fix(self):
        # URL query params sú riešené zvlášť (nie cez textovú pipeline)
        s = "https://example.com/?via%3Dihub"
        fixed, types = _fix_text_str(s)
        assert types == []  # textová pipeline nezasahuje do URL


# -----------------------------------------------------------------------
# validate_record
# -----------------------------------------------------------------------

class TestValidateRecord:
    def _base_row(self, **overrides) -> dict:
        row = {
            "dc.title":                      "Normal title",
            "dc.contributor.author":         ["Author A", "Author B"],
            "dc.description.abstract":       None,
            "dc.identifier.doi":             ["10.1234/valid"],
            "utb.wos.affiliation":           None,
            "utb.scopus.affiliation":        None,
            "utb.identifier.wok":            None,
            "dc.identifier.uri":             None,
            "author_dc_names":       None,
            "author_internal_names": None,
        }
        row.update(overrides)
        return row

    def test_clean_record(self):
        status, issues, fixes = validate_record(self._base_row(), set())
        assert status == "ok"
        assert issues == {}

    def test_trailing_space_in_title(self):
        status, issues, fixes = validate_record(
            self._base_row(**{"dc.title": "Title  "}), set()
        )
        assert status == "has_issues"
        assert "trailing_spaces" in issues
        assert "dc.title" in issues["trailing_spaces"]
        assert "dc.title" in fixes
        assert fixes["dc.title"]["suggested"] == "Title"

    def test_mojibake_in_title(self):
        status, issues, fixes = validate_record(
            self._base_row(**{"dc.title": "\u00c3\u00a9test"}), set()
        )
        assert status == "has_issues"
        assert "mojibake" in issues
        assert "dc.title" in fixes

    def test_encoding_chars_in_title(self):
        status, issues, fixes = validate_record(
            self._base_row(**{"dc.title": "Venˇec"}), set()
        )
        assert status == "has_issues"
        assert "standalone_diacritics" in issues
        assert fixes["dc.title"]["suggested"] == "Venec"

    def test_nbsp_in_abstract(self):
        status, issues, fixes = validate_record(
            self._base_row(**{"dc.description.abstract": "hello\u00a0world"}), set()
        )
        assert status == "has_issues"
        assert "nbsp" in issues
        assert fixes["dc.description.abstract"]["suggested"] == "hello world"

    def test_double_space_in_author(self):
        status, issues, fixes = validate_record(
            self._base_row(**{"dc.contributor.author": ["Smith,  John"]}), set()
        )
        assert status == "has_issues"
        assert "double_space" in issues

    def test_pipe_in_affiliation(self):
        status, issues, _ = validate_record(
            self._base_row(**{"utb.wos.affiliation": "UTB|Zlin"}), set()
        )
        assert status == "has_issues"
        assert "pipe_in_field" in issues

    def test_brackets_in_title(self):
        status, issues, _ = validate_record(
            self._base_row(**{"dc.title": "Main Title [Vedľajší titul]"}), set()
        )
        assert status == "has_issues"
        assert "brackets_in_title" in issues

    def test_invalid_wos_id(self):
        status, issues, _ = validate_record(
            self._base_row(**{"utb.identifier.wok": "A00123456789"}), set()
        )
        assert status == "has_issues"
        assert "invalid_wos_id" in issues

    def test_valid_wos_id(self):
        status, issues, _ = validate_record(
            self._base_row(**{"utb.identifier.wok": "000375229700041"}), set()
        )
        assert "invalid_wos_id" not in issues

    def test_invalid_doi(self):
        status, issues, fixes = validate_record(
            self._base_row(**{"dc.identifier.doi": ["https://doi.org/10.1234/x"]}),
            set(),
        )
        assert status == "has_issues"
        assert "invalid_doi" in issues
        assert fixes["dc.identifier.doi"]["suggested"] == ["10.1234/x"]

    def test_doi_with_query_params_suggestion(self):
        # DOI s ?via... nie je invalid_doi, ale dostane suggested_fix
        status, issues, fixes = validate_record(
            self._base_row(**{"dc.identifier.doi": "10.1016/j.foo.2020?via%3Dihub"}),
            set(),
        )
        assert "dc.identifier.doi" in fixes
        assert fixes["dc.identifier.doi"]["suggested"] == "10.1016/j.foo.2020"

    def test_internal_author_not_in_registry(self):
        row = self._base_row(**{
            "author_internal_names": ["Novák, Jan"],
            "author_dc_names":       ["Novak, Jan", "Smith, Bob"],
        })
        status, issues, _ = validate_record(row, {"Smith, Bob"})
        assert status == "has_issues"
        assert "authors_not_in_registry" in issues

    def test_internal_author_in_registry(self):
        row = self._base_row(**{
            "author_internal_names": ["Novák, Jan"],
            "author_dc_names":       ["Novak, Jan"],
        })
        _, issues, _ = validate_record(row, {"Novák, Jan"})
        assert "authors_not_in_registry" not in issues

    def test_multiple_issues(self):
        row = self._base_row(**{
            "dc.title":          "  Title  ",
            "dc.identifier.doi": ["https://bad-doi"],
        })
        status, issues, fixes = validate_record(row, set())
        assert status == "has_issues"
        assert "trailing_spaces" in issues
        assert "invalid_doi" in issues

    def test_doi_as_string(self):
        row = self._base_row(**{"dc.identifier.doi": "10.1234/string-doi"})
        status, issues, _ = validate_record(row, set())
        assert "invalid_doi" not in issues

    def test_url_query_params_suggestion(self):
        row = self._base_row(**{
            "dc.identifier.uri": "https://www.sciencedirect.com/article/pii/S123?via%3Dihub"
        })
        _, _, fixes = validate_record(row, set())
        assert "dc.identifier.uri" in fixes
        assert "via" not in fixes["dc.identifier.uri"]["suggested"]
