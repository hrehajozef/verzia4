"""Testy pre src/dates/parser.py a src/dates/labels.py"""

import pytest
from datetime import date

from src.dates.labels import DateCategory, match_label, normalize_label
from src.dates.parser import parse_fulltext_dates


# -----------------------------------------------------------------------
# normalize_label / match_label
# -----------------------------------------------------------------------

class TestNormalizeLabel:
    def test_lowercase(self):
        assert normalize_label("Received") == "received"

    def test_removes_diacritics(self):
        assert normalize_label("Přijato") == "prijato"

    def test_strips_colon(self):
        assert normalize_label("Accepted:") == "accepted"

    def test_strips_dot(self):
        assert normalize_label("Published.") == "published"

    def test_collapses_spaces(self):
        assert normalize_label("Received  in  revised") == "received in revised"


class TestMatchLabel:
    def test_received(self):
        assert match_label("Received") == DateCategory.RECEIVED

    def test_received_case_insensitive(self):
        assert match_label("RECEIVED") == DateCategory.RECEIVED

    def test_accepted(self):
        assert match_label("Accepted") == DateCategory.ACCEPTED

    def test_accepted_for_publication(self):
        assert match_label("Accepted for publication") == DateCategory.ACCEPTED

    def test_published_online(self):
        assert match_label("Published online") == DateCategory.PUBLISHED_ONLINE

    def test_available_online(self):
        assert match_label("Available online") == DateCategory.PUBLISHED_ONLINE

    def test_reviewed(self):
        assert match_label("Received in revised form") == DateCategory.REVIEWED

    def test_extra_revised(self):
        assert match_label("Revised") == DateCategory.EXTRA

    def test_czech_received(self):
        assert match_label("Do redakce došlo") == DateCategory.RECEIVED

    def test_czech_accepted(self):
        assert match_label("Přijato do tisku") == DateCategory.ACCEPTED

    def test_unknown_label(self):
        assert match_label("Something completely unknown xyz") == DateCategory.UNKNOWN

    def test_prefix_match(self):
        # "Received August 2018" → starts with "received"
        assert match_label("Received August") == DateCategory.RECEIVED


# -----------------------------------------------------------------------
# parse_fulltext_dates
# -----------------------------------------------------------------------

class TestParseFulltextDates:
    def test_basic_received_accepted(self):
        text  = "Received: 15 March 2018; Accepted: 20 June 2018"
        result = parse_fulltext_dates(resource_id=1, raw_text=text)
        assert result.received == date(2018, 3, 15)
        assert result.accepted == date(2018, 6, 20)
        assert result.status   == "processed"
        assert result.needs_llm is False

    def test_iso_dates(self):
        text   = "Received 2018-01-10; Published online 2018-07-05"
        result = parse_fulltext_dates(resource_id=2, raw_text=text)
        assert result.received         == date(2018, 1, 10)
        assert result.published_online == date(2018, 7, 5)

    def test_month_year_only(self):
        text   = "Received: March 2019; Accepted: June 2019"
        result = parse_fulltext_dates(resource_id=3, raw_text=text)
        assert result.received == date(2019, 3, 1)
        assert result.accepted == date(2019, 6, 1)

    def test_year_only_needs_llm(self):
        text   = "2018"
        result = parse_fulltext_dates(resource_id=4, raw_text=text)
        assert result.needs_llm is True
        assert result.status == "year_only"

    def test_empty_text(self):
        result = parse_fulltext_dates(resource_id=5, raw_text="")
        assert result.status == "empty"
        assert result.needs_llm is False

    def test_no_labels_needs_llm(self):
        text   = "June 2018"
        result = parse_fulltext_dates(resource_id=6, raw_text=text)
        assert result.needs_llm is True
        assert result.status == "no_labels"

    def test_full_pipeline(self):
        text = (
            "Received: 10 January 2017; "
            "Received in revised form: 5 March 2017; "
            "Accepted: 20 April 2017; "
            "Published online: 1 May 2017; "
            "Published: 15 June 2017"
        )
        result = parse_fulltext_dates(resource_id=7, raw_text=text)
        assert result.received         == date(2017, 1, 10)
        assert result.reviewed         == date(2017, 3, 5)
        assert result.accepted         == date(2017, 4, 20)
        assert result.published_online == date(2017, 5, 1)
        assert result.published        == date(2017, 6, 15)
        assert result.needs_llm is False

    def test_chrono_error_triggers_llm(self):
        # accepted PRED received → chyba
        text   = "Received: 20 June 2018; Accepted: 10 January 2018"
        result = parse_fulltext_dates(resource_id=8, raw_text=text)
        assert result.needs_llm is True
        assert "chrono_warnings" in result.flags
        assert any("ERROR" in w for w in result.flags["chrono_warnings"])

    def test_placeholder_needs_llm(self):
        text   = "Received: 00th; Accepted: 20 June 2018"
        result = parse_fulltext_dates(resource_id=9, raw_text=text)
        assert result.needs_llm is True
        assert "placeholder_dates" in result.flags

    def test_dot_dmy_format(self):
        text   = "Received: 15.03.2018; Accepted: 20.06.2018"
        result = parse_fulltext_dates(resource_id=10, raw_text=text)
        assert result.received == date(2018, 3, 15)
        assert result.accepted == date(2018, 6, 20)

    def test_czech_variant(self):
        text   = "Do redakce došlo: 10. 1. 2019; Přijato do tisku: 5. 3. 2019"
        result = parse_fulltext_dates(resource_id=11, raw_text=text)
        assert result.received == date(2019, 1, 10)
        assert result.accepted == date(2019, 3, 5)
