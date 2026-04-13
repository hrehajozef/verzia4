"""Testy pre src/dates/parser.py a src/dates/labels.py"""

import pytest
from datetime import date

from src.dates.labels import DateCategory, match_label, normalize_label
from src.dates.parser import (
    parse_fulltext_dates,
    _try_parse_dot_both,
    resolve_mdr_format,
    MDR_FORMAT_DMY,
    MDR_FORMAT_MDY,
    MDR_CONF_HIGH,
    MDR_CONF_MEDIUM,
    MDR_CONF_LOW,
    MDR_CONF_INVALID,
)


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

    def test_revised_is_reviewed(self):
        assert match_label("Revised") == DateCategory.REVIEWED

    def test_resubmitted_is_reviewed(self):
        assert match_label("Resubmitted") == DateCategory.REVIEWED

    def test_prepracovano_is_reviewed(self):
        assert match_label("Prepracováno") == DateCategory.REVIEWED

    def test_revision_is_reviewed(self):
        result = parse_fulltext_dates(resource_id=12, raw_text="1st Revision: 10 March 2020")
        assert result.reviewed == date(2020, 3, 10)
        assert result.flags.get("extra_dates") is None

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


# -----------------------------------------------------------------------
# _try_parse_dot_both
# -----------------------------------------------------------------------

class TestTryParseDotBoth:
    def test_unambiguous_day_gt_12_dmy(self):
        # 28.10.2002 → DMY: 28 Oct 2002; MDY: month=28 invalid → mdy=None
        dmy, mdy = _try_parse_dot_both("28.10.2002")
        assert dmy == date(2002, 10, 28)
        assert mdy is None   # month 28 neexistuje

    def test_unambiguous_mdy_day_gt_12(self):
        # 7.30.2002 → DMY: month=30 invalid → dmy=None; MDY: 30 Jul 2002
        dmy, mdy = _try_parse_dot_both("7.30.2002")
        assert dmy is None   # mesiac 30 neexistuje
        assert mdy == date(2002, 7, 30)

    def test_ambiguous_both_valid(self):
        # 7.2.2002 → DMY: 7 Feb 2002; MDY: 2 Jul 2002 – obe platné
        dmy, mdy = _try_parse_dot_both("7.2.2002")
        assert dmy == date(2002, 2, 7)
        assert mdy == date(2002, 7, 2)

    def test_ambiguous_equal_values(self):
        # 5.5.2020 → DMY a MDY sú identické
        dmy, mdy = _try_parse_dot_both("5.5.2020")
        assert dmy == date(2020, 5, 5)
        assert mdy == date(2020, 5, 5)

    def test_no_dot_date_returns_none(self):
        assert _try_parse_dot_both("15 March 2018") is None
        assert _try_parse_dot_both("2018-03-15") is None
        assert _try_parse_dot_both("March 2019") is None

    def test_year_out_of_range_returns_none(self):
        assert _try_parse_dot_both("7.2.1800") is None
        assert _try_parse_dot_both("7.2.2100") is None

    def test_spaced_dot_date(self):
        # "10. 5. 2002" – medzery medzi bodkami
        dmy, mdy = _try_parse_dot_both("10. 5. 2002")
        assert dmy == date(2002, 5, 10)
        assert mdy == date(2002, 10, 5)

    def test_invalid_date_both_none(self):
        # 30.13.2002 → month=13 invalid DMY, day=30 valid but month=13>12 invalid MDY
        result = _try_parse_dot_both("30.13.2002")
        # DMY: month=13 invalid; MDY: month=30 invalid → both None
        assert result == (None, None)


# -----------------------------------------------------------------------
# resolve_mdr_format
# -----------------------------------------------------------------------

class TestResolveMdrFormat:
    # ── HIGH confidence ─────────────────────────────────────────────────

    def test_high_dmy_forced_by_day_gt_12(self):
        # 28.10.2002: dmy valid, mdy=None → forced DMY
        dmy = date(2002, 10, 28)
        fmt, conf, flags = resolve_mdr_format(
            [(DateCategory.PUBLISHED, dmy, None)]
        )
        assert fmt  == MDR_FORMAT_DMY
        assert conf == MDR_CONF_HIGH

    def test_high_mdy_forced_by_invalid_dmy(self):
        # 7.30.2002: dmy=None, mdy valid → forced MDY
        mdy = date(2002, 7, 30)
        fmt, conf, flags = resolve_mdr_format(
            [(DateCategory.ACCEPTED, None, mdy)]
        )
        assert fmt  == MDR_FORMAT_MDY
        assert conf == MDR_CONF_HIGH

    def test_high_dmy_with_mixed_ambiguous(self):
        # Jeden forced DMY + jeden ambivalentný → HIGH DMY
        forced_dmy = date(2002, 10, 28)
        amb_dmy    = date(2002, 2, 7)
        amb_mdy    = date(2002, 7, 2)
        fmt, conf, _ = resolve_mdr_format([
            (DateCategory.PUBLISHED, forced_dmy, None),
            (DateCategory.RECEIVED,  amb_dmy,    amb_mdy),
        ])
        assert fmt  == MDR_FORMAT_DMY
        assert conf == MDR_CONF_HIGH

    # ── MEDIUM confidence ────────────────────────────────────────────────

    def test_medium_dmy_via_chronology(self):
        # Received: 7.2.2002 → dmy=7Feb, mdy=2Jul
        # Accepted: 3.5.2002 → dmy=3May, mdy=5Mar
        # DMY: 7Feb ≤ 3May ✓   MDY: 2Jul > 5Mar ✗ → MEDIUM DMY
        fmt, conf, flags = resolve_mdr_format([
            (DateCategory.RECEIVED, date(2002, 2, 7), date(2002, 7, 2)),
            (DateCategory.ACCEPTED, date(2002, 5, 3), date(2002, 3, 5)),
        ])
        assert fmt  == MDR_FORMAT_DMY
        assert conf == MDR_CONF_MEDIUM
        assert "UPOZORNENIE PRE KNIŽNÍKA" in flags["mdr_format_resolved"]["note"]

    def test_medium_mdy_via_chronology(self):
        # MDY: received < accepted; DMY: received > accepted
        # Received: 1.10.2020 → dmy=1Oct, mdy=10Jan
        # Accepted: 1.11.2020 → dmy=1Nov, mdy=11Jan
        # Wait - let me think: dmy Oct < Nov ✓, mdy Jan.10 < Jan.11 ✓ — both ok
        # Better: MDY consistent, DMY not:
        # Received: 5.3.2020 → dmy=5Mar, mdy=3May
        # Accepted: 1.4.2020 → dmy=1Apr, mdy=4Jan
        # DMY: 5Mar ≤ 1Apr ✓, MDY: 3May > 4Jan ✗ → MEDIUM DMY
        # Let's invert for MDY test:
        # Received: 3.5.2020 → dmy=3May, mdy=5Mar
        # Accepted: 4.6.2020 → dmy=4Jun, mdy=6Apr
        # DMY: 3May ≤ 4Jun ✓, MDY: 5Mar ≤ 6Apr ✓ → both ok → LOW
        # Hard to make pure MDY medium without also being DMY medium.
        # Use: Received: 10.2.2020→dmy=10Feb, mdy=2Oct; Accepted: 11.3.2020→dmy=11Mar, mdy=3Nov
        # DMY: 10Feb ≤ 11Mar ✓; MDY: 2Oct ≤ 3Nov ✓ → both ok → LOW
        # Use cases where MDY wins: dmy violates but mdy ok
        # Received: 2.10.2020→dmy=2Oct, mdy=10Feb; Accepted: 3.5.2020→dmy=3May, mdy=5Mar
        # DMY: 2Oct > 3May ✗; MDY: 10Feb ≤ 5Mar ✓ → MEDIUM MDY
        fmt, conf, flags = resolve_mdr_format([
            (DateCategory.RECEIVED, date(2020, 10, 2), date(2020,  2, 10)),
            (DateCategory.ACCEPTED, date(2020,  5, 3), date(2020,  3,  5)),
        ])
        assert fmt  == MDR_FORMAT_MDY
        assert conf == MDR_CONF_MEDIUM

    # ── LOW confidence ───────────────────────────────────────────────────

    def test_low_single_ambiguous_date(self):
        # Len jeden bodkový dátum, obe interpretácie platné
        fmt, conf, flags = resolve_mdr_format(
            [(DateCategory.PUBLISHED, date(2002, 5, 10), date(2002, 10, 5))]
        )
        assert fmt  is None
        assert conf == MDR_CONF_LOW
        assert "mdr_ambiguous" in flags
        assert "UPOZORNENIE PRE KNIŽNÍKA" in flags["mdr_ambiguous"]["note"]

    def test_low_multiple_ambiguous_both_orderings_ok(self):
        # Received: 1.3.2020→dmy=1Mar, mdy=3Jan; Published: 2.4.2020→dmy=2Apr, mdy=4Feb
        # DMY: 1Mar ≤ 2Apr ✓; MDY: 3Jan ≤ 4Feb ✓ → LOW
        fmt, conf, flags = resolve_mdr_format([
            (DateCategory.RECEIVED,  date(2020, 3, 1), date(2020, 1, 3)),
            (DateCategory.PUBLISHED, date(2020, 4, 2), date(2020, 2, 4)),
        ])
        assert fmt  is None
        assert conf == MDR_CONF_LOW
        assert "mdr_ambiguous" in flags

    # ── INVALID confidence ───────────────────────────────────────────────

    def test_invalid_no_valid_interpretations(self):
        # Obe hodnoty None → invalid
        fmt, conf, _ = resolve_mdr_format(
            [(DateCategory.PUBLISHED, None, None)]
        )
        assert conf == MDR_CONF_INVALID

    def test_invalid_conflict_forced_dmy_and_mdy(self):
        # Jeden record vynucuje DMY, druhý vynucuje MDY → konflikt
        fmt, conf, flags = resolve_mdr_format([
            (DateCategory.RECEIVED, date(2002, 10, 28), None),   # forced DMY
            (DateCategory.ACCEPTED, None, date(2002,  7, 30)),   # forced MDY
        ])
        assert conf == MDR_CONF_INVALID
        assert "mdr_format_conflict" in flags
        assert "UPOZORNENIE PRE KNIŽNÍKA" in flags["mdr_format_conflict"]["note"]

    def test_invalid_both_orderings_violate(self):
        # DMY: Received > Accepted; MDY: Received > Accepted too
        # Received: 5.8.2020→dmy=5Aug, mdy=8May; Accepted: 3.2.2020→dmy=3Feb, mdy=2Mar
        # DMY: 5Aug > 3Feb ✗; MDY: 8May > 2Mar ✗ → INVALID
        fmt, conf, flags = resolve_mdr_format([
            (DateCategory.RECEIVED, date(2020, 8, 5), date(2020, 5, 8)),
            (DateCategory.ACCEPTED, date(2020, 2, 3), date(2020, 3, 2)),
        ])
        assert conf == MDR_CONF_INVALID
        assert "mdr_chrono_error" in flags

    def test_empty_candidates(self):
        fmt, conf, flags = resolve_mdr_format([])
        assert fmt  is None
        assert conf is None
        assert flags == {}


# -----------------------------------------------------------------------
# parse_fulltext_dates – MDR integration
# -----------------------------------------------------------------------

class TestMdrIntegration:
    def test_case1_high_dmy_day_gt_12(self):
        # Published: 28.10.2002 → deň 28 > 12, jednoznačne DMY, HIGH
        text   = "Published: 28.10.2002"
        result = parse_fulltext_dates(resource_id=100, raw_text=text)
        assert result.mdr_format     == MDR_FORMAT_DMY
        assert result.mdr_confidence == MDR_CONF_HIGH
        assert result.published      == date(2002, 10, 28)
        assert result.needs_llm is False

    def test_case2_medium_dmy_via_temporal_order(self):
        # Received: 7.2.2002; Accepted: 3.5.2002
        # DMY: 7Feb ≤ 3May ✓; MDY: 2Jul > 5Mar ✗ → MEDIUM DMY
        text   = "Received: 7.2.2002; Accepted: 3.5.2002"
        result = parse_fulltext_dates(resource_id=101, raw_text=text)
        assert result.mdr_format     == MDR_FORMAT_DMY
        assert result.mdr_confidence == MDR_CONF_MEDIUM
        assert result.received       == date(2002, 2, 7)
        assert result.accepted       == date(2002, 5, 3)
        # MEDIUM → needs_llm=False, ale flag upozornenia je prítomný
        assert result.needs_llm is False
        assert "mdr_format_resolved" in result.flags

    def test_case3_low_single_ambiguous(self):
        # Published: 10.5.2002 – obe hodnoty ≤ 12, len jeden dátum → LOW
        text   = "Published: 10.5.2002"
        result = parse_fulltext_dates(resource_id=102, raw_text=text)
        assert result.mdr_confidence == MDR_CONF_LOW
        assert result.needs_llm is True
        assert "mdr_ambiguous" in result.flags

    def test_case4_high_mdy_invalid_dmy(self):
        # Accepted: 7.30.2002 → deň 30 je > 12 v MDY; DMY: mesiac 30 neexistuje → HIGH MDY
        text   = "Accepted: 7.30.2002"
        result = parse_fulltext_dates(resource_id=103, raw_text=text)
        assert result.mdr_format     == MDR_FORMAT_MDY
        assert result.mdr_confidence == MDR_CONF_HIGH
        assert result.accepted       == date(2002, 7, 30)
        assert result.needs_llm is False

    def test_no_dot_dates_no_mdr(self):
        # ISO dátumy → MDR resolver sa nespustí (žiadni kandidáti)
        text   = "Received: 2020-03-01; Accepted: 2020-06-15"
        result = parse_fulltext_dates(resource_id=104, raw_text=text)
        assert result.mdr_format     is None
        assert result.mdr_confidence is None
        assert result.needs_llm is False

    def test_invalid_mdr_sets_needs_llm(self):
        # Conflict: forced DMY + forced MDY → INVALID → needs_llm
        # Received: 28.10.2002 (forced DMY) + Accepted: 7.30.2002 (forced MDY)
        text   = "Received: 28.10.2002; Accepted: 7.30.2002"
        result = parse_fulltext_dates(resource_id=105, raw_text=text)
        assert result.mdr_confidence == MDR_CONF_INVALID
        assert result.needs_llm is True

    def test_medium_librarian_flag_content(self):
        # Skontroluj, že flag obsahuje sekvenciu dátumov pre knihovníka
        text   = "Received: 7.2.2002; Accepted: 3.5.2002"
        result = parse_fulltext_dates(resource_id=106, raw_text=text)
        flag   = result.flags.get("mdr_format_resolved", {})
        assert "dmy_sequence" in flag
        assert "mdy_would_violate" in flag
        assert flag["confidence"] == MDR_CONF_MEDIUM
