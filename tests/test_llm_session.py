"""Testy pre src/llm/client.py (LLMSession) a src/llm/prompts/dates.py"""

import json
import pytest
from datetime import date
from unittest.mock import MagicMock, patch

from src.llm.client import LLMSession, OllamaClient, CloudLLMCompatibleClient, parse_llm_json_output
from src.llm.prompts.dates import DateLLMResult
from src.llm.prompts.authors import AUTHORS_JSON_SCHEMA, SYSTEM_PROMPT, AUTHORS_SETUP_PREAMBLE, LLMResult


# -----------------------------------------------------------------------
# parse_llm_json_output
# -----------------------------------------------------------------------

class TestParseLlmJsonOutput:
    def test_clean_json(self):
        raw = '{"internal_authors": []}'
        assert parse_llm_json_output(raw) == {"internal_authors": []}

    def test_with_markdown_backticks(self):
        raw = '```json\n{"internal_authors": []}\n```'
        assert parse_llm_json_output(raw) == {"internal_authors": []}

    def test_with_preamble_text(self):
        raw = 'Tu je odpoveď: {"internal_authors": [{"name": "Test"}]}'
        result = parse_llm_json_output(raw)
        assert result["internal_authors"][0]["name"] == "Test"

    def test_nested_json(self):
        raw = '{"received": "2018-01-01", "accepted": ""}'
        result = parse_llm_json_output(raw)
        assert result["received"] == "2018-01-01"


# -----------------------------------------------------------------------
# LLMSession
# -----------------------------------------------------------------------

class FakeClient:
    """Fake klient pre testovanie session."""

    def __init__(self, response: str = '{"internal_authors": []}'):
        self.response       = response
        self.last_system    = None
        self.last_user      = None
        self.last_schema    = None
        self.last_preamble  = None

    def complete(self, system_prompt, user_message, *, json_schema=None, preamble=None):
        self.last_system   = system_prompt
        self.last_user     = user_message
        self.last_schema   = json_schema
        self.last_preamble = preamble
        return self.response


class TestLLMSession:
    def test_ask_calls_complete(self):
        client  = FakeClient()
        session = LLMSession(client, "system", {"type": "object"})
        result  = session.ask("user message")
        assert result == '{"internal_authors": []}'
        assert client.last_system == "system"
        assert client.last_user   == "user message"

    def test_json_schema_passed(self):
        schema  = {"type": "object", "properties": {}}
        client  = FakeClient()
        session = LLMSession(client, "sys", schema)
        session.ask("test")
        assert client.last_schema == schema

    def test_preamble_not_used_for_non_ollama(self):
        """Pre non-Ollama klienta sa preamble ignoruje."""
        client  = FakeClient()
        session = LLMSession(client, "sys", {}, preamble=[{"role": "user", "content": "setup"}])
        session.ask("test")
        assert client.last_preamble is None

    def test_preamble_used_for_ollama(self):
        """Pre Ollama klienta sa preamble použije."""
        preamble = [{"role": "user", "content": "setup"}, {"role": "assistant", "content": "ok"}]

        with patch.object(OllamaClient, "complete", return_value='{"internal_authors": []}') as mock_complete:
            ollama  = OllamaClient.__new__(OllamaClient)
            ollama.base_url = "http://localhost:11434"
            ollama.model    = "test-model"
            ollama.timeout  = 30

            session = LLMSession(ollama, "sys", {}, preamble=preamble)
            session.ask("user")

            args, kwargs = mock_complete.call_args
            assert kwargs.get("preamble") == preamble

    def test_multiple_calls_independent(self):
        """Každé volanie ask() je nezávislé – história sa neakumuluje."""
        responses = ['{"received": "2018-01-01"}', '{"received": "2019-06-15"}']
        call_count = [0]

        class CountingClient:
            def complete(self, sp, um, *, json_schema=None, preamble=None):
                r = responses[call_count[0]]
                call_count[0] += 1
                return r

        session = LLMSession(CountingClient(), "sys", {})
        r1 = session.ask("record 1")
        r2 = session.ask("record 2")
        assert json.loads(r1)["received"] == "2018-01-01"
        assert json.loads(r2)["received"] == "2019-06-15"


# -----------------------------------------------------------------------
# DateLLMResult
# -----------------------------------------------------------------------

class TestDateLLMResult:
    def test_valid_iso_date(self):
        r = DateLLMResult(received="2018-03-15")
        assert r.received == "2018-03-15"

    def test_empty_string_stays_empty(self):
        r = DateLLMResult(received="")
        assert r.received == ""

    def test_invalid_date_becomes_empty(self):
        r = DateLLMResult(received="not-a-date")
        assert r.received == ""

    def test_invalid_date_wrong_format(self):
        r = DateLLMResult(received="15.03.2018")
        assert r.received == ""

    def test_to_date_valid(self):
        r = DateLLMResult(accepted="2018-06-20")
        assert r.to_date("accepted") == date(2018, 6, 20)

    def test_to_date_empty(self):
        r = DateLLMResult(accepted="")
        assert r.to_date("accepted") is None

    def test_all_fields_default_empty(self):
        r = DateLLMResult()
        for field in ("received", "reviewed", "accepted", "published_online", "published"):
            assert getattr(r, field) == ""
            assert r.to_date(field) is None

    def test_full_result(self):
        r = DateLLMResult(
            received         = "2018-01-10",
            reviewed         = "2018-03-01",
            accepted         = "2018-06-15",
            published_online = "2018-07-01",
            published        = "2018-09-01",
        )
        assert r.to_date("received")         == date(2018, 1, 10)
        assert r.to_date("reviewed")         == date(2018, 3, 1)
        assert r.to_date("accepted")         == date(2018, 6, 15)
        assert r.to_date("published_online") == date(2018, 7, 1)
        assert r.to_date("published")        == date(2018, 9, 1)

    def test_extra_fields_forbidden(self):
        from pydantic import ValidationError
        with pytest.raises(ValidationError):
            DateLLMResult(received="2018-01-01", unknown_field="x")


# -----------------------------------------------------------------------
# LLMResult (authors)
# -----------------------------------------------------------------------

class TestLLMResult:
    def test_valid_author(self):
        r = LLMResult(internal_authors=[
            {"name": "Novák, Jan", "faculty": "Faculty of Technology", "ou": ""}
        ])
        assert len(r.internal_authors) == 1
        assert r.internal_authors[0].name == "Novák, Jan"

    def test_invalid_faculty_cleared(self):
        r = LLMResult(internal_authors=[
            {"name": "Test, Name", "faculty": "Invalid Faculty XYZ", "ou": ""}
        ])
        assert r.internal_authors[0].faculty == ""

    def test_empty_authors(self):
        r = LLMResult(internal_authors=[])
        assert r.internal_authors == []

    def test_model_dump(self):
        r = LLMResult(internal_authors=[
            {"name": "Test, Name", "faculty": "", "ou": ""}
        ])
        d = r.model_dump()
        assert "internal_authors" in d
        assert d["internal_authors"][0]["name"] == "Test, Name"
