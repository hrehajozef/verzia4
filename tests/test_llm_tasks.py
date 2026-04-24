import httpx

from src.authors.registry import InternalAuthor
from src.llm.client import _compute_http_retry_delay, _parse_retry_after_seconds
from src.llm.tasks.authors import (
    LLMAuthorEntry,
    LLMResult,
    _filter_by_registry,
    _registry_identity,
    _source_author_allowlist,
)
from src.llm.tasks.dates import DateLLMResult, _sanitize_year_only_llm_result


def test_author_llm_filter_keeps_only_source_subset_and_normalizes_alias():
    registry = [
        InternalAuthor(
            surname="Danko",
            firstname="Martin",
            aliases=("Danko, Martin",),
            limited_author_id=1,
        ),
        InternalAuthor(
            surname="Danko",
            firstname="Lukáš",
            aliases=("Danko, Lukáš",),
            limited_author_id=2,
        ),
    ]
    allowed_names, allowed_map, preferred = _source_author_allowlist(["Danko, Martin"], registry)

    result = _filter_by_registry(
        LLMResult(
            internal_authors=[
                LLMAuthorEntry(name="Danko, Lukáš", faculty="Faculty of Technology", ou=""),
                LLMAuthorEntry(name="Danko, Martin", faculty="Fakulta technologická", ou=""),
            ]
        ),
        registry,
        allowed_names,
        allowed_map,
        preferred,
    )

    assert [entry.name for entry in result.internal_authors] == ["Danko, Martin"]
    assert [entry.faculty for entry in result.internal_authors] == ["Faculty of Technology"]


def test_author_llm_filter_maps_registry_alias_back_to_preferred_repo_name():
    author = InternalAuthor(
        surname="Vávra",
        firstname="Jarmila",
        aliases=("Ambrožová, Jarmila", "Vávra Ambrožová, Jarmila"),
        limited_author_id=10,
    )
    registry = [author]
    preferred_name = "Vávra Ambrožová, Jarmila"
    allowed_names = [preferred_name]
    allowed_map = {preferred_name: author}
    preferred = {_registry_identity(author): preferred_name}

    result = _filter_by_registry(
        LLMResult(
            internal_authors=[
                LLMAuthorEntry(
                    name="Ambrožová, Jarmila",
                    faculty="University Institute",
                    ou="Centre of Polymer Systems",
                )
            ]
        ),
        registry,
        allowed_names,
        allowed_map,
        preferred,
    )

    assert [entry.name for entry in result.internal_authors] == [preferred_name]


def test_year_only_llm_result_is_cleared():
    sanitized = _sanitize_year_only_llm_result(
        DateLLMResult(
            received="2024-01-01",
            reviewed="",
            accepted="",
            published_online="2024-01-01",
            published="",
        ),
        {"year_only_dates": ["2024"]},
    )

    assert sanitized.received == ""
    assert sanitized.published_online == ""


def test_retry_after_parser_accepts_numeric_seconds():
    assert _parse_retry_after_seconds("12") == 12.0


def test_http_retry_delay_prefers_retry_after_header_for_429():
    response = httpx.Response(429, headers={"Retry-After": "7"})
    assert _compute_http_retry_delay(response, attempt=1) == 7.0
