from collections import namedtuple
from unittest.mock import MagicMock

from src.authors.registry import (
    InternalAuthor,
    clear_author_registry_cache,
    get_author_registry,
    match_author,
)


def test_matches_reversed_name_without_diacritics():
    registry = [InternalAuthor(surname="Sedlařík", firstname="Vladimír")]

    result = match_author("Vladimir Sedlarik", registry)

    assert result.matched is True
    assert result.author == registry[0]
    assert result.match_type == "exact_normalized"


def test_matches_wos_surname_initial_form():
    registry = [InternalAuthor(surname="Sedlařík", firstname="Vladimír")]

    result = match_author("Sedlarik V", registry)

    assert result.matched is True
    assert result.author == registry[0]
    assert result.match_type == "initial_surname"


def test_initial_match_must_be_unique():
    registry = [
        InternalAuthor(surname="Novák", firstname="Jan"),
        InternalAuthor(surname="Novák", firstname="Jiří"),
    ]

    result = match_author("Novak J", registry)

    assert result.matched is False


def test_surname_only_match_must_be_unique():
    registry = [
        InternalAuthor(surname="Novák", firstname="Jan"),
        InternalAuthor(surname="Novák", firstname="Jiří"),
    ]

    result = match_author("Novak", registry)

    assert result.matched is False
    assert result.match_type == "ambiguous_surname"


def test_matches_compound_surname_alias_from_limited_registry():
    registry = [
        InternalAuthor(
            surname="Vávra",
            firstname="Jarmila",
            aliases=(
                "Ambrožová, Jarmila",
                "Vávra Ambrožová, Jarmila",
                "Ambrožová Vávra, Jarmila",
            ),
        )
    ]

    result = match_author("Vávra Ambrožová, Jarmila", registry)

    assert result.matched is True
    assert result.author == registry[0]


def test_fuzzy_match_rejects_different_initial_same_surname():
    registry = [
        InternalAuthor(surname="Danko", firstname="Lukáš"),
        InternalAuthor(surname="Danko", firstname="Martin"),
    ]

    result = match_author("Danko, Martin", [registry[0]], require_surname_match=True)

    assert result.matched is False
    assert result.match_type == "initial_mismatch"


def test_get_author_registry_reads_remote_utb_authors_limited():
    clear_author_registry_cache()
    Row = namedtuple("Row", ["author_id", "display_name", "surname", "given_name"])

    conn = MagicMock()
    conn.__enter__ = lambda s: conn
    conn.__exit__ = MagicMock(return_value=False)
    conn.execute.return_value.fetchall.return_value = [
        Row(1, "Novak, Jan", "Novak", "Jan"),
        Row(1, "Novak, Jan", "Novak", "Jan"),
        Row(2, "Sedlarik, Vladimir||Sedlarik V", "Sedlarik", "Vladimir"),
    ]
    engine = MagicMock()
    engine.connect.return_value = conn

    registry = get_author_registry(remote_engine=engine)

    assert [author.full_name for author in registry] == [
        "Novak, Jan",
        "Sedlarik, Vladimir",
    ]
    clear_author_registry_cache()
