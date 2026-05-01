from collections import namedtuple
from unittest.mock import MagicMock

from src.authors.registry import (
    InternalAuthor,
    clear_author_registry_cache,
    get_author_registry,
    match_author,
)


def test_matches_reversed_name_without_diacritics():
    registry = [
        InternalAuthor(
            surname="Sedla\u0159\u00edk",
            firstname="Vladim\u00edr",
        )
    ]

    result = match_author("Vladimir Sedlarik", registry)

    assert result.matched is True
    assert result.author == registry[0]
    assert result.match_type == "exact_normalized"


def test_matches_wos_surname_initial_form():
    registry = [InternalAuthor(surname="Sedlarik", firstname="Vladimir")]

    result = match_author("Sedlarik V", registry)

    assert result.matched is True
    assert result.author == registry[0]
    assert result.match_type == "initial_surname"


def test_initial_match_must_be_unique():
    registry = [
        InternalAuthor(surname="Novak", firstname="Jan"),
        InternalAuthor(surname="Novak", firstname="Jiri"),
    ]

    result = match_author("Novak J", registry)

    assert result.matched is False


def test_surname_only_match_must_be_unique():
    registry = [
        InternalAuthor(surname="Novak", firstname="Jan"),
        InternalAuthor(surname="Novak", firstname="Jiri"),
    ]

    result = match_author("Novak", registry)

    assert result.matched is False
    assert result.match_type == "ambiguous_surname"


def test_matches_compound_surname_alias_from_registry():
    registry = [
        InternalAuthor(
            surname="Vavra",
            firstname="Jarmila",
            aliases=(
                "Ambrozova, Jarmila",
                "Vavra Ambrozova, Jarmila",
                "Ambrozova Vavra, Jarmila",
            ),
        )
    ]

    result = match_author("Vavra Ambrozova, Jarmila", registry)

    assert result.matched is True
    assert result.author == registry[0]


def test_fuzzy_match_rejects_different_initial_same_surname():
    registry = [
        InternalAuthor(surname="Danko", firstname="Lukas"),
        InternalAuthor(surname="Danko", firstname="Martin"),
    ]

    result = match_author("Danko, Martin", [registry[0]], require_surname_match=True)

    assert result.matched is False
    assert result.match_type == "initial_mismatch"


def test_matches_by_orcid_before_name_logic():
    registry = [
        InternalAuthor(
            surname="Novak",
            firstname="Jan",
            orcid="0000-0001-2345-6789",
        )
    ]

    result = match_author("0000-0001-2345-6789", registry)

    assert result.matched is True
    assert result.author == registry[0]
    assert result.score == 1.0
    assert result.match_type == "orcid"


def test_matches_by_scopus_id_before_name_logic():
    registry = [
        InternalAuthor(
            surname="Sedlarik",
            firstname="Vladimir",
            scopus_id="12345678901",
        )
    ]

    result = match_author("author id 12345678901", registry)

    assert result.matched is True
    assert result.author == registry[0]
    assert result.score == 1.0
    assert result.match_type == "scopus_id"


def test_matches_by_wos_id_before_name_logic():
    registry = [
        InternalAuthor(
            surname="Burita",
            firstname="Lukas",
            wos_id="RID-4567-2024",
        )
    ]

    result = match_author("ResearcherID RID-4567-2024", registry)

    assert result.matched is True
    assert result.author == registry[0]
    assert result.score == 1.0
    assert result.match_type == "wos_id"


def test_get_author_registry_reads_remote_utb_authors():
    clear_author_registry_cache()
    Row = namedtuple(
        "Row",
        [
            "poradie",
            "author_id",
            "utbid",
            "display_name",
            "surname",
            "given_name",
            "middle_name",
            "other_name",
            "scopusid",
            "researcherid",
            "wos_id",
            "orcid",
            "obd_id",
            "organization_id",
            "faculty",
        ],
    )

    conn = MagicMock()
    conn.__enter__ = lambda s: conn
    conn.__exit__ = MagicMock(return_value=False)
    conn.execute.return_value.fetchall.return_value = [
        Row(
            1,
            1,
            "UTB-1",
            "Novak, Jan",
            "Novak",
            "Jan",
            "",
            None,
            None,
            None,
            None,
            None,
            "OBD-1",
            101,
            "Faculty of Technology",
        ),
        Row(
            1,
            1,
            "UTB-1",
            "Novak, Jan",
            "Novak",
            "Jan",
            "",
            None,
            None,
            None,
            None,
            None,
            "OBD-1",
            101,
            "Faculty of Technology",
        ),
        Row(
            2,
            2,
            "UTB-2",
            "Sedlarik, Vladimir||Sedlarik V",
            "Sedlarik",
            "Vladimir",
            "Josef",
            "Sedlarik, Vladimir",
            "12345678901",
            "RID-123",
            "WOS-ALT",
            "0000-0001-2345-6789",
            "OBD-2",
            202,
            "University Institute",
        ),
    ]
    engine = MagicMock()
    engine.connect.return_value = conn

    registry = get_author_registry(remote_engine=engine)

    assert [author.full_name for author in registry] == [
        "Novak, Jan",
        "Sedlarik, Vladimir",
    ]
    assert registry[1].middle_name == "Josef"
    assert registry[1].utb_id == "UTB-2"
    assert registry[1].display_name == "Sedlarik, Vladimir"
    assert registry[1].scopus_id == "12345678901"
    assert registry[1].wos_id == "RID-123"
    assert registry[1].orcid == "0000-0001-2345-6789"
    assert registry[1].obd_id == "OBD-2"
    assert registry[1].organization_id == 202
    assert registry[1].faculty == "University Institute"
    assert registry[1].aliases == (
        "Sedlarik, Vladimir",
        "Sedlarik V",
    )
    clear_author_registry_cache()
