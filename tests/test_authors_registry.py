from src.authors.registry import InternalAuthor, match_author


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
