from collections import namedtuple
from unittest.mock import MagicMock

from src.authors.workplace_tree import (
    find_workplace_by_name,
    load_workplace_tree,
    walk_to_faculty,
)


def _mock_engine(rows):
    conn = MagicMock()
    conn.__enter__ = lambda s: conn
    conn.__exit__ = MagicMock(return_value=False)
    conn.execute.return_value.fetchall.return_value = rows
    engine = MagicMock()
    engine.connect.return_value = conn
    return engine


def test_walk_to_faculty_returns_university_institute_for_cps():
    Row = namedtuple("Row", "id kodprac nazev nazev_eng zkratka zkr id_nadrizene je_katedra")
    engine = _mock_engine([
        Row(1, "UTB", "Univerzita Tomase Bati ve Zline", "Tomas Bata University in Zlin", None, None, None, "N"),
        Row(2, "UI", "Univerzitni institut", "University Institute", "UI", None, 1, "N"),
        Row(3, "CPS", "Centrum polymernich systemu", "Centre of Polymer Systems", "CPS", None, 2, "A"),
    ])

    tree = load_workplace_tree(remote_engine=engine)
    faculty = walk_to_faculty(3, tree)

    assert faculty is not None
    assert faculty.id == 2
    assert faculty.name_en == "University Institute"


def test_find_workplace_by_name_matches_cps_exactly():
    Row = namedtuple("Row", "id kodprac nazev nazev_eng zkratka zkr id_nadrizene je_katedra")
    engine = _mock_engine([
        Row(2, "UI", "Univerzitni institut", "University Institute", "UI", None, 1, "N"),
        Row(3, "CPS", "Centrum polymernich systemu", "Centre of Polymer Systems", "CPS", None, 2, "A"),
    ])

    tree = load_workplace_tree(remote_engine=engine)
    node, score = find_workplace_by_name("Centre of Polymer Systems", tree)

    assert node is not None
    assert node.id == 3
    assert score == 1.0


def test_find_workplace_by_name_matches_american_spelling():
    Row = namedtuple("Row", "id kodprac nazev nazev_eng zkratka zkr id_nadrizene je_katedra")
    engine = _mock_engine([
        Row(3, "CPS", "Centrum polymernich systemu", "Centre of Polymer Systems", "CPS", None, 2, "A"),
    ])

    tree = load_workplace_tree(remote_engine=engine)
    node, score = find_workplace_by_name("Center of Polymer Systems", tree)

    assert node is not None
    assert node.id == 3
    assert score >= 0.92


def test_find_workplace_by_name_returns_none_for_random_department():
    Row = namedtuple("Row", "id kodprac nazev nazev_eng zkratka zkr id_nadrizene je_katedra")
    engine = _mock_engine([
        Row(3, "CPS", "Centrum polymernich systemu", "Centre of Polymer Systems", "CPS", None, 2, "A"),
    ])

    tree = load_workplace_tree(remote_engine=engine)
    node, score = find_workplace_by_name("Random Department", tree)

    assert node is None
    assert score == 0.0


def test_load_workplace_tree_translates_en_placeholder_names():
    Row = namedtuple("Row", "id kodprac nazev nazev_eng zkratka zkr id_nadrizene je_katedra")
    engine = _mock_engine([
        Row(30, "FAME", "Fakulta managementu a ekonomiky", "EN_Fakulta managementu a ekonomiky", "FaME", None, 1, "N"),
        Row(
            31,
            "UPIIIS",
            "Ústav průmyslového inženýrství a IS",
            "EN_Ústav průmyslového inženýrství a IS",
            "UPIIIS",
            None,
            30,
            "A",
        ),
    ])

    tree = load_workplace_tree(remote_engine=engine)

    assert tree[30].name_en == "Faculty of Management and Economics"
    assert tree[31].name_en == "Department of Industrial Engineering and Information Systems"
