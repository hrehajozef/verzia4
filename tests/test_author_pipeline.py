from src.authors.heuristics import process_record
from src.authors.registry import InternalAuthor, _extract_person_data
from src.authors.source_authors import merge_author_lists, split_source_author_lists
from web.services.queue_service import _author_llm_proposed


def test_extract_person_data_prefers_parent_cps_and_university_institute():
    faculties, ou = _extract_person_data([
        ("Vyzkum CPS", "Centrum polymernich systemu", "Univerzitni institut"),
    ])

    assert faculties == ("University Institute",)
    assert ou == "Centre of Polymer Systems"


def test_process_record_uses_merged_source_authors_and_keeps_alignment(monkeypatch):
    registry = [
        InternalAuthor(surname="Danko", firstname="Martin"),
        InternalAuthor(surname="Humpolicek", firstname="Petr"),
        InternalAuthor(surname="Lehocky", firstname="Marian"),
        InternalAuthor(surname="Kovacova", firstname="Klara"),
    ]

    def fake_lookup(surname: str, firstname: str, remote_engine=None):
        mapping = {
            ("Danko", "Martin"): ((), ""),
            ("Humpolicek", "Petr"): (("Faculty of Technology", "University Institute"), "Department of Physics and Materials Engineering"),
            ("Lehocky", "Marian"): (("Faculty of Technology", "University Institute"), "Department of Polymer Engineering"),
            ("Kovacova", "Klara"): (("Faculty of Humanities",), "Language Centre"),
        }
        return mapping[(surname, firstname)]

    monkeypatch.setattr("src.authors.heuristics.lookup_author_affiliations", fake_lookup)

    result = process_record(
        resource_id=10058,
        wos_aff_arr=None,
        wos_author_arr=["Danko M", "Humpolicek P"],
        scopus_author_arr=["Danko, Martin", "Humpolicek, Petr", "Lehocky, Marian"],
        scopus_aff_arr=[
            "Centre of Polymer Systems, Tomas Bata University in Zlin, Trida Tomase Bati, Zlin, Czech Republic"
        ],
        fulltext_aff_arr=[
            "3 Centre of Polymer Systems, Tomas Bata University in Zlin, Trida Tomase Bati, Zlin, Czech Republic"
        ],
        dc_authors_arr=[
            "Danko, Martin",
            "Humpolicek, Petr",
            "Lehocky, Marian",
            "Kovacova, Maria",
        ],
        registry=registry,
        remote_engine=None,
    )

    assert result["author_dc_names"] == [
        "Danko, Martin",
        "Humpolicek, Petr",
        "Lehocky, Marian",
        "Kovacova, Maria",
    ]
    assert result["author_internal_names"] == [
        "Danko, Martin",
        "Humpolicek, Petr",
        "Lehocky, Marian",
    ]
    assert result["author_faculty"] == [
        "University Institute",
        "University Institute",
        "University Institute",
    ]
    assert result["author_ou"] == [
        "Centre of Polymer Systems",
        "Centre of Polymer Systems",
        "Centre of Polymer Systems",
    ]
    assert result["author_flags"]["utb_authors_found_count"] == 3
    assert "path_b_low_confidence_matches" not in result["author_flags"]


def test_merge_author_lists_prefers_richer_variant():
    assert merge_author_lists(
        ["Grant B", "Pfleger, Jiri"],
        ["Grant, Benjamin", "Pfleger, Jiri"],
    ) == [
        "Grant, Benjamin",
        "Pfleger, Jiri",
    ]


def test_split_source_author_lists_uses_history_rows():
    split = split_source_author_lists(
        current_authors=["Grant, Benjamin", "Pfleger, Jiri"],
        current_sources=["j-wok", "j-scopus"],
        history_rows=[
            {
                "sources": ["j-wok"],
                "authors": ["Grant B", "Pfleger, Jiri"],
            },
            {
                "sources": ["j-scopus"],
                "authors": ["Grant, Benjamin", "Pfleger, Jiri"],
            },
        ],
    )

    assert split["wos"] == ["Grant B", "Pfleger, Jiri"]
    assert split["scopus"] == ["Grant, Benjamin", "Pfleger, Jiri"]
    assert split["repo"] == ["Grant, Benjamin", "Pfleger, Jiri"]


def test_author_llm_proposed_preserves_repeated_faculties():
    queue_data = {
        "author_llm_status": "processed",
        "author_llm_result": {
            "internal_authors": [
                {
                    "name": "Novak, Jan",
                    "faculty": "Faculty of Management and Economics",
                    "ou": "Department of Economics",
                },
                {
                    "name": "Kovacova, Petra",
                    "faculty": "Faculty of Management and Economics",
                    "ou": "Department of Economics",
                },
                {
                    "name": "Vasko, Milan",
                    "faculty": "Faculty of Technology",
                    "ou": "Department of Polymer Engineering",
                },
            ]
        },
    }

    assert _author_llm_proposed(queue_data, "utb.faculty") == (
        "Faculty of Management and Economics||"
        "Faculty of Management and Economics||"
        "Faculty of Technology"
    )


def test_process_record_keeps_internal_author_as_subset_of_repo_authors(monkeypatch):
    registry = [
        InternalAuthor(
            surname="Vávra",
            firstname="Jarmila",
            aliases=(
                "Ambrožová, Jarmila",
                "Vávra Ambrožová, Jarmila",
                "Ambrožová Vávra, Jarmila",
            ),
            limited_author_id=10,
        ),
        InternalAuthor(
            surname="Samek",
            firstname="Dušan",
            aliases=("Samek, Dušan",),
            limited_author_id=11,
        ),
    ]

    def fake_lookup(surname: str, firstname: str, remote_engine=None):
        return (("Faculty of Technology",), "Department of Food Analysis and Chemistry")

    monkeypatch.setattr("src.authors.heuristics.lookup_author_affiliations", fake_lookup)

    result = process_record(
        resource_id=5271,
        wos_aff_arr=None,
        wos_author_arr=None,
        scopus_author_arr=None,
        scopus_aff_arr=None,
        fulltext_aff_arr=None,
        dc_authors_arr=[
            "Vávra Ambrožová, Jarmila",
            "Samek, Dušan",
        ],
        registry=registry,
        remote_engine=None,
    )

    assert result["author_internal_names"] == [
        "Vávra Ambrožová, Jarmila",
        "Samek, Dušan",
    ]
    assert result["author_internal_names"] == result["author_dc_names"]
