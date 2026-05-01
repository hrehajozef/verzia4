from src.authors.heuristics import process_record
from src.authors.registry import InternalAuthor, _extract_person_data
from src.authors.source_authors import merge_author_lists, split_source_author_lists
from src.authors.workplace_tree import WorkplaceNode
from web.services.queue_service import _author_llm_proposed


def _test_workplace_tree():
    return {
        1: WorkplaceNode(1, "UTB", "Univerzita", "Tomas Bata University in Zlin", (), None, False),
        10: WorkplaceNode(10, "UI", "Univerzitni institut", "University Institute", ("UI",), 1, False),
        11: WorkplaceNode(11, "CPS", "Centrum polymernich systemu", "Centre of Polymer Systems", ("CPS",), 10, True),
        20: WorkplaceNode(20, "FT", "Fakulta technologicka", "Faculty of Technology", ("FT",), 1, False),
        21: WorkplaceNode(
            21,
            "DPME",
            "Ustav fyziky a materialoveho inzenyrstvi",
            "Department of Physics and Materials Engineering",
            ("DPME",),
            20,
            True,
        ),
        22: WorkplaceNode(
            22,
            "DPE",
            "Ustav inzenyrstvi polymeru",
            "Department of Polymer Engineering",
            ("DPE",),
            20,
            True,
        ),
        30: WorkplaceNode(30, "FAME", "Fakulta managementu a ekonomiky", "Faculty of Management and Economics", ("FaME",), 1, False),
        31: WorkplaceNode(
            31,
            "UPIIIS",
            "Ustav prumysloveho inzenyrstvi a IS",
            "Department of Industrial Engineering and Information Systems",
            ("UPIIIS",),
            30,
            True,
        ),
        40: WorkplaceNode(40, "FHS", "Fakulta humanitnich studii", "Faculty of Humanities", ("FHS",), 1, False),
        41: WorkplaceNode(41, "CJV", "Centrum jazykoveho vzdelavani", "Language Centre", ("CJV",), 40, True),
    }


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
            "Danko M., Centre of Polymer Systems, Tomas Bata University in Zlin, Trida Tomase Bati, Zlin, Czech Republic"
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
        workplace_tree=_test_workplace_tree(),
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
        "Faculty of Technology",
        "Faculty of Technology",
    ]
    assert result["author_ou"] == [
        "Centre of Polymer Systems",
        "Department of Physics and Materials Engineering",
        "Department of Polymer Engineering",
    ]
    assert result["author_flags"]["utb_authors_found_count"] == 3


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
            surname="Vavra",
            firstname="Jarmila",
            aliases=(
                "Ambrozova, Jarmila",
                "Vavra Ambrozova, Jarmila",
                "Ambrozova Vavra, Jarmila",
            ),
            limited_author_id=10,
        ),
        InternalAuthor(
            surname="Samek",
            firstname="Dusan",
            aliases=("Samek, Dusan",),
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
            "Vavra Ambrozova, Jarmila",
            "Samek, Dusan",
        ],
        registry=registry,
        remote_engine=None,
    )

    assert result["author_internal_names"] == [
        "Vavra Ambrozova, Jarmila",
        "Samek, Dusan",
    ]
    assert result["author_internal_names"] == result["author_dc_names"]


def test_process_record_prefers_scopus_affiliation_author_names(monkeypatch):
    registry = [
        InternalAuthor(surname="Montag", firstname="Pavel", limited_author_id=21),
        InternalAuthor(surname="Kalendova", firstname="Alena", limited_author_id=22),
    ]

    def fake_lookup(surname: str, firstname: str, remote_engine=None):
        mapping = {
            ("Montag", "Pavel"): (("Faculty of Management and Economics",), ""),
            ("Kalendova", "Alena"): (("Faculty of Technology",), ""),
        }
        return mapping[(surname, firstname)]

    monkeypatch.setattr("src.authors.heuristics.lookup_author_affiliations", fake_lookup)

    result = process_record(
        resource_id=999,
        wos_aff_arr=None,
        wos_author_arr=None,
        scopus_author_arr=None,
        scopus_aff_arr=[
            "Montag P., Tomas Bata University in Zlin, Faculty of Management and Economics, Zlin, Czech Republic;"
            " Kalendova A., Tomas Bata University in Zlin, Faculty of Technology, Zlin, Czech Republic"
        ],
        fulltext_aff_arr=None,
        dc_authors_arr=[
            "Montag, Pavel",
            "Kalendova, Alena",
        ],
        registry=registry,
        remote_engine=None,
        workplace_tree=_test_workplace_tree(),
    )

    assert result["author_dc_names"] == [
        "Montag, Pavel",
        "Kalendova, Alena",
    ]
    assert result["author_internal_names"] == [
        "Montag, Pavel",
        "Kalendova, Alena",
    ]
    assert result["author_faculty"] == [
        "Faculty of Management and Economics",
        "Faculty of Technology",
    ]


def test_process_record_does_not_apply_one_author_hint_to_all_internal_authors(monkeypatch):
    registry = [
        InternalAuthor(surname="Prvy", firstname="Autor"),
        InternalAuthor(surname="Druhy", firstname="Autor"),
        InternalAuthor(surname="Treti", firstname="Autor"),
    ]

    def fake_lookup(surname: str, firstname: str, remote_engine=None):
        mapping = {
            ("Prvy", "Autor"): (("University Institute",), "Centre of Polymer Systems"),
            ("Druhy", "Autor"): (("Faculty of Technology", "University Institute"), "Department of Polymer Engineering"),
            ("Treti", "Autor"): (("Faculty of Technology", "University Institute"), "Department of Physics and Materials Engineering"),
        }
        return mapping[(surname, firstname)]

    monkeypatch.setattr("src.authors.heuristics.lookup_author_affiliations", fake_lookup)

    result = process_record(
        resource_id=11137,
        wos_aff_arr=None,
        wos_author_arr=None,
        scopus_author_arr=None,
        scopus_aff_arr=[
            "Prvy A., Centre of Polymer Systems, Tomas Bata University in Zlin, Zlin, Czech Republic"
        ],
        fulltext_aff_arr=[
            "1 Centre of Polymer Systems, Tomas Bata University in Zlin, Zlin, Czech Republic"
        ],
        dc_authors_arr=[
            "Prvy, Autor",
            "Druhy, Autor",
            "Treti, Autor",
        ],
        registry=registry,
        remote_engine=None,
        workplace_tree=_test_workplace_tree(),
    )

    assert result["author_internal_names"] == [
        "Prvy, Autor",
        "Druhy, Autor",
        "Treti, Autor",
    ]
    assert result["author_faculty"] == [
        "University Institute",
        "Faculty of Technology",
        "Faculty of Technology",
    ]
    assert result["author_ou"] == [
        "Centre of Polymer Systems",
        "Department of Polymer Engineering",
        "Department of Physics and Materials Engineering",
    ]


def test_process_record_builds_verified_attribution_for_golden_record_5305():
    registry = [
        InternalAuthor(
            surname="Burita",
            firstname="Lukas",
            limited_author_id=5305,
            faculty="Faculty of Management and Economics",
            organization_id=31,
        )
    ]

    result = process_record(
        resource_id=5305,
        wos_aff_arr=None,
        wos_author_arr=None,
        scopus_author_arr=["Burita, Lukas"],
        scopus_aff_arr=[
            "Burita L., Tomas Bata University in Zlin, Faculty of Management and Economics, "
            "Department of Industrial Engineering and Information Systems, Zlin, Czech Republic"
        ],
        fulltext_aff_arr=None,
        dc_authors_arr=["Burita, Lukas"],
        registry=registry,
        remote_engine=None,
        workplace_tree=_test_workplace_tree(),
    )

    assert result["author_internal_names"] == ["Burita, Lukas"]
    assert result["author_faculty"] == ["Faculty of Management and Economics"]
    assert result["author_ou"] == ["Department of Industrial Engineering and Information Systems"]
    assert result["author_flags"]["attributions"][0]["per_paper_source"] == "scopus_verified"
    assert (
        result["author_flags"]["attributions"][0]["default_ou"]
        == "Department of Industrial Engineering and Information Systems"
    )
