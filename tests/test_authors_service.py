from unittest.mock import MagicMock

import pytest

from web.services import authors_service


def test_get_author_editor_config_marks_faculty_as_select(monkeypatch):
    monkeypatch.setattr(
        authors_service,
        "_fetch_author_columns",
        lambda engine=None: [
            {"name": "display_name", "kind": "text", "options": []},
            {
                "name": "faculty",
                "kind": "select",
                "options": ["Faculty of Technology"],
            },
        ],
    )
    monkeypatch.setattr(
        authors_service,
        "_fetch_faculty_options",
        lambda engine=None: ["Faculty of Technology"],
    )
    monkeypatch.setattr(authors_service, "can_write_authors", lambda engine=None: True)

    config = authors_service.get_author_editor_config(engine=MagicMock())

    faculty = next(column for column in config["columns"] if column["name"] == "faculty")
    assert faculty["kind"] == "select"
    assert faculty["options"] == ["Faculty of Technology"]
    assert config["faculty_options"] == ["Faculty of Technology"]
    assert config["can_write"] is True


def test_search_authors_is_diacritic_insensitive(monkeypatch):
    monkeypatch.setattr(
        authors_service,
        "_fetch_author_rows",
        lambda engine=None: [
            {
                "row_ref": "(0,1)",
                "author_id": 1,
                "display_name": "Ambrožová, Jarmila",
                "surname": "Ambrožová",
                "given_name": "Jarmila",
                "utbid": "123",
                "orcid": None,
                "faculty": "Faculty of Technology||Faculty of Humanities",
                "email": None,
                "public_email": None,
            }
        ],
    )

    authors = authors_service.search_authors("ambrozova", engine=MagicMock())

    assert len(authors) == 1
    assert authors[0]["primary"] == "Ambrožová, Jarmila"
    assert authors[0]["faculties"] == ["Faculty of Technology", "Faculty of Humanities"]


def test_create_author_normalizes_payload_and_sets_utb_default(monkeypatch):
    monkeypatch.setattr(
        authors_service,
        "_fetch_author_columns",
        lambda engine=None: [
            {"name": "poradie"},
            {"name": "author_id"},
            {"name": "display_name"},
            {"name": "utb"},
            {"name": "email"},
            {"name": "organization_id"},
            {"name": "faculty"},
        ],
    )
    monkeypatch.setattr(
        authors_service,
        "_fetch_faculty_options",
        lambda engine=None: ["Faculty of Technology"],
    )
    monkeypatch.setattr(
        authors_service,
        "get_author",
        lambda row_ref, engine=None: {
            "row_ref": row_ref,
            "author_id": 991,
            "display_name": "Test, Autor",
            "utb": "ano",
        },
    )

    begin_conn = MagicMock()
    begin_conn.__enter__ = lambda s: begin_conn
    begin_conn.__exit__ = MagicMock(return_value=False)

    select_result = MagicMock()
    select_result.scalar.return_value = "(0,1)"
    read_conn = MagicMock()
    read_conn.__enter__ = lambda s: read_conn
    read_conn.__exit__ = MagicMock(return_value=False)
    read_conn.execute.return_value = select_result

    engine = MagicMock()
    engine.begin.return_value = begin_conn
    engine.connect.return_value = read_conn

    author = authors_service.create_author(
        {
            "poradie": "12",
            "author_id": "991",
            "display_name": "Test, Autor",
            "email": "",
            "organization_id": "30160",
            "faculty": "Faculty of Technology",
        },
        engine=engine,
    )

    params = begin_conn.execute.call_args.args[1]
    assert params["poradie"] == 12
    assert params["author_id"] == 991
    assert params["utb"] == "ano"
    assert params["email"] is None
    assert params["organization_id"] == 30160
    assert params["faculty"] == "Faculty of Technology"
    assert author["author_id"] == 991
    assert author["row_ref"] == "(0,1)"


def test_update_author_uses_row_ref_and_validates_types(monkeypatch):
    monkeypatch.setattr(
        authors_service,
        "_fetch_author_columns",
        lambda engine=None: [
            {"name": "poradie"},
            {"name": "author_id"},
            {"name": "display_name"},
            {"name": "organization_id"},
            {"name": "faculty"},
        ],
    )
    monkeypatch.setattr(
        authors_service,
        "_fetch_faculty_options",
        lambda engine=None: ["Faculty of Technology"],
    )

    def fake_get_author(row_ref, engine=None):
        if row_ref == "(0,1)":
            return {
                "row_ref": "(0,1)",
                "poradie": 1,
                "author_id": 10,
                "display_name": "Autor, Test",
                "organization_id": 100,
                "faculty": "Faculty of Technology",
            }
        if row_ref == "(0,2)":
            return {
                "row_ref": "(0,2)",
                "poradie": 1,
                "author_id": 10,
                "display_name": "Autor, Test",
                "organization_id": 200,
                "faculty": "Faculty of Technology",
            }
        return None

    monkeypatch.setattr(authors_service, "get_author", fake_get_author)

    update_result = MagicMock()
    update_result.rowcount = 1
    begin_conn = MagicMock()
    begin_conn.__enter__ = lambda s: begin_conn
    begin_conn.__exit__ = MagicMock(return_value=False)
    begin_conn.execute.return_value = update_result

    select_result = MagicMock()
    select_result.scalar.return_value = "(0,2)"
    read_conn = MagicMock()
    read_conn.__enter__ = lambda s: read_conn
    read_conn.__exit__ = MagicMock(return_value=False)
    read_conn.execute.return_value = select_result

    engine = MagicMock()
    engine.begin.return_value = begin_conn
    engine.connect.return_value = read_conn

    author = authors_service.update_author(
        "(0,1)",
        {"organization_id": "200", "faculty": "Faculty of Technology"},
        engine=engine,
    )

    params = begin_conn.execute.call_args.args[1]
    assert params["organization_id"] == 200
    assert params["faculty"] == "Faculty of Technology"
    assert params["row_ref"] == "(0,1)"
    assert author["row_ref"] == "(0,2)"
    assert author["organization_id"] == 200


def test_delete_author_raises_for_missing_row():
    result = MagicMock()
    result.rowcount = 0
    conn = MagicMock()
    conn.__enter__ = lambda s: conn
    conn.__exit__ = MagicMock(return_value=False)
    conn.execute.return_value = result

    engine = MagicMock()
    engine.begin.return_value = conn

    with pytest.raises(ValueError, match="Autor neexistuje"):
        authors_service.delete_author("(9,9)", engine=engine)


def test_get_all_authors_groups_faculties_and_departments(monkeypatch):
    monkeypatch.setattr(
        authors_service,
        "_fetch_author_rows",
        lambda engine=None: [
            {
                "row_ref": "(0,1)",
                "author_id": 10,
                "display_name": "Autor, Anna",
                "surname": "Autor",
                "given_name": "Anna",
                "faculty": "Faculty of Technology",
                "organization_id": 100,
            },
            {
                "row_ref": "(0,2)",
                "author_id": 10,
                "display_name": "Autor, Anna",
                "surname": "Autor",
                "given_name": "Anna",
                "faculty": "Faculty of Technology",
                "organization_id": 200,
            },
        ],
    )
    monkeypatch.setattr(
        authors_service,
        "load_workplace_tree",
        lambda remote_engine=None: {
            100: type("Node", (), {"name_en": "Department of Polymers"})(),
            200: type("Node", (), {"name_en": "Department of Food Analysis and Chemistry"})(),
        },
    )

    authors = authors_service.get_all_authors(engine=MagicMock())

    assert len(authors) == 1
    assert authors[0]["primary"] == "Autor, Anna"
    assert authors[0]["row_refs"] == ["(0,1)", "(0,2)"]
    assert authors[0]["departments"] == [
        "Department of Polymers",
        "Department of Food Analysis and Chemistry",
    ]
    assert authors[0]["affiliations"] == [
        {"faculty": "Faculty of Technology", "department": "Department of Polymers"},
        {"faculty": "Faculty of Technology", "department": "Department of Food Analysis and Chemistry"},
    ]


def test_get_author_modal_details_uses_grouped_affiliations(monkeypatch):
    monkeypatch.setattr(
        authors_service,
        "_fetch_author_rows",
        lambda engine=None: [
            {
                "row_ref": "(0,1)",
                "author_id": 10,
                "display_name": "Autor, Anna",
                "surname": "Autor",
                "given_name": "Anna",
                "middle_name": "Maria",
                "orcid": "0000-0001-0000-0001",
                "scopusid": "12345678901",
                "wos_id": "A-1234-2010",
                "public_email": "anna@example.com",
                "email": "hidden@example.com",
                "faculty": "Faculty of Technology",
                "organization_id": 100,
            },
            {
                "row_ref": "(0,2)",
                "author_id": 10,
                "display_name": "Autor, Anna",
                "surname": "Autor",
                "given_name": "Anna",
                "middle_name": "Maria",
                "orcid": "0000-0001-0000-0001",
                "scopusid": "12345678901",
                "wos_id": "A-1234-2010",
                "public_email": "anna@example.com",
                "email": "hidden@example.com",
                "faculty": "Faculty of Technology",
                "organization_id": 200,
            },
        ],
    )
    monkeypatch.setattr(
        authors_service,
        "load_workplace_tree",
        lambda remote_engine=None: {
            100: type("Node", (), {"name_en": "Department of Polymers"})(),
            200: type("Node", (), {"name_en": "Department of Food Analysis and Chemistry"})(),
        },
    )

    details = authors_service.get_author_modal_details("(0,2)", engine=MagicMock())

    assert details is not None
    assert details["display_name"] == "Autor, Anna"
    assert details["preferred_email"] == "anna@example.com"
    assert details["departments"] == [
        "Department of Polymers",
        "Department of Food Analysis and Chemistry",
    ]
    assert len(details["affiliations"]) == 2
