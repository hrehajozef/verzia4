from unittest.mock import MagicMock

from src.common.constants import QUEUE_TABLE
from src.config.settings import settings
from web.services import queue_service


def _result(*, scalar=None, fetchone=None, fetchall=None):
    res = MagicMock()
    res.scalar.return_value = scalar
    res.fetchone.return_value = fetchone
    res.fetchall.return_value = fetchall
    mappings = MagicMock()
    mappings.fetchone.return_value = fetchone
    mappings.fetchall.return_value = fetchall
    res.mappings.return_value = mappings
    return res


def test_pending_change_map_keeps_latest_entry():
    pending = [
        {"id": 1, "field_key": "dc.title", "target_table": settings.local_table, "new_value": "Old pending"},
        {"id": 2, "field_key": "dc.title", "target_table": settings.local_table, "new_value": "Newest pending"},
    ]

    mapped = queue_service._pending_change_map(pending)

    assert mapped[("dc.title", settings.local_table)]["new_value"] == "Newest pending"


def test_save_record_field_only_writes_change_buffer(monkeypatch):
    monkeypatch.setattr(queue_service, "ensure_change_buffer_table", lambda engine=None: None)
    monkeypatch.setattr(
        queue_service,
        "_load_table_columns",
        lambda engine, schema, table: (
            {"dc.title": "_text"} if table == settings.local_table else {"librarian_modified_at": "timestamptz"}
        ),
    )

    statements: list[str] = []
    conn = MagicMock()
    conn.__enter__ = lambda s: conn
    conn.__exit__ = MagicMock(return_value=False)
    conn.execute.side_effect = [
        _result(scalar=["Old title"]),
        _result(fetchone=None),
        _result(),
        _result(),
    ]

    engine = MagicMock()
    engine.begin.return_value = conn

    queue_service.save_record_field("1", "dc.title", "New title", engine=engine)

    for call in conn.execute.call_args_list:
        statement = call.args[0]
        statements.append(getattr(statement, "text", str(statement)))

    assert any("INSERT INTO" in statement and "utb_change_buffer" in statement for statement in statements)
    assert not any(
        f'UPDATE "{settings.local_schema}"."{settings.local_table}"' in statement
        for statement in statements
    )


def test_get_record_detail_overlays_pending_change_and_skips_missing_fields(monkeypatch):
    monkeypatch.setattr(queue_service, "ensure_change_buffer_table", lambda engine=None: None)
    monkeypatch.setattr(
        queue_service,
        "_load_table_columns",
        lambda engine, schema, table: (
            {
                "resource_id": "int8",
                "dc.title": "_text",
                "utb.source": "_text",
            }
            if table == settings.local_table
            else {
                "resource_id": "int8",
                "librarian_checked_at": "_timestamptz",
            }
        ),
    )
    monkeypatch.setattr(queue_service, "_author_source_values", lambda *args, **kwargs: {"wos": None, "scopus": None})
    monkeypatch.setattr(queue_service, "_merged_source_records", lambda *args, **kwargs: [])
    monkeypatch.setattr(queue_service, "get_detail_row_order", lambda: ["dc.title.translated", "dc.title"])

    conn = MagicMock()
    conn.__enter__ = lambda s: conn
    conn.__exit__ = MagicMock(return_value=False)
    conn.execute.side_effect = [
        _result(fetchone={"resource_id": 1, "dc.title": ["Original title"], "utb.source": ["j-riv"]}),
        _result(fetchone={"resource_id": 1, "librarian_checked_at": None}),
        _result(fetchall=[
            {
                "id": 10,
                "field_key": "dc.title",
                "target_table": settings.local_table,
                "old_value": "Original title",
                "new_value": "Pending title",
                "created_at": "2026-04-24T10:00:00",
            }
        ]),
    ]

    engine = MagicMock()
    engine.connect.return_value = conn

    detail = queue_service.get_record_detail("1", engine=engine)

    assert detail is not None
    assert detail["main"]["dc.title"] == "Pending title"
    field_keys = [field["key"] for field in detail["fields"]]
    assert "dc.title" in field_keys
    assert "dc.title.translated" not in field_keys


def test_get_record_detail_keeps_legacy_fulltext_fields_separate_from_utb_faculty(monkeypatch):
    monkeypatch.setattr(queue_service, "ensure_change_buffer_table", lambda engine=None: None)
    monkeypatch.setattr(
        queue_service,
        "_load_table_columns",
        lambda engine, schema, table: (
            {
                "resource_id": "int8",
                "utb.faculty": "_text",
                "utb.ou": "_text",
                "utb.fulltext.faculty": "_text",
                "utb.fulltext.ou": "_text",
                "utb.source": "_text",
            }
            if table == settings.local_table
            else {
                "author_faculty": "_text",
                "author_ou": "_text",
                "resource_id": "int8",
                "librarian_checked_at": "_timestamptz",
            }
        ),
    )
    monkeypatch.setattr(queue_service, "_author_source_values", lambda *args, **kwargs: {"wos": None, "scopus": None})
    monkeypatch.setattr(queue_service, "_merged_source_records", lambda *args, **kwargs: [])
    monkeypatch.setattr(
        queue_service,
        "get_detail_row_order",
        lambda: ["utb.fulltext.faculty", "utb.fulltext.ou", "utb.faculty", "utb.ou"],
    )

    conn = MagicMock()
    conn.__enter__ = lambda s: conn
    conn.__exit__ = MagicMock(return_value=False)
    conn.execute.side_effect = [
        _result(fetchone={
            "resource_id": 1,
            "utb.faculty": ["Faculty of Management and Economics"],
            "utb.ou": ["Department of Economics"],
            "utb.fulltext.faculty": ["Faculty of Technology"],
            "utb.fulltext.ou": ["Department of Polymer Engineering"],
            "utb.source": ["j-riv"],
        }),
        _result(fetchone={
            "resource_id": 1,
            "librarian_checked_at": None,
            "author_faculty": ["Faculty of Management and Economics"],
            "author_ou": ["Department of Economics"],
        }),
        _result(fetchall=[]),
    ]

    engine = MagicMock()
    engine.connect.return_value = conn

    detail = queue_service.get_record_detail("1", engine=engine)

    fields = {field["key"]: field for field in detail["fields"]}
    assert fields["utb.fulltext.faculty"]["main"] == "Faculty of Technology"
    assert fields["utb.fulltext.ou"]["main"] == "Department of Polymer Engineering"
    assert fields["utb.fulltext.faculty"]["proposed"] is None
    assert fields["utb.fulltext.ou"]["proposed"] is None
    assert fields["utb.faculty"]["main"] == "Faculty of Management and Economics"
    assert fields["utb.ou"]["main"] == "Department of Economics"
    assert fields["utb.faculty"]["proposed"] == "Faculty of Management and Economics"
    assert fields["utb.ou"]["proposed"] == "Department of Economics"


def test_get_record_detail_uses_queue_values_as_main_for_date_fields(monkeypatch):
    monkeypatch.setattr(queue_service, "ensure_change_buffer_table", lambda engine=None: None)
    monkeypatch.setattr(
        queue_service,
        "_load_table_columns",
        lambda engine, schema, table: (
            {
                "resource_id": "int8",
                "utb.source": "_text",
            }
            if table == settings.local_table
            else {
                "resource_id": "int8",
                "utb_date_received": "date",
                "librarian_checked_at": "_timestamptz",
            }
        ),
    )
    monkeypatch.setattr(queue_service, "_author_source_values", lambda *args, **kwargs: {"wos": None, "scopus": None})
    monkeypatch.setattr(queue_service, "_merged_source_records", lambda *args, **kwargs: [])
    monkeypatch.setattr(queue_service, "get_detail_row_order", lambda: ["utb_date_received"])

    conn = MagicMock()
    conn.__enter__ = lambda s: conn
    conn.__exit__ = MagicMock(return_value=False)
    conn.execute.side_effect = [
        _result(fetchone={"resource_id": 1, "utb.source": ["j-riv"]}),
        _result(fetchone={"resource_id": 1, "utb_date_received": "2024-03-15", "librarian_checked_at": None}),
        _result(fetchall=[]),
    ]

    engine = MagicMock()
    engine.connect.return_value = conn

    detail = queue_service.get_record_detail("1", engine=engine)

    fields = {field["key"]: field for field in detail["fields"]}
    assert fields["utb_date_received"]["main"] == "2024-03-15"


def test_get_record_detail_includes_merged_sources_summary(monkeypatch):
    monkeypatch.setattr(queue_service, "ensure_change_buffer_table", lambda engine=None: None)
    monkeypatch.setattr(
        queue_service,
        "_load_table_columns",
        lambda engine, schema, table: (
            {
                "resource_id": "int8",
                "dc.title": "_text",
                "utb.source": "_text",
            }
            if table == settings.local_table
            else {
                "resource_id": "int8",
                "librarian_checked_at": "_timestamptz",
            }
        ),
    )
    monkeypatch.setattr(queue_service, "_author_source_values", lambda *args, **kwargs: {"wos": None, "scopus": None})
    monkeypatch.setattr(queue_service, "get_detail_row_order", lambda: ["dc.title"])

    conn = MagicMock()
    conn.__enter__ = lambda s: conn
    conn.__exit__ = MagicMock(return_value=False)
    conn.execute.side_effect = [
        _result(fetchone={"resource_id": 1, "dc.title": ["Merged title"], "utb.source": ["j-riv"]}),
        _result(fetchone={"resource_id": 1, "librarian_checked_at": None}),
        _result(fetchall=[]),
        _result(fetchall=[
            {
                "history_row_ref": "(0,1)",
                "resource_id": 1,
                "dedup_match_type": "exact:dc.identifier.doi",
                "dedup_match_score": 1.0,
                "dedup_match_details": {"matched_value": "10.1234/demo"},
                "dedup_merged_at": "2026-04-30 10:15:00",
                "dedup_kept_resource_id": 1,
                "dedup_other_resource_id": 123,
            },
            {
                "history_row_ref": "(0,2)",
                "resource_id": 123,
                "dedup_match_type": "exact:dc.identifier.doi",
                "dedup_match_score": 1.0,
                "dedup_match_details": {"matched_value": "10.1234/demo"},
                "dedup_merged_at": "2026-04-30 10:15:00",
                "dedup_kept_resource_id": 1,
                "dedup_other_resource_id": 123,
            },
            {
                "history_row_ref": "(0,3)",
                "resource_id": 456,
                "dedup_match_type": "fuzzy_title",
                "dedup_match_score": 0.97,
                "dedup_match_details": {"title_similarity": 0.97},
                "dedup_merged_at": "2026-04-30 10:10:00",
                "dedup_kept_resource_id": 1,
                "dedup_other_resource_id": 456,
            },
        ]),
    ]

    engine = MagicMock()
    engine.connect.return_value = conn

    detail = queue_service.get_record_detail("1", engine=engine)

    assert detail is not None
    assert [item["resource_id"] for item in detail["merged_sources"]] == ["1", "123", "456"]
    assert detail["merged_sources"][0]["is_kept_original"] is True
    assert detail["merged_sources"][1]["match_type"] == "exact:dc.identifier.doi"
    assert detail["merged_sources"][1]["match_score"] == 1.0
    assert detail["merged_sources"][1]["history_row_ref"] == "(0,2)"


def test_get_history_record_detail_returns_read_only_detail(monkeypatch):
    monkeypatch.setattr(
        queue_service,
        "_load_table_columns",
        lambda engine, schema, table: {
            "resource_id": "int8",
            "dc.title": "_text",
            "dc.contributor.author": "_text",
            "utb.source": "_text",
        },
    )
    monkeypatch.setattr(queue_service, "_author_source_values", lambda *args, **kwargs: {"wos": None, "scopus": None})
    monkeypatch.setattr(queue_service, "get_detail_row_order", lambda: ["dc.title", "dc.contributor.author"])
    monkeypatch.setattr(
        queue_service,
        "_get_history_record_row",
        lambda row_ref, engine=None: {
            "history_row_ref": row_ref,
            "resource_id": 5157,
            "dc.title": ["Pôvodný názov"],
            "dc.contributor.author": ["Autor, Test"],
            "utb.source": ["scopus"],
            "dedup_kept_resource_id": 4297,
            "dedup_other_resource_id": 5157,
            "dedup_match_type": "exact:dc.identifier.doi",
            "dedup_match_score": 1.0,
            "dedup_merged_at": "2026-04-30 10:15:00",
        },
    )

    detail = queue_service.get_history_record_detail("(0,9)", engine=MagicMock())

    assert detail is not None
    assert detail["read_only"] is True
    assert detail["is_history"] is True
    assert detail["pending_changes"] == []
    assert detail["history_info"]["kept_resource_id"] == "4297"
    fields = {field["key"]: field for field in detail["fields"]}
    assert fields["dc.title"]["main"] == "Pôvodný názov"



def test_author_modal_data_aligns_repo_authors_with_scopus_and_wos_affiliations():
    main_data = {
        "dc.contributor.author": ["Belas, Jaroslav", "Strnad, Zdenek"],
        "utb.scopus.affiliation": [
            "Belas J., Tomas Bata University in Zlin, Faculty of Management and Economics, Zlin, Czech Republic; "
            "Strnad Z., University of South Bohemia, Ceske Budejovice, Czech Republic"
        ],
        "utb.wos.affiliation": [
            "[Belas, Jaroslav] Tomas Bata Univ Zlin, Fac Management & Econ, Zlin, Czech Republic; "
            "[Strnad, Zdenek] Univ South Bohemia, Ceske Budejovice, Czech Republic"
        ],
    }

    rows = queue_service._author_modal_data(main_data, "Belas, Jaroslav")

    assert rows == [
        {
            "name": "Belas, Jaroslav",
            "is_internal": True,
            "scopus_aff": "Tomas Bata University in Zlin, Faculty of Management and Economics, Zlin, Czech Republic",
            "wos_aff": "Tomas Bata Univ Zlin, Fac Management & Econ, Zlin, Czech Republic",
        },
        {
            "name": "Strnad, Zdenek",
            "is_internal": False,
            "scopus_aff": "University of South Bohemia, Ceske Budejovice, Czech Republic",
            "wos_aff": "Univ South Bohemia, Ceske Budejovice, Czech Republic",
        },
    ]
