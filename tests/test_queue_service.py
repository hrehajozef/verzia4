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
