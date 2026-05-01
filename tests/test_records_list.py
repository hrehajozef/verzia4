from app import app
from web.services import records_service
import web.blueprints.records.routes as records_routes
from web.services.records_service import (
    GROUP_DUPLICATE,
    GROUP_EXISTING,
    GROUP_SINGLE,
    MergedHistoryRow,
    RecordRow,
)


def _empty_groups() -> dict[str, list[RecordRow]]:
    return {
        GROUP_EXISTING: [],
        GROUP_DUPLICATE: [],
        GROUP_SINGLE: [],
    }


def test_index_hides_approved_by_default(monkeypatch):
    captured: dict[str, object] = {}
    monkeypatch.setattr(records_routes, "_local_db_ready", lambda: True)

    def fake_fetch_unchecked_records(*, sort: str, include_checked: bool, engine=None):
        captured["sort"] = sort
        captured["include_checked"] = include_checked
        return _empty_groups()

    monkeypatch.setattr(records_routes, "fetch_unchecked_records", fake_fetch_unchecked_records)
    monkeypatch.setattr(records_routes, "fetch_pending_records", lambda: [])

    with app.test_client() as client:
        response = client.get("/")

    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert captured == {"sort": "id_asc", "include_checked": False}
    assert 'name="show_approved"' in html
    assert 'checked' not in html.split('name="show_approved"', 1)[1].split(">", 1)[0]


def test_index_handles_missing_local_tables(monkeypatch):
    monkeypatch.setattr(records_routes, "_local_db_ready", lambda: False)

    def fail_fetch(*args, **kwargs):
        raise AssertionError("fetch sa pri chýbajúcej DB nemá volať")

    monkeypatch.setattr(records_routes, "fetch_unchecked_records", fail_fetch)
    monkeypatch.setattr(records_routes, "fetch_pending_records", fail_fetch)

    with app.test_client() as client:
        response = client.get("/")

    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "Lokálna databáza ešte nie je inicializovaná." in html
    assert "/settings/pipeline" in html


def test_index_can_include_approved_records(monkeypatch):
    captured: dict[str, object] = {}
    monkeypatch.setattr(records_routes, "_local_db_ready", lambda: True)

    def fake_fetch_unchecked_records(*, sort: str, include_checked: bool, engine=None):
        captured["sort"] = sort
        captured["include_checked"] = include_checked
        rec = RecordRow(
            resource_id="42",
            title="Schválený záznam",
            authors=["Autor, Anna"],
            year="2024",
            journal="Journal",
            volume="1",
            issue="2",
            source=["j-wok"],
            has_wos=True,
            has_scopus=False,
            has_riv=False,
            doi="10.1000/test",
            issn=[],
            isbn=[],
            checked_count=2,
        )
        return {
            GROUP_EXISTING: [rec],
            GROUP_DUPLICATE: [],
            GROUP_SINGLE: [],
        }

    monkeypatch.setattr(records_routes, "fetch_unchecked_records", fake_fetch_unchecked_records)
    monkeypatch.setattr(records_routes, "fetch_pending_records", lambda: [])

    with app.test_client() as client:
        response = client.get("/?show_approved=1&sort=newest")

    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert captured == {"sort": "newest", "include_checked": True}
    assert 'name="show_approved"' in html
    assert "Zobraziť schválené" in html
    assert "schválené" in html
    assert '/?sort=id_asc&show_approved=1' in html


def test_fetch_pending_records_reads_checked_count_from_query(monkeypatch):
    row = type("Row", (), {
        "resource_id": 10,
        "title_arr": ["Pending"],
        "authors_arr": ["Autor, Anna"],
        "issued_arr": ["2024"],
        "journal_arr": ["Journal"],
        "volume": "1",
        "issue": "2",
        "source_arr": ["j-wok"],
        "doi_arr": ["10.1000/test"],
        "issn_arr": [],
        "isbn_arr": [],
        "checked_count": 0,
        "queue_author_flags": {},
        "main_author_flags": {},
    })()

    class Result:
        def fetchall(self):
            return [row]

    class Conn:
        def execute(self, *args, **kwargs):
            return Result()
        def __enter__(self):
            return self
        def __exit__(self, exc_type, exc, tb):
            return False

    class Engine:
        def connect(self):
            return Conn()

    monkeypatch.setattr(records_service, "_attach_duplicate_metadata", lambda records, engine: None)

    records = records_service.fetch_pending_records(engine=Engine())

    assert len(records) == 1
    assert records[0].checked_count == 0


def test_index_renders_history_link_for_merged_child(monkeypatch):
    monkeypatch.setattr(records_routes, "_local_db_ready", lambda: True)
    rec = RecordRow(
        resource_id="42",
        title="Zlúčený záznam",
        authors=["Autor, Anna"],
        year="2024",
        journal="Journal",
        volume="1",
        issue="2",
        source=["j-wok"],
        has_wos=True,
        has_scopus=False,
        has_riv=False,
        doi="10.1000/test",
        issn=[],
        isbn=[],
        merged_children=[
            MergedHistoryRow(
                history_row_ref="(0,7)",
                resource_id="41",
                title="Pôvodný záznam",
                authors=["Autor, Anna"],
                year="2024",
                journal="Journal",
                volume="1",
                issue="2",
                doi="10.1000/test",
                match_type="exact:dc.identifier.doi",
                match_score=1.0,
                summary="Presná zhoda",
            )
        ],
    )
    monkeypatch.setattr(records_routes, "fetch_unchecked_records", lambda **kwargs: _empty_groups())
    monkeypatch.setattr(records_routes, "fetch_pending_records", lambda: [rec])

    with app.test_client() as client:
        response = client.get("/")

    html = response.get_data(as_text=True)
    assert '/record/history?row_ref=(0,7)' in html

