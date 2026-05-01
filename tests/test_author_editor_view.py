from app import app
import web.blueprints.records.routes as records_routes


def _editor_config(can_write: bool = True):
    return {
        "can_write": can_write,
        "columns": [
            {"name": "poradie", "label": "Poradie", "kind": "number", "required": True, "options": []},
            {"name": "author_id", "label": "Author ID", "kind": "number", "required": True, "options": []},
            {"name": "display_name", "label": "Zobrazované meno", "kind": "text", "required": True, "options": []},
            {"name": "faculty", "label": "Fakulta", "kind": "select", "required": False, "options": ["Faculty of Technology"]},
            {"name": "utb", "label": "UTB", "kind": "text", "required": False, "options": []},
        ],
    }


def test_author_editor_get_renders_existing_author(monkeypatch):
    monkeypatch.setattr(records_routes, "get_author_editor_config", lambda: _editor_config())
    monkeypatch.setattr(records_routes, "get_author", lambda row_ref: {
        "row_ref": row_ref,
        "poradie": 1,
        "author_id": 10,
        "display_name": "Autor, Anna",
        "faculty": "Faculty of Technology",
        "utb": "ano",
    })

    with app.test_client() as client:
        response = client.get("/authors/editor?row_ref=%280%2C1%29&return_to=%2Frecord%2F42")

    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "Úprava UTB autora" in html
    assert "Autor, Anna" in html
    assert 'value="/record/42"' in html


def test_author_editor_post_updates_and_redirects(monkeypatch):
    captured = {}
    monkeypatch.setattr(records_routes, "get_author_editor_config", lambda: _editor_config())

    def fake_update(row_ref, payload):
        captured["row_ref"] = row_ref
        captured["payload"] = payload

    monkeypatch.setattr(records_routes, "update_author", fake_update)

    with app.test_client() as client:
        response = client.post(
            "/authors/editor",
            data={
                "row_ref": "(0,1)",
                "return_to": "/record/42",
                "poradie": "1",
                "author_id": "10",
                "display_name": "Autor, Anna",
                "faculty": "Faculty of Technology",
                "utb": "ano",
            },
        )

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/record/42")
    assert captured["row_ref"] == "(0,1)"
    assert captured["payload"]["display_name"] == "Autor, Anna"


def test_author_editor_post_shows_validation_error(monkeypatch):
    monkeypatch.setattr(records_routes, "get_author_editor_config", lambda: _editor_config())

    def fake_create(payload):
        raise ValueError("Pole 'author_id' musí byť celé číslo.")

    monkeypatch.setattr(records_routes, "create_author", fake_create)

    with app.test_client() as client:
        response = client.post(
            "/authors/editor",
            data={
                "return_to": "/record/42",
                "poradie": "1",
                "author_id": "abc",
                "display_name": "Autor, Anna",
                "faculty": "Faculty of Technology",
                "utb": "ano",
            },
        )

    assert response.status_code == 422
    html = response.get_data(as_text=True)
    assert "Pole &#39;author_id&#39; musí byť celé číslo." in html
    assert "Autor, Anna" in html


def test_author_editor_get_shows_delete_button_for_existing_author(monkeypatch):
    monkeypatch.setattr(records_routes, "get_author_editor_config", lambda: _editor_config())
    monkeypatch.setattr(records_routes, "get_author", lambda row_ref: {
        "row_ref": row_ref,
        "poradie": 1,
        "author_id": 10,
        "display_name": "Autor, Anna",
        "faculty": "Faculty of Technology",
        "utb": "ano",
    })

    with app.test_client() as client:
        response = client.get("/authors/editor?row_ref=%280%2C1%29&return_to=%2Frecord%2F42")

    assert response.status_code == 200
    html = response.get_data(as_text=True)
    assert "Vymazať autora" in html
    assert "/authors/editor/(0,1)/delete" in html


def test_author_editor_delete_redirects(monkeypatch):
    captured = {}

    def fake_delete(row_ref):
        captured["row_ref"] = row_ref

    monkeypatch.setattr(records_routes, "delete_author", fake_delete)

    with app.test_client() as client:
        response = client.post(
            "/authors/editor/%280,1%29/delete",
            data={"return_to": "/record/42"},
        )

    assert response.status_code == 302
    assert response.headers["Location"].endswith("/record/42")
    assert captured["row_ref"] == "(0,1)"
