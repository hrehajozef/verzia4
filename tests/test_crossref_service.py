from unittest.mock import MagicMock

from web.services import crossref_service


def _response(payload):
    response = MagicMock()
    response.json.return_value = payload
    response.raise_for_status.return_value = None
    return response


def test_fetch_crossref_maps_publication_dates(monkeypatch):
    payload = {
        "message": {
            "published-print": {"date-parts": [[2019, 4, 5]]},
            "published-online": {"date-parts": [[2018, 11, 18]]},
            "published": {"date-parts": [[2018, 11, 18]]},
            "issued": {"date-parts": [[2018, 11]]},
            "created": {"date-parts": [[2018, 11, 18]]},
            "deposited": {"date-parts": [[2023, 9, 15]]},
        }
    }

    monkeypatch.setattr(crossref_service.httpx, "get", lambda *args, **kwargs: _response(payload))

    result = crossref_service.fetch_crossref("10.1234/example")

    assert result["ok"] is True
    assert result["by_field"]["utb_date_published"] == "2019-04-05"
    assert result["by_field"]["utb_date_published_online"] == "2018-11-18"
    assert result["by_field"]["dc.date.issued"] == "2018-11-18"
    labels = {item["label"] for item in result["extra"]}
    assert "Vytvorene v Crossref" not in labels
    assert "Aktualizovane v Crossref" not in labels


def test_fetch_crossref_falls_back_to_issued_when_published_missing(monkeypatch):
    payload = {
        "message": {
            "issued": {"date-parts": [[2020, 7, 2]]},
            "title": ["Fallback test"],
        }
    }

    monkeypatch.setattr(crossref_service.httpx, "get", lambda *args, **kwargs: _response(payload))

    result = crossref_service.fetch_crossref("10.1234/fallback")

    assert result["ok"] is True
    assert result["by_field"]["dc.date.issued"] == "2020-07-02"
    assert "utb_date_published" not in result["by_field"]
    assert "utb_date_published_online" not in result["by_field"]
