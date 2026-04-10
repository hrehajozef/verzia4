"""API endpoint pre Crossref lookup – vracia JSON pre inline zobrazenie v tabuľke."""

from flask import Blueprint, request, jsonify

from web.services.crossref_service import fetch_crossref

bp = Blueprint("crossref_api", __name__)


@bp.route("/crossref/<resource_id>")
def crossref_data(resource_id: str):
    doi = request.args.get("doi", "").strip()
    if not doi:
        return jsonify({"ok": False, "by_field": {}, "extra": [], "error": "Žiadny DOI"})
    result = fetch_crossref(doi)
    return jsonify(result)
