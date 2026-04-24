"""API endpointy pre utb_authors_limited (HTMX)."""

from flask import Blueprint, request, render_template_string

from web.services.authors_service import get_all_authors, search_authors

bp = Blueprint("authors_api", __name__)


def add_author(display_name: str) -> None:
    """Temporary no-op: adding to utb_authors_limited is disabled."""
    return None


def remove_author(display_name: str) -> None:
    """Temporary no-op: deleting from utb_authors_limited is disabled."""
    return None

# Inline Jinja2 template pre zoznam autorov (HTMX swap)
_AUTHORS_LIST_TMPL = """
{% for author in authors %}
<div class="author-row d-flex justify-content-between align-items-center py-1 border-bottom"
     data-name="{{ author.display_name }}">
  <div style="min-width:0;">
    <span class="fw-semibold">{{ author.primary }}</span>
    {% if author.variants|length > 1 %}
    <br><small class="text-muted">
      {% for v in author.variants[1:] %}{{ v }}{% if not loop.last %}<br>{% endif %}{% endfor %}
    </small>
    {% endif %}
  </div>
  <button class="btn p-0 px-1 text-secondary author-menu-btn flex-shrink-0 ms-1"
          style="font-size:1rem; line-height:1.4; background:none; border:none;"
          type="button"
          data-display-name="{{ author.display_name }}">⋮</button>
</div>
{% else %}
<p class="text-muted small">Žiadni autori.</p>
{% endfor %}
"""


@bp.route("/authors", methods=["GET"])
def list_authors():
    q = request.args.get("q", "").strip()
    if q:
        authors = search_authors(q)
    else:
        authors = get_all_authors()
    return render_template_string(_AUTHORS_LIST_TMPL, authors=authors)


@bp.route("/authors", methods=["POST"])
def create_author():
    display_name = (request.form.get("display_name") or "").strip()
    if not display_name:
        return '<p class="text-danger small">Meno nesmie byť prázdne.</p>', 400
    add_author(display_name)
    authors = get_all_authors()
    return render_template_string(_AUTHORS_LIST_TMPL, authors=authors)


@bp.route("/authors", methods=["DELETE"])
def delete_author():
    display_name = (request.form.get("display_name") or "").strip()
    if not display_name:
        return "", 400
    remove_author(display_name)
    authors = get_all_authors()
    return render_template_string(_AUTHORS_LIST_TMPL, authors=authors)
