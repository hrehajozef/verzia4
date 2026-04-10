from flask import Blueprint

bp = Blueprint("records", __name__)

from web.blueprints.records import routes  # noqa: E402, F401
