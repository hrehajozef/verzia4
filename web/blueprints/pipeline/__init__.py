from flask import Blueprint

bp = Blueprint("pipeline", __name__)

from web.blueprints.pipeline import routes  # noqa: E402, F401
