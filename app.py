"""Flask web app entry point.

Spustenie:
    uv run python app.py
"""

from src.config.settings import settings
from web import create_app

app = create_app()

if __name__ == "__main__":
    app.run(
        host="127.0.0.1",
        port=settings.web_port,
        debug=settings.web_debug,
    )
