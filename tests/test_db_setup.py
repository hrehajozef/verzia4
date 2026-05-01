from unittest.mock import MagicMock

from src.db import setup


def test_setup_prirastky_view_skips_when_table_missing(monkeypatch, capsys):
    class Inspector:
        def has_table(self, table_name, schema=None):
            return False

    engine = MagicMock()
    monkeypatch.setattr(setup, "inspect", lambda local_engine: Inspector())

    setup.setup_prirastky_view(local_engine=engine)

    captured = capsys.readouterr()
    assert setup.PRIRASTKY_TABLE in captured.out
    engine.begin.assert_not_called()


def test_setup_prirastky_view_creates_union_view(monkeypatch):
    class Inspector:
        def has_table(self, table_name, schema=None):
            return table_name == setup.PRIRASTKY_TABLE

    conn = MagicMock()
    tx = MagicMock()
    tx.__enter__ = lambda s: conn
    tx.__exit__ = MagicMock(return_value=False)
    engine = MagicMock()
    engine.begin.return_value = tx
    monkeypatch.setattr(setup, "inspect", lambda local_engine: Inspector())

    setup.setup_prirastky_view(local_engine=engine)

    sql = str(conn.execute.call_args.args[0])
    assert "CREATE OR REPLACE VIEW" in sql
    assert setup.PRIRASTKY_VIEW in sql
    assert setup.PRIRASTKY_TABLE in sql
