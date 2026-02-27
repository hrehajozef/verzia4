"""
Vytvára SQLAlchemy enginy pre remote aj lokálnu PostgreSQL databázu.

DÔLEŽITÉ: Remote engine je určený len na čítanie (SELECT).
Lokálny engine sa používa na zápis výsledkov.

Enginy sa vytvárajú až pri prvom zavolaní jednej z týchto funkcií.
Engine je objekt, ktorý drží konfiguráciu pripojenia, pool a vie vytvárať connections.
"""

# TODO: Pridať typer miesto print

from functools import lru_cache
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from src.config.settings import settings


@lru_cache(maxsize=1)       # cache pre 1 remote engine
def get_remote_engine() -> Engine:
    """
    Vráti SQLAlchemy engine pre remote univerzitnú databázu.
    Pripojenie je read-only - nepoužívať na zápis!
    """
    engine = create_engine(
        settings.remote_db_url,
        pool_pre_ping=True,          # over spojenie pred použitím
        pool_size=2,                 # malý pool, len čítame
        max_overflow=2,
        connect_args={"connect_timeout": 15},
    )
    return engine # výstup vo forme


@lru_cache(maxsize=1)       # cache pre 1 local engine
def get_local_engine() -> Engine:
    """
    Vráti SQLAlchemy engine pre lokálnu databázu.
    Používa sa na zápis výsledkov spracovania.
    """
    engine = create_engine(
        settings.local_db_url,
        pool_pre_ping=True,
        pool_size=5,
        max_overflow=10,
    )
    return engine


def test_connection(engine: Engine, label: str = "DB") -> bool:
    """
    Otestuje pripojenie na databázu jednoduchým SELECT 1.
    Vráti True ak je spojenie funkčné, inak vypíše chybu a vráti False.
    """
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1")) # vráti 1 ak spojenie funguje
        print(f"[OK] Pripojenie na {label} funguje.")
        return True
    except Exception as exc:
        print(f"[CHYBA] Pripojenie na {label} zlyhalo: {exc}")
        return False
