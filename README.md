# UTB Metadata Pipeline

Pipeline pre spracovanie afiliácií publikácií UTB. Projekt má dve hlavné fázy:
1. heuristiky nad WoS afiliáciou,
2. voliteľná LLM kontrola nejednoznačných záznamov.

Kód je pripravený ako modulárny backend. CLI používa rovnaké služby, ktoré sa dajú priamo volať aj z budúcej Flask aplikácie.

## Čo projekt robí

- `bootstrap` skopíruje remote tabuľku do lokálnej DB a doplní výstupné stĺpce.
- `import-authors` nahrá interných UTB autorov z CSV do tabuľky `utb_internal_authors`.
- `heuristics` spracuje `utb.wos.affiliation`, nájde interných autorov, fakultu, OÚ a nastaví `needs_llm`.
- `llm` spracuje záznamy s `needs_llm=true` a uloží validovaný JSON výstup.
- `status` zobrazí priebežné štatistiky.
- `export` vyexportuje výsledky do CSV.

## Požiadavky

- Python 3.11+
- PostgreSQL 14+
- Prístup na remote DB `publikace.k.utb.cz` (read-only účet)
- Voliteľne Ollama alebo OpenAI kompatibilné API

## Inštalácia

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Na Windows PowerShell:

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
copy .env.example .env
```

## Nastavenie `.env`

V `.env` nastav minimálne:

- remote DB prístup (`REMOTE_DB_*`),
- local DB prístup (`LOCAL_DB_*`),
- tabuľky (`REMOTE_SCHEMA`, `REMOTE_TABLE`, `LOCAL_SCHEMA`, `LOCAL_TABLE`),
- heuristické a LLM parametre podľa potreby.

Nikdy necommituj reálne heslá ani API kľúče.

## Vytvorenie lokálneho DB používateľa a databázy

Príklad pre lokálny PostgreSQL:

```bash
psql -U postgres -h 127.0.0.1 -p 5432 <<'SQL'
CREATE USER veda_local_user WITH PASSWORD 'veda_local_pass';
CREATE DATABASE veda_local OWNER veda_local_user;
GRANT ALL PRIVILEGES ON DATABASE veda_local TO veda_local_user;
SQL
```

Potom zosúlaď `LOCAL_DB_USER`, `LOCAL_DB_PASSWORD`, `LOCAL_DB_NAME` v `.env`.

## Spustenie pipeline

### 1) Bootstrap lokálnej tabuľky

```bash
python -m src.cli bootstrap
```

Bezpečný režim je default. Ak tabuľka existuje, doplnia sa chýbajúce stĺpce a existujúce dáta sa ponechajú.

Nútený rebuild:

```bash
python -m src.cli bootstrap --drop
```

Tento režim zmaže lokálnu tabuľku a skopíruje dáta od začiatku.

### 2) Import interných autorov

```bash
python -m src.cli import-authors --csv autori_utb_oficial_utf8.csv
```

### 3) Heuristiky

```bash
python -m src.cli heuristics
```

Obmedzenie počtu záznamov:

```bash
python -m src.cli heuristics --limit 500 --batch-size 200
```

Opakované spracovanie chýb:

```bash
python -m src.cli heuristics --reprocess-errors
```

### 4) LLM fáza

```bash
python -m src.cli llm
```

Prepnutie providera:

```bash
python -m src.cli llm --provider openai --limit 100
```

### 5) Stav a export

```bash
python -m src.cli status
python -m src.cli export --output vysledky_export.csv
```

## Testy

```bash
pytest
```

## Troubleshooting

- `connection refused` na local DB: skontroluj, že beží PostgreSQL a správny host/port v `.env`.
- Chyba pri remote DB: over firewall/VPN a remote prihlasovacie údaje.
- `llm_status=error`: skontroluj `LLM_PROVIDER`, endpoint, API key a timeout.
- Pomalé heuristiky: zvýš `HEURISTICS_BATCH_SIZE` postupne podľa výkonu DB.

## Breaking changes

- Žiadne CLI príkazy neboli odstránené.
- `export` používa stĺpce `utb_faculty` a `utb_ou` (predtým bol v kóde chybný odkaz na `utb_faculty_guess`).

