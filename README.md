# UTB Metadata Pipeline

Pipeline na spracovanie metadát publikácií UTB. Identifikuje interných autorov,
parsuje dátumy, validuje záznamy a deduplikuje výstup. Voliteľne využíva LLM
na doriešenie nejednoznačných prípadov.

## Čo projekt robí

| Príkaz              | Popis |
|---------------------|---|
| `bootstrap`         | Skopíruje remote tabuľku do lokálnej DB |
| `import-authors`    | Nahrajú interných UTB autorov z CSV do `utb_internal_authors` |
| `validate-setup`    | Pridá validačné stĺpce (raz pred prvou validáciou) |
| `validate`          | Kontroluje kvalitu metadát (trailing spaces, mojibake, DOI, …) |
| `dates-setup`       | Pridá dátumové stĺpce (raz pred prvým spracovaním dátumov) |
| `heuristics`        | Heuristicky nájde interných autorov z WoS/DC afiliácií |
| `dates`             | Heuristicky parsuje dátumy z `utb.fulltext.dates` |
| `heuristics-llm`    | LLM spracovanie autorov pre záznamy s `needs_llm=TRUE` |
| `dates-llm`         | LLM spracovanie dátumov pre záznamy s `date_needs_llm=TRUE` |
| `deduplicate`       | Identifikácia a označenie duplikátov |
| `heuristics-status` | Štatistiky autorov a LLM fázy |
| `dates-status`      | Štatistiky dátumov |
| `validate-status`   | Štatistiky validácie |
| `dedup-status`      | Štatistiky deduplikácie |
| `export`            | Export výsledkov do CSV |

## Požiadavky

- Python 3.11+
- [uv](https://docs.astral.sh/uv/) (správca závislostí)
- PostgreSQL 14+ (lokálna DB)
- Prístup na remote DB `publikace.k.utb.cz` (read-only, len pre `bootstrap`)
- Voliteľne: Ollama alebo OpenAI-kompatibilné API

## Inštalácia

```bash
# 1. Nainštaluj závislosti (vrátane dev)
uv sync --all-groups

# 2. Vytvor .env zo šablóny
cp .env.example .env
# Uprav .env – nastav DB prihlasovacie údaje
```

## Nastavenie `.env`

Nastav minimálne:

- `LOCAL_DB_*` – pripojenie k lokálnej PostgreSQL
- `REMOTE_DB_*` – pripojenie k remote DB (len pre `bootstrap`)
- `LOCAL_SCHEMA`, `LOCAL_TABLE` – cieľová lokálna tabuľka
- `REMOTE_SCHEMA`, `REMOTE_TABLE` – zdrojová remote tabuľka

Nikdy necommituj reálne heslá ani API kľúče.

## Vytvorenie lokálnej DB

```sql
-- spusti v psql ako postgres superuser
CREATE USER veda_local_user WITH PASSWORD 'veda_local_pass';
CREATE DATABASE veda_local OWNER veda_local_user;
GRANT ALL PRIVILEGES ON DATABASE veda_local TO veda_local_user;
```

## Spustenie pipeline

Odporúčané poradie príkazov:

### 1. Bootstrap

```bash
uv run python -m src.cli bootstrap
```

Nútený rebuild (zmaže a znovu skopíruje):

```bash
uv run python -m src.cli bootstrap --drop
```

### 2. Import interných autorov

```bash
uv run python -m src.cli import-authors
# alebo s vlastným súborom:
uv run python -m src.cli import-authors --csv data/autori_utb_oficial_utf8.csv
```

Formát CSV: `priezvisko;krstné_meno`, s hlavičkou, 1 riadok = 1 osoba.

### 3. Validácia (jednorazový setup + spustenie)

```bash
uv run python -m src.cli validate-setup
uv run python -m src.cli validate
```

### 4. Príprava dátumových stĺpcov (jednorazovo)

```bash
uv run python -m src.cli dates-setup
```

### 5. Heuristiky – autori

```bash
uv run python -m src.cli heuristics
```

Voliteľné prepínače:

```bash
# Porovnávať mená aj na normalizovaných hodnotách (bez diakritiky) + fuzzy matching
uv run python -m src.cli heuristics --normalize

# Limit a veľkosť dávky
uv run python -m src.cli heuristics --limit 500 --batch-size 200

# Spracovať aj záznamy so statusom error
uv run python -m src.cli heuristics --reprocess-errors
```

### 6. Heuristiky – dátumy

```bash
uv run python -m src.cli dates
```

### 7. LLM fáza – autori

```bash
uv run python -m src.cli heuristics-llm
uv run python -m src.cli heuristics-llm --provider openai --limit 100
```

### 8. LLM fáza – dátumy

```bash
uv run python -m src.cli dates-llm
```

Voliteľné prepínače:

```bash
# Spracovať aj záznamy kde utb.fulltext.dates = '{-}' (štandardne preskočené)
uv run python -m src.cli dates-llm --include-dash

# Znovu spracovať záznamy s chybou
uv run python -m src.cli dates-llm --reprocess
```

### 9. Deduplikácia

```bash
uv run python -m src.cli deduplicate
uv run python -m src.cli deduplicate --by dc.identifier.doi
uv run python -m src.cli deduplicate --by dc.title --no-fuzzy --dry-run
```

### 10. Štatistiky a export

```bash
uv run python -m src.cli heuristics-status
uv run python -m src.cli dates-status
uv run python -m src.cli validate-status
uv run python -m src.cli dedup-status

uv run python -m src.cli export --output data/vysledky_export.csv
```

## Testy

```bash
uv run pytest
```

## Ako funguje porovnávanie autorov

Predvolene sa mená porovnávajú na **surových hodnotách** (presná zhoda s diakritikou, potom fuzzy na surových reťazcoch). Prepínač `--normalize` zapína navyše normalizovanú zhodu (lowercase, bez diakritiky) a fuzzy na normalizovaných menách – vhodné pre datasety kde chýba diakritika (napr. WoS).

## Troubleshooting

- **`connection refused` na local DB** – skontroluj, že beží PostgreSQL a správny host/port v `.env`
- **Chyba pri remote DB** – over firewall/VPN a remote prihlasovacie údaje (potrebné len pre `bootstrap`)
- **`llm_status=error`** – skontroluj `LLM_PROVIDER`, endpoint, API kľúč a timeout
- **Pomalé heuristiky** – zvýš `HEURISTICS_BATCH_SIZE` v `.env`
- **Import autorov: menej záznamov ako riadkov v CSV** – normálne chovanie, CSV obsahuje duplikáty (rovnaké meno viackrát), ktoré sa deduplikujú
