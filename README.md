# UTB Metadata Pipeline

Pipeline na spracovanie a čistenie metadát publikácií Tomášovej Baťovej Univerzity (UTB). Identifikuje interných autorov, parsuje dátumy, normalizuje názvy žurnálov a vydavateľov, validuje a opravuje záznamy, deduplikuje výstup. Voliteľne využíva LLM na doriešenie nejednoznačných prípadov.

## Čo projekt robí

| Fáza        | Príkaz                | Popis                                                           |
|-------------|-----------------------|-----------------------------------------------------------------|
| Init        | `bootstrap`           | Skopíruje remote tabuľku do lokálnej DB                         |
| Init        | `import-authors`      | Nahrajú interných UTB autorov z CSV do `utb_internal_authors`   |
| Init        | `migrate-columns`     | Jednorazová migrácia starých názvov stĺpcov na `author_*`       |
| Validácia   | `validate-setup`      | Pridá validačné stĺpce *(raz pred prvou validáciou)*            |
| Validácia   | `validate`            | Kontroluje kvalitu metadát + generuje navrhnuté opravy          |
| Validácia   | `apply-fixes`         | Aplikuje navrhnuté opravy do skutočných stĺpcov                 |
| Dátumy      | `dates-setup`         | Pridá dátumové stĺpce *(raz)*                                   |
| Autori      | `heuristics`          | Heuristicky nájde interných autorov z WoS/DC afiliácií          |
| Dátumy      | `dates`               | Heuristicky parsuje dátumy z `utb_fulltext_dates`               |
| LLM         | `heuristics-llm`      | LLM spracovanie autorov (`author_needs_llm=TRUE`)               |
| LLM         | `dates-llm`           | LLM spracovanie dátumov (`date_needs_llm=TRUE`)                 |
| Žurnály     | `journals-setup`      | Pridá stĺpce normalizácie žurnálov *(raz)*                      |
| Žurnály     | `journals-lookup`     | Lookupuje Crossref/OpenAlex/Google Books/OpenLibrary            |
| Žurnály     | `journals-apply`      | Zobrazí diff + aplikuje po schválení knihovníkom                |
| Dedup       | `dedup-setup`         | Vytvorí históriu deduplikácie *(raz)*                           |
| Dedup       | `deduplicate`         | Nájde, fyzicky zlúči a označí duplikáty                         |
| Výstup      | `export`              | Export výsledkov do CSV                                         |
| Štatistiky  | `heuristics-status`   | Štatistiky spracovania autorov                                  |
| Štatistiky  | `heuristics-compare`  | Porovná program vs knihovník (author_internal_names)            |
| Štatistiky  | `dates-status`        | Štatistiky dátumov                                              |
| Štatistiky  | `validate-status`     | Štatistiky validácie                                            |
| Štatistiky  | `journals-status`     | Štatistiky normalizácie žurnálov                                |
| Štatistiky  | `dedup-status`        | Štatistiky deduplikácie                                         |

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

```env
# Remote DB (read-only – len pre bootstrap)
REMOTE_DB_HOST=publikace.k.utb.cz
REMOTE_DB_PORT=5432
REMOTE_DB_NAME=...
REMOTE_DB_USER=...
REMOTE_DB_PASSWORD=...
REMOTE_SCHEMA=veda
REMOTE_TABLE=obd_record_export_all

# Lokálna DB (write)
LOCAL_DB_HOST=localhost
LOCAL_DB_PORT=5432
LOCAL_DB_NAME=veda_local
LOCAL_DB_USER=veda_local_user
LOCAL_DB_PASSWORD=veda_local_pass
LOCAL_SCHEMA=veda
LOCAL_TABLE=obd_record_export_all

# LLM (vyber jedného poskytovateľa)
LLM_PROVIDER=ollama
LOCAL_LLM_BASE_URL=http://localhost:11434
LOCAL_LLM_MODEL=qwen3:8b
# alebo:
# LLM_PROVIDER=openai
# OPENAI_API_KEY=...
# OPENAI_BASE_URL=https://api.groq.com/openai/v1
# OPENAI_MODEL=llama-3.3-70b-versatile

# Prahy
AUTHOR_MATCH_THRESHOLD=0.80
FUZZY_DEDUP_THRESHOLD=0.85
```

Nikdy necommituj reálne heslá ani API kľúče.

## Vytvorenie lokálnej DB

```sql
-- spusti v psql ako postgres superuser
CREATE USER veda_local_user WITH PASSWORD 'veda_local_pass';
CREATE DATABASE veda_local OWNER veda_local_user;
GRANT ALL PRIVILEGES ON DATABASE veda_local TO veda_local_user;
```

## Spustenie pipeline

### 1. Bootstrap – inicializácia lokálnej DB

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
# s vlastným súborom:
uv run python -m src.cli import-authors --csv data/utb_internal_authors.csv
```

Formát CSV: `priezvisko;krstné_meno` s hlavičkou, 1 riadok = 1 osoba.

### 3. Validácia metadát

```bash
# Jednorazová príprava stĺpcov (spusti raz)
uv run python -m src.cli validate-setup

# Spustenie validácie (+ návrhy opráv)
uv run python -m src.cli validate
uv run python -m src.cli validate --limit 100     # len prvých 100
uv run python -m src.cli validate --revalidate    # re-validuj aj hotové

# Prehľad navrhnutých opráv (farebný diff, bez zápisu do DB)
uv run python -m src.cli apply-fixes --preview

# Aplikovanie opráv (záznamy sa automaticky nastavia na re-validáciu)
uv run python -m src.cli apply-fixes
uv run python -m src.cli apply-fixes --limit 50

# Štatistiky
uv run python -m src.cli validate-status
```

Čo validate kontroluje:
- Trailing/leading whitespace
- Non-breaking space (U+00A0, U+202F)
- Dvojité medzery
- Mojibake (UTF-8 čítaný ako Latin-1, opravuje cez `ftfy`)
- PUA ligatúry a encoding chyby (Ĝ→ř, Ĥ→ů, ı→í, ௅→–, ...)
- Osamotené diakritické znaky (ˇ ˝ ˚ ˘ ˆ ´ ¨)
- DOI formát a URL prefix (`https://doi.org/` → strip)
- URL query parametre (`?via%3Dihub` → strip)
- Rúra `|` vo vnútri hodnoty
- Hranatá zátvorka `[...]` v `dc.title`
- WoS identifikátor (musí začínať `000`)
- OBDID existencia v remote `veda.obd_publikace`

### 4. Príprava dátumových stĺpcov (jednorazovo)

```bash
uv run python -m src.cli dates-setup
```

### 5. Heuristiky – autori

```bash
uv run python -m src.cli heuristics
uv run python -m src.cli heuristics --limit 500 --batch-size 200
uv run python -m src.cli heuristics --normalize        # fuzzy + normalizované mená
uv run python -m src.cli heuristics --reprocess-errors # re-spracuj záznamy s error
uv run python -m src.cli heuristics --reprocess        # re-spracuj aj hotové záznamy
```

**Path A (WoS záznamy):** Parsuje WoS afiliačné bloky `[Authors] Institution;`, extrahuje UTB bloky, matchuje autorov z registra. WoS fakulta má prednosť pred remote DB.

**Path B (bez WoS):** Priamy fuzzy matching `dc.contributor.author` voči registru s `require_surname_match` guardom (zabraňuje falošným pozitívam naprieč priezviskami). Fuzzy zhody sú flagované ako `path_b_low_confidence_matches`. Fakulta/OU z remote DB.

```bash
# Porovnanie s hodnotami od knihovníka
uv run python -m src.cli heuristics-compare

# Štatistiky
uv run python -m src.cli heuristics-status
```

### 6. Heuristiky – dátumy

```bash
uv run python -m src.cli dates
uv run python -m src.cli dates --limit 50
uv run python -m src.cli dates --reprocess
```

Parsuje `utb_fulltext_dates` s MDR resolverom:
- Hodnota > 12 → jednoznačný formát (HIGH)
- Chronologické obmedzenie → MEDIUM (+ flag pre knižníka)
- Obe interpretácie platné → LOW (→ LLM fallback)

```bash
uv run python -m src.cli dates-status
```

### 7. LLM fáza – autori

```bash
uv run python -m src.cli heuristics-llm
uv run python -m src.cli heuristics-llm --provider openai --limit 100
```

Spracováva záznamy kde `author_needs_llm=TRUE` po heuristikách.

### 8. LLM fáza – dátumy

```bash
uv run python -m src.cli dates-llm
uv run python -m src.cli dates-llm --include-dash   # spracuj aj '{-}' záznamy
uv run python -m src.cli dates-llm --reprocess      # re-spracuj chyby
```

### 9. Normalizácia žurnálov

```bash
# Jednorazová príprava stĺpcov (spusti raz)
uv run python -m src.cli journals-setup

# Lookup kanonických hodnôt cez API
uv run python -m src.cli journals-lookup
uv run python -m src.cli journals-lookup --limit 50   # len prvých 50 skupín
uv run python -m src.cli journals-lookup --reprocess  # znovu aj spracované

# Zobraz navrhnuté zmeny (farebný diff, bez zápisu)
uv run python -m src.cli journals-apply --preview

# Interaktívny mód – potvrdzuj každú ISSN skupinu zvlášť
uv run python -m src.cli journals-apply --interactive
uv run python -m src.cli journals-apply --interactive --issn 0002-9726

# Dávkové schválenie všetkých návrhov naraz
uv run python -m src.cli journals-apply

# Štatistiky
uv run python -m src.cli journals-status
```

Zdroje pravdy (podľa priority):
1. **API** – Crossref (ISSN) → OpenAlex (ISSN fallback) · Google Books (ISBN) → OpenLibrary (ISBN fallback)
2. **Existujúce záznamy** – Scopus afiliácia → WoS afiliácia → najpočetnejšia hodnota

Normalizujú sa len záznamy, ktorých hodnota sa líši od kanonickej.

### 10. Deduplikácia

```bash
# Jednorazová príprava histórie (spusti raz)
uv run python -m src.cli dedup-setup

# Deduplikácia
uv run python -m src.cli deduplicate
uv run python -m src.cli deduplicate --dry-run       # len výpis, bez zápisu
uv run python -m src.cli deduplicate --no-fuzzy      # len presná + obsahová zhoda
uv run python -m src.cli deduplicate --by dc.title   # presná zhoda podľa titulu

# Štatistiky
uv run python -m src.cli dedup-status
```

Typy duplikátov:
- `exact:<column>` – presná zhoda stĺpca → fyzické zlúčenie
- `early_access` – rovnaký obsah+ISSN, jeden bez pagination → fyzické zlúčenie
- `merged_type` – rovnaký obsah, iný časopis, article vs conferenceObject → fyzické zlúčenie
- `autoplagiat` – rovnaký obsah, iný časopis, rovnaký typ → len flag
- `fuzzy_title` – podobný titul (Jaro-Winkler ≥ 0.85) → len flag

### 11. Export

```bash
uv run python -m src.cli export --output data/vysledky_export.csv
```

## Migrácia existujúcej DB (jednorazovo)

Ak máš existujúcu DB so starými názvami stĺpcov (pred refactoringom), spusti:

```bash
uv run python -m src.cli migrate-columns
```

Premenuje: `flags→author_flags`, `heuristic_status→author_heuristic_status`, `needs_llm→author_needs_llm`, `llm_status→author_llm_status`, `utb_contributor_internalauthor→author_internal_names`, `utb_faculty→author_faculty` atď.

## Testy

```bash
uv run python -m pytest
uv run python -m pytest tests/test_validation.py -v
uv run python -m pytest tests/test_dates_parser.py -v
```

## Troubleshooting

| Problém                                              | Riešenie                                                                      |
|------------------------------------------------------|-------------------------------------------------------------------------------|
| `connection refused` na local DB                     | Skontroluj, že beží PostgreSQL a správny host/port v `.env`                   |
| Chyba pri remote DB                                  | Over firewall/VPN a remote prihlasovacie údaje (potrebné len pre `bootstrap`) |
| `author_llm_status=error`                            | Skontroluj `LLM_PROVIDER`, endpoint, API kľúč a timeout                       |
| Pomalé heuristiky                                    | Zvýš `HEURISTICS_BATCH_SIZE` v `.env`                                         |
| Import autorov: menej záznamov ako riadkov v CSV     | Normálne – CSV môže obsahovať duplicitné mená                                 |
| `validate` hlási 0 záznamov                          | Buď sú všetky `not_checked` status, alebo použi `--revalidate`                |
| `apply-fixes` nič nerobí                             | Spusti najprv `validate`; skontroluj `validate-status`                        |
| `journals-lookup` nenájde nič                        | API je dostupné len online; over sieťové pripojenie                           |
| `journals-apply` bez `--preview` nič nezapíše        | Najprv spusti `journals-lookup`; skontroluj `journals-status`                 |
| Stĺpce `author_*` neexistujú                         | Spusti `migrate-columns` (jednorazová migrácia starého DB schéma)             |
