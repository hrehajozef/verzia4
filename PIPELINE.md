# Pipeline – Detailný popis toku dát a funkcií

Tento dokument popisuje celú pipeline na úrovni funkcií: čo každá funkcia robí, čo konzumuje, čo produkuje a na ktoré ďalšie funkcie sa odkazuje.

---

## Prehľad fáz

```
Remote PostgreSQL DB (read-only)
         │
         ▼
[1] bootstrap → lokálna DB tabuľka (kópia)
         │
         ▼
[2] import-authors → utb_internal_authors (register)
         │
         ├──────────────────────────────────────┐
         ▼                                      │
[3] validate → validation_status                │
         │      validation_flags                │
         │      validation_suggested_fixes      │
         ▼                                      │
[3b] apply-fixes → opravené stĺpce              │
         │          validation_status='not_checked' (re-queue)
         │                                      │
         ▼                                      │
[4] heuristics → author_heuristic_status        │
         │        author_internal_names         │
         │        author_faculty / author_ou    │
         │        author_needs_llm              │
         │        author_flags                  │
         │                                      │
         ▼                                      │
[5] dates → date_heuristic_status               │
         │   utb_date_*                         │
         │   date_needs_llm                     │
         │                                      │
         ├──────────────────────────────────────┘
         │  (validate môže re-checkovať auth. subset
         │   po tom, čo heuristiky doplnili autorov)
         ▼
[6] heuristics-llm → author_llm_status
         │            author_internal_names (doplnenie)
         │            author_faculty / author_ou (doplnenie)
         ▼
[7] dates-llm → date_llm_status
         │       utb_date_* (doplnenie)
         ▼
[8] journals-lookup → journal_norm_status='has_proposal'
         │             journal_norm_proposed_publisher
         │             journal_norm_proposed_ispartof
         ▼
[8b] journals-apply → dc.publisher (update)
         │              dc.relation.ispartof (update)
         │              journal_norm_status='applied'
         ▼
[9] deduplicate → author_flags['duplicates']
         │         fyzické zlúčenie (UPDATE + DELETE)
         │         dedup_histoire (história)
         ▼
[10] export → CSV súbor
```

---

## Fáza 1: Bootstrap (`src/db/setup.py`)

### `run_bootstrap(drop_existing=False)`
**Vstup:** remote DB (read-only)
**Výstup:** lokálna tabuľka `{schema}.{table}` – kópia remote tabuľky
**Volá:**
- `_get_remote_columns(remote_engine)` – načíta metadáta stĺpcov z `information_schema.columns`
- `_build_create_table_sql(columns)` – generuje DDL podľa remote stĺpcov + `OUTPUT_COLUMNS`
- `_ensure_output_columns(local_engine)` – idempotentné pridanie `author_*` stĺpcov
- `_create_indexes(local_engine)` – indexy na `author_heuristic_status`, `author_needs_llm`, `author_llm_status`
- `_copy_data(remote_engine, local_engine, columns)` – kopíruje dáta po dávkach

**Poznámka:** `--drop` zmaže a znovu vytvorí tabuľku. Bezpečné spustiť opakovane bez `--drop`.

### `rename_legacy_author_columns(local_engine=None)`
Jednorazová migrácia: premenuje staré názvy stĺpcov na nové `author_*` názvy.
Spúšťa sa príkazom `migrate-columns`. Bezpečné spustiť opakovane.

---

## Fáza 2: Import autorov (`src/authors/registry.py`)

### `load_authors_from_csv(csv_path) → list[InternalAuthor]`
**Vstup:** CSV súbor (`priezvisko;krstné_meno` s hlavičkou)
**Výstup:** zoznam `InternalAuthor(surname, firstname)`
**Poznámka:** Deduplikuje záznamy v rámci CSV.

### `setup_authors_table(engine)`
Zmaže a znovu vytvorí tabuľku `utb_internal_authors` (DROP + CREATE + INDEX).

### `import_authors_to_db(authors, engine) → int`
**Vstup:** zoznam `InternalAuthor`
**Výstup:** počet importovaných záznamov
TRUNCATE + INSERT. Volá sa po `setup_authors_table`.

### `get_author_registry(engine) → list[InternalAuthor]`
Načíta register z lokálnej DB. Výsledok je cachovaný v pamäti (modul-level `_AUTHOR_REGISTRY`).

### `match_author(name, registry, threshold, normalize, require_surname_match) → MatchResult`
**Vstup:** meno z publikácie, register interných autorov
**Výstup:** `MatchResult(matched, author, score, match_type)`
**Stratégie (v poradí):**
1. `exact_diacritic` – presná zhoda s diakritikou (vždy, ignoruje `require_surname_match`)
2. `exact_normalized` – lowercase bez diakritiky (len ak `normalize=True`; ignoruje `require_surname_match`)
3. `fuzzy` – Jaro-Winkler ≥ threshold na surových (alebo normalizovaných) menách

Ak `require_surname_match=True`: fuzzy kandidátska množina je prefiltrovaná na záznamy
s normalizovaným priezviskom zhodným s kandidátom. Eliminuje falošné pozitíva v Path B.

### `lookup_author_affiliations(surname, firstname, remote_engine) → (tuple[str,...], str)`
**Vstup:** priezvisko a meno matchovaného autora
**Výstup:** `(tuple českých názvov fakúlt, český názov ústavu)` z remote DB
Výsledok je cachovaný v `_AFFILIATION_CACHE` per-run.

---

## Fáza 3: Validácia (`src/quality/checks.py`)

### `setup_validation_columns(engine=None)`
Pridá stĺpce `validation_status`, `validation_flags`, `validation_suggested_fixes`,
`validation_version`, `validation_checked_at` do lokálnej tabuľky (idempotentné).

### `run_validation(engine, remote_engine, batch_size, limit, revalidate)`
**Hlavný runner validácie.**
**Volá:**
- `get_author_registry(engine)` – register pre kontrolu autorov (po heuristikách)
- `validate_record(row_data, registry_names)` – validácia jedného záznamu
- `check_obdid_batch(engine, remote_engine)` – batch kontrola OBDID v remote DB

**Výstup do DB:** `validation_status`, `validation_flags`, `validation_suggested_fixes`,
`validation_version`, `validation_checked_at`

### `validate_record(row_data, registry_names) → (status, issues, suggested_fixes)`
**Vstup:** dict hodnôt stĺpcov pre jeden záznam, set mien z registra
**Výstup:** trojica `(str, dict, dict)`

**Kontroly:**
| Typ | Popis | Oprava |
|-----|-------|--------|
| `trailing_spaces` | Vedúce/koncové whitespace | `strip()` |
| `mojibake` | UTF-8 čítaný ako Latin-1 | `ftfy.fix_text()` |
| `encoding_chars` | PUA ligatúry, Ĝ→ř, Ĥ→ů, ı→í, ... | `_CHAR_FIX_MAP` |
| `standalone_diacritics` | ˇ ˝ ˚ ˘ ˆ ´ ¨ | odstrániť |
| `nbsp` | U+00A0, U+202F, U+2007 | → normálna medzera |
| `double_space` | Viacnásobná medzera | → jedna medzera |
| `pipe_in_field` | `\|` vo vnútri hodnoty | len flag |
| `invalid_doi` | DOI s URL prefixom | strip prefix |
| `url_query_params` | `?via%3Dihub` a pod. | strip `?...` |
| `brackets_in_title` | `[...]` v `dc.title` | len flag |
| `invalid_wos_id` | `utb.identifier.wok` nezačína `0` | len flag |
| `authors_not_in_registry` | Interní autori chýbajú v registri | len flag |
| `obdid_not_in_remote` | OBDID neexistuje v `veda.obd_publikace` | len flag |

### `_fix_text_str(s) → (fixed, [fix_types])`
Opravná pipeline pre jeden reťazec. Poradie krokov je záväzné:
1. `ftfy.fix_text(s)` → opraví mojibake
2. `_CHAR_FIX_MAP` substitúcie → PUA ligatúry, encoding chyby
3. `_STANDALONE_DIACRITICS_RE.sub("", s)` → osamotené diakritické znaky
4. `_NBSP_RE.sub(" ", s)` → non-breaking space → normálna medzera
5. `_DOUBLE_SPACE_RE.sub(" ", s)` → jedna medzera
6. `s.strip()` → trailing/leading whitespace

### `check_obdid_batch(engine, remote_engine) → {resource_id: [invalid_ids]}`
Načíta `utb.identifier.obdid` z lokálnej DB, konvertuje na int, batch-overuje existenciu
v `veda.obd_publikace WHERE id = ANY(:ids)` (chunky po 999 kvôli Oracle FDW limitu).
Vracia len záznamy s neplatnými OBDID.

### `run_apply_fixes(engine, preview, dry_run, limit)`
Čerpá `validation_suggested_fixes` z DB. S `--preview`: ANSI farebný diff (červená = original,
zelená = navrhnutá). Bez preview: `UPDATE` každého stĺpca + reset `validation_status='not_checked'`.

---

## Fáza 4: Heuristiky – autori (`src/authors/heuristics.py`)

### `run_heuristics(engine, remote_engine, batch_size, limit, reprocess_errors, reprocess, normalize)`
**Hlavný runner pre matching autorov.**
Spracováva záznamy v dávkach. Pre každú dávku volá `process_batch()`.
**Výstup do DB:** `author_heuristic_status`, `author_heuristic_version`,
`author_heuristic_processed_at`, `author_needs_llm`, `author_dc_names`,
`author_internal_names`, `author_faculty`, `author_ou`, `author_flags`

### `process_batch(rows, registry, normalize, remote_engine) → list[dict]`
Volá `process_record()` pre každý riadok v dávke.

### `process_record(resource_id, wos_aff_arr, dc_authors_arr, registry, normalize, remote_engine) → dict`
Rozvetvuje na Path A alebo Path B podľa prítomnosti WoS afiliácie.

**Path A – WoS záznamy:**
1. `parse_wos_affiliation(raw_aff, resource_id)` → parsuje bloky `[Autori] Inštitúcia;`
2. `detect_utb_affiliation(affiliation_raw)` → identifikuje UTB bloky
3. `resolve_faculty_and_ou(block.affiliation_raw)` → WoS text → (faculty, ou)
4. `extract_ou_candidates(affiliation_raw)` → fallback OU ak WoS neobsahuje OU
5. Pre každého autora v UTB bloku: `match_author(name, registry, threshold, normalize=normalize)`
6. `lookup_author_affiliations(surname, firstname, remote_engine)` → fakulta/OU z remote DB
7. Validácia WoS fakulty voči remote DB → flag `wos_faculty_not_in_registry` pri nesúlade

Záznamy s nenájdenými UTB autormi dostanú `author_needs_llm=True`.

**Path B – bez WoS afiliácie:**
1. Pre každé meno v `dc.contributor.author`:
   `match_author(name, registry, threshold, normalize=normalize, require_surname_match=True)`
2. Fuzzy zhody (match_type=`"fuzzy"`) zaznamenané do flagu `path_b_low_confidence_matches`
3. `lookup_author_affiliations(surname, firstname, remote_engine)` → fakulta/OU z remote DB
4. Viacero fakúlt pre jedného autora → flag `multiple_faculties_ambiguous`

### `resolve_faculty_and_ou(affiliation_text) → (faculty, ou)`
Prehľadáva `WOS_ABBREV_NORM` a `DEPT_KEYWORD_MAP` (oddelenia dostávajú bonus +20 voči len-fakultovým zápisom). Fallback: `FACULTY_KEYWORD_RULES`. Vracia (plný anglický názov fakulty, plný názov oddelenia).

### `compare_with_librarian(engine=None)`
Porovná `author_internal_names` (program) vs `utb.contributor.internalauthor` (knihovník)
pre všetky záznamy s `author_heuristic_status='processed'`. Normalizuje obe množiny
(lowercase, bez diakritiky) a reportuje:
- `exact` – normalizované množiny totožné
- `partial` – neprázdny prienik, ale nie totožné
- `no_overlap` – obe neprázdne, prázdny prienik
- `only_prog` / `only_lib` – len jeden zdroj má hodnoty
- `both_empty` – oba prázdne

---

## Fáza 4b: Parsery afiliácií

### `parse_wos_affiliation(text, resource_id)` (`src/authors/parsers/wos.py`)
Parsuje WoS afiliačný reťazec vo formáte:
```
[Autor1; Autor2] Inštitúcia1, Mesto1; [Autor3] Inštitúcia2, Mesto2
```
Vracia `ParseResult` s:
- `blocks` – všetky afiliačné bloky
- `utb_blocks` – bloky identifikované ako UTB (`detect_utb_affiliation`)
- `warnings` – varovania (Viac UTB blokov, neštandardný formát, ...)
- `ok` – False ak format nebol rozpoznaný (→ `author_needs_llm=True`)

### `detect_utb_affiliation(text)` (`src/authors/parsers/wos.py`)
Hľadá UTB kľúčové slová ("Tomas Bata", "UTB", "Zlin", "Zl n" a pod.).
Vracia `(is_utb: bool, matched_keyword: str)`.

### `extract_ou_candidates(affiliation_text)` (`src/authors/parsers/wos.py`)
Extrahuje kandidátov na organizačnú jednotku (katedry, ústavy) z textu afiliácie.

### `parse_scopus_affiliation(text)` (`src/authors/parsers/scopus.py`)
Parsuje Scopus afiliačný reťazec (použitie: kontext pre LLM fázu).

---

## Fáza 5: Heuristiky – dátumy (`src/dates/heuristics.py`)

### `setup_date_columns(engine=None)`
Pridá DATE a LLM stĺpce pre dátumy (idempotentné). Zoznam stĺpcov v `DATE_COLUMNS`.

### `run_date_heuristics(engine, batch_size, limit, reprocess)`
**Hlavný runner pre parsovanie dátumov.**
Pre každý záznam volá `parse_fulltext_dates(resource_id, raw_text)`.
**Výstup do DB:** `utb_date_received`, `utb_date_reviewed`, `utb_date_accepted`,
`utb_date_published_online`, `utb_date_published`, `utb_date_extra`,
`date_heuristic_status`, `date_needs_llm`, `date_flags`,
`date_heuristic_version`, `date_processed_at`

### `parse_fulltext_dates(resource_id, raw_text, dc_issued=None)` (`src/dates/parser.py`)
**Jadro parsera dátumov.** Dvojpriechod:

**Priechod 1 – zbieranie MDR kandidátov:**
- `_split_into_segments(text)` – rozdelí podľa labelov (Received, Accepted, ...)
- Pre každý segment: `_try_parse_dot_both(date_text)` – skúsi obe interpretácie bodkového dátumu
- `resolve_mdr_format(mdr_candidates)` – určí formát (DMY/MDY) a confidence

**Priechod 2 – parsovanie so správnym formátom:**
- Pre bodkové dátumy: použije `use_dmy` flag z MDR resolvera
- Pre ostatné: `_try_parse_date(date_text)` – štandardný multi-regex parser
- `match_label(label_raw)` → mapuje label na `DateCategory`
- `_validate_chronology(parsed)` – kontroluje Received ≤ ... ≤ Published

### `_try_parse_dot_both(text) → (dmy, mdy) | None`
Extrahuje A.B.YYYY pomocou `_DOT_DATE_RE`. Vracia:
- `dmy = date(year, B, A)` – DD.MM.YYYY interpretácia
- `mdy = date(year, A, B)` – MM.DD.YYYY interpretácia
- `None` pre každú interpretáciu ak dáva neplatný dátum
- `None` (celé) ak text neobsahuje bodkový dátum

### `resolve_mdr_format(candidates) → (format, confidence, lib_flags)`
**Vstup:** `[(category, dmy_date, mdy_date)]`
**Výstup:** `(use_dmy: bool | None, confidence: str, librarian_flags: dict)`

Klasifikácia každého kandidáta:
- `forced_dmy`: len DMY platná (mdy=None, napr. deň 28 > 12)
- `forced_mdy`: len MDY platná (dmy=None)
- `invalid`: ani DMY ani MDY platná
- `ambiguous`: obe platné

Rozhodovanie:
| Situácia | Výsledok |
|----------|----------|
| Konflikt (forced_dmy + forced_mdy) | INVALID → `date_needs_llm=True` |
| Len forced_dmy | HIGH, DMY |
| Len forced_mdy | HIGH, MDY |
| DMY ok, MDY poruší chronológiu | MEDIUM, DMY (+ flag pre knižníka) |
| MDY ok, DMY poruší chronológiu | MEDIUM, MDY (+ flag pre knižníka) |
| Obe ok alebo len ambiguous | LOW (+ flag) → `date_needs_llm=True` |
| Ani jedna ok | INVALID → `date_needs_llm=True` |

### `_try_parse_date(text) → (date, day_exact, year_only) | None`
Multi-pattern parser pre nebodkové dátumy. Vzory (v poradí priority):
1. `iso_ymd` – `2018-03-15`
2. `dot_ymd` – `2018.03.15`
3. `dot_dmy_spaced` – `15. 3. 2018`
4. `dot_dmy` – `15.3.2018`
5. `day_of_month_year` – `15th of March 2018`
6. `day_month_year` – `15 March 2018`
7. `month_day_year` – `March 15, 2018`
8. `month_year_only` – `March 2018` (→ `day_exact=False`)
9. `year_only` – `2018` (→ `year_only=True`)

---

## Fáza 6: LLM – autori (`src/llm/tasks/authors.py`)

### `run_llm(engine, batch_size, limit, provider)`
**Spracováva záznamy kde `author_needs_llm=TRUE` a `author_llm_status='not_processed'`.**
**Volá:**
- `get_llm_client(provider)` → vytvorí LLM klienta
- `create_authors_session(client)` → session so systémovým promptom
- `get_author_registry(engine)` – register pre výber kandidátov + anti-halucináciu
- `process_llm_record(...)` pre každý záznam v dávke

**Výstup do DB:** `author_llm_status`, `author_llm_result`, `author_llm_processed_at`,
`author_internal_names` (COALESCE – zachová heuristické ak LLM vráti prázdne),
`author_faculty`, `author_ou` (COALESCE)

### `process_llm_record(resource_id, wos_aff, scopus_aff, flags, session, registry) → dict`
1. `_select_candidates(registry, unmatched_authors)` – vyberie ≤80 relevantných kandidátov z registra
   (surname prefix matching; ak žiadni nenájdení, vráti prvých 80 z registra)
2. `build_user_message(...)` – zostaví prompt s WoS/Scopus afiliáciou + zoznamom kandidátov
3. `session.ask(user_message)` → string odpoveď z LLM (1 retry pri chybe)
4. `parse_llm_json_output(response)` → parsuje JSON
5. `LLMResult(**parsed_dict)` → Pydantic validácia (`ValidationError` → `validation_error`, bez retry)
6. `_filter_by_registry(llm_result, registry)` → odfiltruje halucinované mená

### `_select_candidates(registry, unmatched_authors, max_candidates=80) → list[str]`
Pre každé nenájdené meno hľadá v registri záznamy s podobným priezviskom
(prefix matching ≥60% zhoda). Výsledky zoradí podľa skóre. Ak menej ako 10 kandidátov,
dopĺňa z registra.

### `build_user_message(...) → str`
Zostaví prompt: WoS afiliácia + Scopus afiliácia + kontext z heuristík (flagy)
+ `"Povolené mená interných autorov UTB"` (zoznam kandidátov).

---

## Fáza 7: LLM – dátumy (`src/llm/tasks/dates.py`)

### `run_date_llm(engine, batch_size, limit, provider, reprocess, include_dash)`
**Spracováva záznamy kde `date_needs_llm=TRUE` a `date_llm_status='not_processed'`.**
`include_dash=True` spracuje aj záznamy s `utb_fulltext_dates='{-}'`.

**Volá:**
- `create_dates_session(client)` – session so systémovým promptom pre dátumy
- `process_date_llm_record(...)` pre každý záznam

**Výstup do DB:** `date_llm_status`, `date_llm_result`, `date_llm_processed_at`,
`utb_date_received`, `utb_date_reviewed`, `utb_date_accepted`,
`utb_date_published_online`, `utb_date_published`

### `process_date_llm_record(resource_id, fulltext_dates, ..., session) → dict`
1. `build_date_user_message(...)` – prompt s raw textom dátumov + aktuálne parsované hodnoty
2. `session.ask(user_message)` → string odpoveď (1 retry pri chybe)
3. `parse_llm_json_output(response)` → parsuje JSON
4. `DateLLMResult(**parsed_dict)` → Pydantic validácia
5. `.to_date(field)` → konvertuje ISO string na `datetime.date`

### `DateLLMResult`
Pydantic model – každé pole je ISO `YYYY-MM-DD` alebo `""` (nie None, kvôli OpenAI strict mode).

---

## Fáza 8: Normalizácia žurnálov (`src/journals/normalizer.py`)

### `setup_journal_columns(engine=None)`
Pridá stĺpce `journal_norm_status`, `journal_norm_proposed_publisher`,
`journal_norm_proposed_ispartof`, `journal_norm_api_source`, `journal_norm_issn_key`,
`journal_norm_version`, `journal_norm_processed_at` (idempotentné).

### `run_journal_lookup(engine, limit, reprocess)`
**Lookupuje kanonické hodnoty publisher a relation.ispartof podľa ISSN/ISBN.**

1. Načíta záznamy s `journal_norm_status='not_processed'` (alebo všetky ak `reprocess=True`)
2. Zoskupí podľa prvého ISSN (resp. ISBN, alebo `no_id`)
3. Pre každú skupinu volá `_api_lookup_issn()` / `_api_lookup_isbn()` (Crossref → OpenAlex / Google Books → OpenLibrary)
4. Ak API zlyhá: `_pick_canonical_from_existing(rows)` – Scopus → WoS → most_common
5. `_build_update(row, canonical_pub, canonical_isp, source)` – porovná aktuálnu vs kanonickú hodnotu
6. Záznamy kde sa hodnota líši → `has_proposal`; kde sa zhoduje → `no_change`

**Výstup do DB:** `journal_norm_status`, `journal_norm_proposed_publisher`,
`journal_norm_proposed_ispartof`, `journal_norm_api_source`, `journal_norm_issn_key`,
`journal_norm_version`, `journal_norm_processed_at`

### `run_journal_apply(engine, preview, interactive, limit, issn_filter)`
**Zobrazí navrhnuté zmeny a aplikuje po schválení knihovníkom.**

Tri módy:
- `preview=True` – farebný ANSI diff, žiadny zápis
- `interactive=True` – pre každú ISSN skupinu zvlášť `y/n` prompt
- bez flagu – zobrazí všetko, jedno spoločné potvrdenie

Pre každú skupinu s `journal_norm_status='has_proposal'` volá `_print_group()` (diff).
Po schválení `_apply_rows()` zapíše kanonické hodnoty ako TEXT[] do `dc.publisher`
a `dc.relation.ispartof`.

### `_pick_canonical_from_existing(rows) → (publisher, ispartof, source_label)`
Fallback keď API nenájde nič:
1. Záznamy so Scopus afiliáciou → most_common hodnota
2. Záznamy s WoS afiliáciou → most_common hodnota
3. Všetky záznamy → most_common hodnota

### Lookup API (`src/journals/lookup.py`)

#### `lookup_by_issn(issn) → LookupResult | None`
Crossref (`api.crossref.org/journals/{issn}`) → OpenAlex (`api.openalex.org/sources`)

#### `lookup_by_isbn(isbn) → LookupResult | None`
Google Books (`googleapis.com/books/v1/volumes?q=isbn:`) → OpenLibrary (`openlibrary.org/api/books`)

`LookupResult(publisher, title, source)` – `title` sa mapuje na `dc.relation.ispartof`.

---

## LLM infraštruktúra (`src/llm/client.py`, `src/llm/session.py`)

### `get_llm_client(provider=None) → LLMClient`
Vytvorí `OllamaClient` alebo `CloudLLMCompatibleClient` podľa `LLM_PROVIDER` env var.

### `OllamaClient.chat(messages, schema) → str`
Pošle request na Ollama API (`/api/chat`) s `format: json_schema`. Vracia string obsah.

### `CloudLLMCompatibleClient.chat(messages, schema) → str`
OpenAI-kompatibilný klient. Fallback chain pre structured output:
1. `response_format: {type: json_schema, json_schema: ...}`
2. Function calling
3. `response_format: {type: json_object}`

Retry logic: 3 pokusy s exponenciálnym backoffom pre 429/5xx.

### `LLMSession.ask(user_message) → str`
Bez histórie medzi záznamami. Každý request je čistý kontext:
- Ollama: `[system, preamble..., user_record]`
- Cloud: `[system, user_record]`

### `parse_llm_json_output(response) → dict`
Parsuje JSON z odpovede LLM. Toleruje markdown code fences (` ```json ... ``` `).
Regex `r"\{.*\}"` s `re.DOTALL` extrahuje vonkajší JSON objekt (greedy = správne pre nested JSON).

---

## Fáza 9: Deduplikácia (`src/quality/dedup.py`)

### `setup_dedup_table(engine=None)`
Vytvorí `{schema}.dedup_histoire` ako prázdnu kópiu zdrojovej tabuľky + pridá stĺpce
`dedup_merged_at`, `dedup_match_type`, `dedup_kept_resource_id`, `dedup_other_resource_id`.
Idempotentné.

### `run_deduplication(engine, by_column, fuzzy_fallback, title_threshold, dry_run)`
**Hlavný runner deduplikácie – 3 fázy:**

**Fáza 1 – presná zhoda:**
- `find_duplicates_by_column(engine, by_column)` → skupiny s rovnakou hodnotou DOI
- Výsledok: `merge_pairs` s `match_type="exact:{column}"`

**Fáza 2 – obsahová zhoda:**
- `find_content_duplicates(engine)` → páry s identickým normalizovaným obsahom
- Pre `autoplagiat`: pridá do `flag_only`
- Pre `early_access`, `merged_type`, `exact:content`: `_fetch_record()` + pridá do `merge_pairs`

**Fáza 3 – fuzzy zhoda:**
- `find_duplicates_fuzzy(engine, title_threshold)` → páry s podobným titulom
- Všetky fuzzy výsledky idú do `flag_only`

**Zápis:**
- `merge_pairs` → `_merge_pair(raw_conn, ...)` pre každý pár (skip ak `deleted_id` už zmazaný)
- `flag_only` → `_write_duplicates_to_flags(engine, ...)`

### `find_duplicates_by_column(engine, by_column) → [(ids, col, val, score)]`
Načíta všetky neprázdne hodnoty stĺpca, zoskupí podľa normalizovanej hodnoty.
Vracia skupiny s > 1 záznamom.

### `find_content_duplicates(engine) → [(id_a, id_b, match_type, score, details)]`
1. Normalizuje title + authors + abstract pre každý záznam
2. Bucket-uje podľa MD5 normalizovaného titulu
3. V rámci každého bucketu porovnáva pármi (title 100% + autori + abstrakt)
4. Kategorizácia:
   - `early_access`: rovnaký ISSN, jeden nemá pagination
   - `merged_type`: iný ISSN, article vs conferenceObject
   - `autoplagiat`: iný ISSN, rovnaký typ
   - `exact:content`: rovnaký ISSN, obaja majú (alebo nemajú) pagination

### `find_duplicates_fuzzy(engine, title_threshold) → [(id_a, id_b, match_type, score, details)]`
1. Načíta tituly, roky, ISSN, ISBN
2. Blocking podľa roku ±1
3. Jaro-Winkler similarity na normalizovaných tituloch
4. Výsledky s score ≥ threshold: `fuzzy_title` / `fuzzy_title+issn` / `fuzzy_title+isbn`

### `_merge_pair(raw_conn, schema, table, kept_id, deleted_id, match_type, col_names, extra_updates)`
1. `_copy_to_history(...)` pre `kept_id` – kópia pred zmenou
2. `_copy_to_history(...)` pre `deleted_id` – kópia pred mazaním
3. `extra_updates` → UPDATE `kept_id` (napr. doplnenie pagination pri early_access)
4. `DELETE` záznam `deleted_id`

### `_write_duplicates_to_flags(engine, id_to_duplicates)`
JSONB merge: `author_flags = (author_flags - 'duplicates') || jsonb_build_object('duplicates', ...)`
pre každý flagovaný záznam.

---

## Pomocné moduly

### `src/db/engines.py`
- `get_local_engine()` → SQLAlchemy engine pre lokálnu DB (singleton)
- `get_remote_engine()` → SQLAlchemy engine pre remote DB (singleton)
- `test_connection(engine, name) → bool`

### `src/config/settings.py` – `Settings`
Pydantic-settings dataclass načítava z `.env`:
- `local_schema`, `local_table`, `remote_schema`, `remote_table`
- `local_db_*`, `remote_db_*` – prihlasovacie údaje
- `author_match_threshold`, `fuzzy_dedup_threshold`
- `llm_provider`, `local_llm_base_url`, `local_llm_model`
- `heuristics_batch_size`, `llm_batch_size`, `copy_batch_size`, `copy_limit`

### `src/common/constants.py`
- `FACULTIES` – `{faculty_id: full_english_name}` pre 7 UTB fakúlt
- `DEPARTMENTS` – plný anglický názov oddelenia → `faculty_id`
- `WOS_ABBREV_MAP` / `WOS_ABBREV_NORM` – WoS skratky → (dept_name, faculty_id)
- `DEPT_KEYWORD_MAP` – kľúčové slová → (dept_name, faculty_id); WoS skratky + plné názvy
- `FACULTY_KEYWORD_RULES` – fallback pravidlá pre rozpoznanie fakulty z WoS textu
- `CZECH_FACULTY_MAP` / `CZECH_FACULTY_MAP_NORM` – české názvy fakúlt → faculty_id
- `FACULTY_ENGLISH_TO_ID` – anglický názov fakulty → faculty_id
- `OUTPUT_COLUMNS` – stĺpce pridávané bootstrapom (všetky `author_*` + LLM stĺpce)
- `HeuristicStatus`, `LLMStatus`, `ValidationStatus`, `DateLLMStatus` – stavové konštanty
- `FlagKey` – konštanty pre kľúče JSONB flag diktov

---

## Dátové stĺpce – prehľad

| Stĺpec                            | Typ         | Fáza              | Popis                                                         |
|-----------------------------------|-------------|-------------------|---------------------------------------------------------------|
| `validation_status`               | TEXT        | validate          | `not_checked / ok / has_issues`                               |
| `validation_flags`                | JSONB       | validate          | Typy chýb + detaily                                           |
| `validation_suggested_fixes`      | JSONB       | validate          | Navrhnuté opravy `{col: {original, suggested, fix_type}}`     |
| `author_heuristic_status`         | TEXT        | heuristics        | `not_processed / processed / error`                           |
| `author_heuristic_version`        | TEXT        | heuristics        | Verzia heuristík (napr. `"4.0.0"`)                            |
| `author_heuristic_processed_at`   | TIMESTAMPTZ | heuristics        | Čas spracovania                                               |
| `author_needs_llm`                | BOOLEAN     | heuristics        | True ak autori neboli nájdení heuristikou (Path A unmatched)  |
| `author_dc_names`                 | TEXT[]      | heuristics        | Kópia `dc.contributor.author` v čase spracovania              |
| `author_internal_names`           | TEXT[]      | heuristics / llm  | Nájdení interní autori (`Priezvisko, Meno`)                   |
| `author_faculty`                  | TEXT[]      | heuristics / llm  | Anglické názvy fakúlt pre nájdených autorov                   |
| `author_ou`                       | TEXT[]      | heuristics / llm  | Anglické názvy oddelení pre nájdených autorov                 |
| `author_flags`                    | JSONB       | heuristics / dedup| Heuristické flagy + `duplicates` z deduplikácie               |
| `author_llm_status`               | TEXT        | heuristics-llm    | `not_processed / processed / error / validation_error`        |
| `author_llm_result`               | JSONB       | heuristics-llm    | Raw LLM odpoveď (na ladenie)                                  |
| `author_llm_processed_at`         | TIMESTAMPTZ | heuristics-llm    | Čas LLM spracovania                                           |
| `date_heuristic_status`           | TEXT        | dates             | `not_processed / processed / error`                           |
| `date_needs_llm`                  | BOOLEAN     | dates             | True ak dátumy neboli spoľahlivo sparsované                   |
| `utb_date_received`               | DATE        | dates / llm       | Dátum prijatia rukopisu                                       |
| `utb_date_reviewed`               | DATE        | dates / llm       | Dátum recenzie                                                |
| `utb_date_accepted`               | DATE        | dates / llm       | Dátum prijatia na publikáciu                                  |
| `utb_date_published_online`       | DATE        | dates / llm       | Dátum online publikácie                                       |
| `utb_date_published`              | DATE        | dates / llm       | Dátum tlačenej publikácie                                     |
| `utb_date_extra`                  | JSONB       | dates             | Ďalšie parsované dátumy (nezapadajúce do hlavných kategórií)  |
| `date_flags`                      | JSONB       | dates             | MDR confidence, lib. upozornenia, chrono warnings             |
| `date_llm_status`                 | TEXT        | dates-llm         | `not_processed / processed / error / validation_error`        |
| `date_llm_result`                 | JSONB       | dates-llm         | Raw LLM odpoveď (na ladenie)                                  |
| `journal_norm_status`             | TEXT        | journals          | `not_processed / no_change / has_proposal / applied / error`  |
| `journal_norm_proposed_publisher` | TEXT        | journals-lookup   | Kanonická hodnota publisher z API / existujúcich záznamov     |
| `journal_norm_proposed_ispartof`  | TEXT        | journals-lookup   | Kanonická hodnota relation.ispartof z API                     |
| `journal_norm_api_source`         | TEXT        | journals-lookup   | Zdroj (`crossref`, `openalex`, `google_books`, `existing_*`)  |
| `journal_norm_issn_key`           | TEXT        | journals-lookup   | ISSN/ISBN kľúč skupiny (napr. `"0002-9726"`)                  |

---

## FlagKey konštanty (`author_flags`, `date_flags`)

| Konštanta | Kľúč v JSON | Fáza | Popis |
|-----------|-------------|------|-------|
| `NO_WOS_DATA` | `no_wos_data` | heuristics Path B | Záznam nemá WoS afiliáciu |
| `PARSE_WARNINGS` | `wos_parse_warnings` | heuristics Path A | Varovania z WoS parsera |
| `MULTIPLE_UTB_BLOCKS` | `multiple_utb_blocks` | heuristics Path A | Viac UTB blokov v jednej afiliácii |
| `UNMATCHED_UTB_AUTHORS` | `utb_authors_unmatched` | heuristics Path A | Autori z UTB bloku nenájdení v registri |
| `MATCHED_UTB_AUTHORS` | `utb_authors_found_count` | heuristics | Počet nájdených interných autorov |
| `WOS_FACULTY_NOT_IN_REGISTRY` | `wos_faculty_not_in_registry` | heuristics Path A | WoS fakulta nezodpovedá remote DB |
| `MULTIPLE_FACULTIES_AMBIGUOUS` | `multiple_faculties_ambiguous` | heuristics Path B | Autor patrí do viacerých fakúlt |
| `PATH_B_LOW_CONFIDENCE` | `path_b_low_confidence_matches` | heuristics Path B | Fuzzy zhody bez WoS scopingu (na review) |
| `ERROR` | `error` | všetky | Výnimka počas spracovania |

---

## Tok dát – jeden záznam cez celú pipeline

```
resource_id=12345
  │
  ├─ [validate] dc.title="Venˇec studie"
  │     _fix_text_str("Venˇec studie")
  │       → standalone_diacritics: "ˇ" odstrán → "Venec studie"
  │     validation_status = "has_issues"
  │     validation_suggested_fixes["dc.title"] = {
  │       original: "Venˇec studie", suggested: "Venec studie",
  │       fix_type: "standalone_diacritics"
  │     }
  │
  ├─ [apply-fixes] dc.title ← "Venec studie"
  │     validation_status = "not_checked"
  │
  ├─ [validate again] → validation_status="ok"
  │
  ├─ [heuristics] utb.wos.affiliation prítomná (Path A)
  │     parse_wos_affiliation(affil_text)
  │       → blok: [Novak J] Tomas Bata Univ, Fac Technol, Zlin
  │     detect_utb_affiliation([...]) → UTB blok identifikovaný
  │     match_author("Novak J", registry)
  │       → exact_diacritic: InternalAuthor("Novák", "Jan")
  │     resolve_faculty_and_ou("Fac Technol")
  │       → ("Faculty of Technology", "")
  │     author_internal_names = ["Novák, Jan"]
  │     author_faculty = ["Faculty of Technology"]
  │     author_heuristic_status = "processed"
  │
  ├─ [dates] utb_fulltext_dates="Received: 7.2.2002; Accepted: 3.5.2002"
  │     _split_into_segments → [(Received, "7.2.2002"), (Accepted, "3.5.2002")]
  │     _try_parse_dot_both("7.2.2002") → (2002-02-07, 2002-07-02)
  │     _try_parse_dot_both("3.5.2002") → (2002-05-03, 2002-03-05)
  │     resolve_mdr_format(...)
  │       DMY: Feb07 ≤ May03 ✓  MDY: Jul02 > Mar05 ✗ → MEDIUM, DMY
  │     utb_date_received = 2002-02-07
  │     utb_date_accepted = 2002-05-03
  │     date_flags["mdr_format_resolved"] = {confidence:"medium", ...}
  │
  ├─ [journals-lookup] dc.identifier.issn = ["0001-6373"]
  │     _parse_issns → ["0001-6373"]
  │     Skupina: 0001-6373 → 15 záznamov
  │     lookup_by_issn("0001-6373") → Crossref: {publisher: "Wiley", title: "AIChE Journal"}
  │     dc.publisher aktuálne = ["Wiley-Blackwell"]  ≠ "Wiley"
  │     journal_norm_status = "has_proposal"
  │     journal_norm_proposed_publisher = "Wiley"
  │     journal_norm_proposed_ispartof = "AIChE Journal"
  │
  ├─ [journals-apply --interactive] → knihovník potvrdí y
  │     dc.publisher = ["Wiley"]
  │     dc.relation.ispartof = ["AIChE Journal"]
  │     journal_norm_status = "applied"
  │
  └─ [deduplicate] DOI="10.1234/example"
        find_duplicates_by_column → skupina [12345, 12399]
        _merge_pair(kept=12345, deleted=12399, type="exact:dc.identifier.doi")
          _copy_to_history(12345, ...)
          _copy_to_history(12399, ...)
          DELETE 12399
```
