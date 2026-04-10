"""Validácia kvality metadát a návrhy opráv.

Spúšťa sa PRED heuristickým spracovaním ako vstupná kontrola.

Kontroly (+ návrhy opráv uložené do validation_suggested_fixes):
  1.  Trailing spaces / non-breaking space  – vedúce/koncové biele znaky   → trim
  2.  Non-breaking space vo vnútri hodnoty  – U+00A0, U+202F, U+2007        → nahradiť medzerou
  3.  Dvojité medzery                        – "  " vo vnútri hodnoty        → single space
  4.  Mojibake                               – UTF-8 čítaný ako Latin-1      → ftfy.fix_text
  5.  Encoding chyby (PUA + wrong codepoints)                               → _CHAR_FIX_MAP
  6.  Osamotené diakritické znaky           – ˇ ˝ ˚ ˘ ˆ ´ ¨ ́  ̈            → odstrániť
  7.  Formát DOI                             – URL prefix / query params      → strip
  8.  URL query params                       – tracking parametre v URL       → strip
  9.  Rúra (|) vo vnútri hodnoty            – zvyšok CSV separátora          → flag
  10. Zátvorky v dc.title                   – cudzojaz. titul vmiešaný       → flag
  11. Formát WoS identifikátora             – utb.identifier.wok ≠ "000…"   → flag
  12. Interní autori ⊆ všetci autori        – len ak heuristika prebehla
  13. Interní autori existujú v registri    – len ak heuristika prebehla
  14. OBDID existuje v remote obd_publikace – batch check                    → flag

Výsledky → validation_status, validation_flags
Navrhnuté opravy → validation_suggested_fixes (JSONB)

  Formát validation_suggested_fixes:
  {
    "dc.identifier.doi": {
      "original": "https://doi.org/10.xxx?via=hub",
      "suggested": "10.xxx",
      "fix_type": "doi_url_cleanup"
    },
    "dc.title": {
      "original": ["Vˇenec"],
      "suggested": ["Venec"],
      "fix_type": "standalone_diacritic"
    }
  }

TODO (frontend): zobraziť original červenou, suggested zelenou pred potvrdením opravy.

Aplikovanie opráv:
  python -m src.cli apply-fixes [--preview] [--dry-run]
"""

from __future__ import annotations

import json
import re
import unicodedata
from datetime import datetime, timezone
from typing import Any

import ftfy
from sqlalchemy import text
from sqlalchemy.engine import Engine

from src.common.constants import QUEUE_TABLE
from src.config.settings import settings
from src.db.engines import get_local_engine, get_remote_engine

VALIDATION_VERSION = "1.2.0"

# -----------------------------------------------------------------------
# Stĺpce
# -----------------------------------------------------------------------

# Textové stĺpce – prechádzajú kompletnou textovou validáciou
_TEXT_COLUMNS: list[str] = [
    "dc.title",
    "dc.contributor.author",
    "dc.description.abstract",
    "dc.identifier.doi",
    "utb.wos.affiliation",
    "utb.scopus.affiliation",
    "utb.identifier.wok",
]

# URL stĺpce – čistenie query params
_URL_COLUMNS: list[str] = [
    "dc.identifier.uri",
]

# Všetky stĺpce pre SELECT vo validačnom runneri (okrem resource_id)
_FETCH_COLS: list[str] = [
    "dc.title",
    "dc.contributor.author",
    "dc.description.abstract",
    "dc.identifier.doi",
    "utb.wos.affiliation",
    "utb.scopus.affiliation",
    "utb.identifier.wok",
    "dc.identifier.uri",
    "author_dc_names",
    "author_internal_names",
]

# -----------------------------------------------------------------------
# Regexpy a mapy – detekcia + opravy
# -----------------------------------------------------------------------

# DOI formát: začína 10. + registrant + suffix, bez http prefixu
_DOI_RE = re.compile(r"^10\.\d{4,9}/[-._;()/:A-Za-z0-9]+$")

# Mojibake: klasické UTF-8 → Latin-1 prepisové vzory
_MOJIBAKE_RE = re.compile(
    "\ufffd"                               # U+FFFD replacement char
    "|\u00c3[\u0080-\u00bf\u00c0-\u00ff]"  # Ã + 0x80-0xFF (C3+xx ako Latin-1)
    "|\u00e2\u0080[^\\s]"                    # â€ + char (E2 80 xx ako Latin-1)
)

# Mapa opráv konkrétnych chybných znakov (PUA ligaturáty + encoding chyby).
# Zdokumentované v UTB DSpace import kontrole; platné pre česko-slovenský text.
# Poradie je relevantné pri reťazení opráv – dlhšie kľúče musia ísť skôr.
_CHAR_FIX_MAP: dict[str, str] = {
    "\ue09d": "ft",       # U+E09D PUA → ft ligature
    "\ue104": "fl",       # U+E104 PUA → fl ligature
    "\ue103": "fi",       # U+E103 PUA → fi ligature
    "\u0100": "\u201c",   # Ā U+0100 → " U+201C left double quotation mark
    "\u011c": "ř",        # Ĝ U+011C → ř (encoding chyba špecifická pre čes./slov.)
    "\u0124": "ů",        # Ĥ U+0124 → ů (encoding chyba špecifická pre čes./slov.)
    "\u0bc5": "\u2013",   # ௅ U+0BC5 → – U+2013 en dash
    "\u0131": "í",        # ı U+0131 Latin small dotless i → í (v čes./slov. kontexte)
}

# Regex pre detekciu kľúčov _CHAR_FIX_MAP
_ENCODING_CHARS_RE = re.compile(
    "[" + "".join(re.escape(c) for c in _CHAR_FIX_MAP) + "]"
)

# Osamotené diakritické znaky (modifikátory / combining) – nemajú čo robiť
# ako samostatné znaky; indikujú rozpad kompozitu pri enkódovaní.
_STANDALONE_DIACRITICS_RE = re.compile(
    "[\u02c7"   # ˇ  U+02C7  MODIFIER LETTER CARON
    "\u02dd"    # ˝  U+02DD  DOUBLE ACUTE ACCENT
    "\u02da"    # ˚  U+02DA  RING ABOVE
    "\u0301"    # ́   U+0301  COMBINING ACUTE ACCENT
    "\u00b4"    # ´  U+00B4  ACUTE ACCENT
    "\u02d8"    # ˘  U+02D8  BREVE
    "\u0308"    # ̈   U+0308  COMBINING DIAERESIS
    "\u00a8"    # ¨  U+00A8  DIAERESIS
    "\u02c6"    # ˆ  U+02C6  MODIFIER LETTER CIRCUMFLEX ACCENT
    "]"
)

# Non-breaking a iné "špeciálne" medzery
_NBSP_RE = re.compile("[\u00a0\u202f\u2007]")

# Dvojitá (alebo viacnásobná) medzera
_DOUBLE_SPACE_RE = re.compile(r" {2,}")

# Hranatá zátvorka v titule (napr. "Hlavný titul [iný jazyk]")
_BRACKETS_IN_TITLE_RE = re.compile(r"\[.+\]")

# Rúra | v hodnote (CSV separátor, ktorý sa dostal do samotnej hodnoty)
_PIPE_RE = re.compile(r"\|")

# URL prefxy pred DOI
_DOI_URL_PREFIXES: tuple[str, ...] = (
    "https://doi.org/",
    "http://doi.org/",
    "https://dx.doi.org/",
    "http://dx.doi.org/",
    "doi:",
)


# -----------------------------------------------------------------------
# Textová opravná pipeline
# -----------------------------------------------------------------------

def _fix_text_str(s: str) -> tuple[str, list[str]]:
    """
    Aplikuje všetky textové opravy na jeden reťazec v správnom poradí.
    Vracia (opravený_reťazec, zoznam_typov_opráv).
    Ak nie je potrebná žiadna oprava, vracia (pôvodný, []).
    """
    original = s
    applied: list[str] = []

    # 1. ftfy – UTF-8/Latin-1 mojibake
    fixed = ftfy.fix_text(s)
    if fixed != s:
        applied.append("mojibake")
        s = fixed

    # 2. Konkrétne chybné kódové body (PUA ligatúry, Ĝ→ř, Ĥ→ů, atď.)
    if _ENCODING_CHARS_RE.search(s):
        for bad, good in _CHAR_FIX_MAP.items():
            if bad in s:
                s = s.replace(bad, good)
        applied.append("encoding_chars")

    # 3. Osamotené diakritické znaky
    stripped_diac = _STANDALONE_DIACRITICS_RE.sub("", s)
    if stripped_diac != s:
        applied.append("standalone_diacritics")
        s = stripped_diac

    # 4. Non-breaking space → normálna medzera (musí byť pred double_space)
    fixed_nbsp = _NBSP_RE.sub(" ", s)
    if fixed_nbsp != s:
        applied.append("nbsp")
        s = fixed_nbsp

    # 5. Dvojitá medzera → jedna medzera
    fixed_double = _DOUBLE_SPACE_RE.sub(" ", s)
    if fixed_double != s:
        applied.append("double_space")
        s = fixed_double

    # 6. Trailing / leading whitespace (vrátane zvyškov po predošlých krokoch)
    stripped = s.strip()
    if stripped != s:
        applied.append("trailing_spaces")
        s = stripped

    if s == original:
        return original, []
    return s, applied


def _compute_text_fix(value: Any) -> tuple[Any, list[str]]:
    """
    Aplikuje _fix_text_str na skalár alebo každý prvok zoznamu.
    Vracia (fixed_value, zjednotený_zoznam_typov_opráv).
    """
    if isinstance(value, str):
        return _fix_text_str(value)

    if isinstance(value, list):
        fixed_items: list = []
        all_types:   set[str] = set()
        for item in value:
            if isinstance(item, str):
                fitem, types = _fix_text_str(item)
                fixed_items.append(fitem)
                all_types.update(types)
            else:
                fixed_items.append(item)
        return fixed_items, list(all_types)

    return value, []


# -----------------------------------------------------------------------
# Opravné funkcie (verejné – pre apply-fixes a testy)
# -----------------------------------------------------------------------

def fix_trailing_spaces(value: Any) -> Any:
    def _strip(s: str) -> str:
        return s.strip()
    if isinstance(value, str):
        return _strip(value)
    if isinstance(value, list):
        return [_strip(i) if isinstance(i, str) else i for i in value]
    return value


def fix_mojibake(value: Any) -> Any:
    """Aplikuje kompletnú textovú opravu (ftfy + encoding chars + diacritics + nbsp + double_space + strip)."""
    fixed, _ = _compute_text_fix(value)
    return fixed


def _clean_doi_str(doi: str) -> str:
    s = doi.strip()
    for prefix in _DOI_URL_PREFIXES:
        if s.lower().startswith(prefix):
            s = s[len(prefix):]
            break
    s = s.split("?")[0].split("#")[0].rstrip("/")
    return s


def _clean_url_str(url: str) -> str:
    return url.split("?")[0] if "?" in url else url


def fix_doi(value: Any) -> Any:
    if isinstance(value, list):
        return [_clean_doi_str(d) if isinstance(d, str) else d for d in value]
    if isinstance(value, str):
        return _clean_doi_str(value)
    return value


def fix_url(value: Any) -> Any:
    if isinstance(value, list):
        return [_clean_url_str(u) if isinstance(u, str) else u for u in value]
    if isinstance(value, str):
        return _clean_url_str(value)
    return value


# -----------------------------------------------------------------------
# Kontrolné funkcie (verejné – pre testy)
# -----------------------------------------------------------------------

def check_trailing_spaces(value: str) -> bool:
    """True ak reťazec má vedúce alebo koncové biele znaky."""
    return bool(value) and value != value.strip()


def check_mojibake(value: str) -> bool:
    """True ak reťazec obsahuje klasické UTF-8→Latin-1 vzory."""
    return bool(value) and bool(_MOJIBAKE_RE.search(value))


def check_doi_format(doi: str) -> bool:
    """True ak DOI zodpovedá formátu 10.XXXX/suffix (bez http prefixu)."""
    if not doi:
        return True
    return bool(_DOI_RE.match(doi.strip()))


def check_encoding_chars(value: str) -> bool:
    """True ak reťazec obsahuje znaky z _CHAR_FIX_MAP alebo osamotené diakritiky."""
    return bool(value) and (
        bool(_ENCODING_CHARS_RE.search(value))
        or bool(_STANDALONE_DIACRITICS_RE.search(value))
    )


def check_nbsp(value: str) -> bool:
    """True ak reťazec obsahuje non-breaking space."""
    return bool(value) and bool(_NBSP_RE.search(value))


def check_double_space(value: str) -> bool:
    """True ak reťazec obsahuje dvojitú (alebo viacnásobnú) medzeru."""
    return bool(value) and bool(_DOUBLE_SPACE_RE.search(value))


# -----------------------------------------------------------------------
# Pomocné funkcie pre validate_record
# -----------------------------------------------------------------------

def _normalize_name(name: str) -> str:
    nfd    = unicodedata.normalize("NFD", name)
    no_acc = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    return re.sub(r"\s+", " ", no_acc.lower()).strip()


def _scalar_str(value: Any) -> str | None:
    """Vráti prvý neprázdny reťazec z hodnoty (skalár alebo pole)."""
    if isinstance(value, list):
        return next((s for s in value if s), None)
    return value or None


# -----------------------------------------------------------------------
# Validácia jedného záznamu
# -----------------------------------------------------------------------

def validate_record(
    row_data:       dict[str, Any],
    registry_names: set[str],
) -> tuple[str, dict, dict]:
    """
    Zvaliduje jeden záznam.

    Vracia (status, issues, suggested_fixes):
      status:           'ok' | 'has_issues'
      issues:           dict s nájdenými problémami (kľúče = typ chyby)
      suggested_fixes:  dict s navrhnutými opravami pre frontend
    """
    issues:          dict[str, Any] = {}
    suggested_fixes: dict[str, Any] = {}

    # Akumulátory pre textové problémy (per stĺpec → hromadíme do issues)
    trailing_cols:    list[str] = []
    mojibake_cols:    list[str] = []
    encoding_cols:    list[str] = []
    diacritics_cols:  list[str] = []
    nbsp_cols:        list[str] = []
    dblspace_cols:    list[str] = []
    pipe_cols:        list[str] = []

    # --- Textová pipeline pre všetky _TEXT_COLUMNS ---
    for col in _TEXT_COLUMNS:
        value = row_data.get(col)
        if value is None:
            continue

        # Komplexná oprava v jednom prechode
        fixed, fix_types = _compute_text_fix(value)

        if fix_types:
            suggested_fixes[col] = {
                "original":  value,
                "suggested": fixed,
                "fix_type":  "+".join(sorted(set(fix_types))),
            }
            if "trailing_spaces"     in fix_types: trailing_cols.append(col)
            if "mojibake"            in fix_types: mojibake_cols.append(col)
            if "encoding_chars"      in fix_types: encoding_cols.append(col)
            if "standalone_diacritics" in fix_types: diacritics_cols.append(col)
            if "nbsp"                in fix_types: nbsp_cols.append(col)
            if "double_space"        in fix_types: dblspace_cols.append(col)

        # Rúra | vo hodnote (nekorigujeme automaticky – môže byť zámerné)
        vals = value if isinstance(value, list) else [value]
        if any(isinstance(v, str) and _PIPE_RE.search(v) for v in vals):
            pipe_cols.append(col)

    if trailing_cols:    issues["trailing_spaces"]       = trailing_cols
    if mojibake_cols:    issues["mojibake"]              = mojibake_cols
    if encoding_cols:    issues["encoding_chars"]        = encoding_cols
    if diacritics_cols:  issues["standalone_diacritics"] = diacritics_cols
    if nbsp_cols:        issues["nbsp"]                  = nbsp_cols
    if dblspace_cols:    issues["double_space"]          = dblspace_cols
    if pipe_cols:        issues["pipe_in_field"]         = pipe_cols

    # --- DOI formát + URL prefix + query params ---
    doi_raw = row_data.get("dc.identifier.doi")
    doi_str = _scalar_str(doi_raw)

    if doi_str:
        if not check_doi_format(doi_str):
            issues["invalid_doi"] = doi_str
            cleaned = _clean_doi_str(doi_str)
            if _DOI_RE.match(cleaned):
                suggested_fixes["dc.identifier.doi"] = {
                    "original":  doi_raw,
                    "suggested": [cleaned] if isinstance(doi_raw, list) else cleaned,
                    "fix_type":  "doi_url_cleanup",
                }
        elif "?" in doi_str or any(doi_str.lower().startswith(p) for p in _DOI_URL_PREFIXES):
            cleaned = _clean_doi_str(doi_str)
            if cleaned != doi_str:
                suggested_fixes["dc.identifier.doi"] = {
                    "original":  doi_raw,
                    "suggested": [cleaned] if isinstance(doi_raw, list) else cleaned,
                    "fix_type":  "doi_url_cleanup",
                }

    # --- URL stĺpce – query params (vrátane ?via%3Dihub a podobných) ---
    for col in _URL_COLUMNS:
        value = row_data.get(col)
        if value is None:
            continue
        fixed = fix_url(value)
        if fixed != value:
            suggested_fixes[col] = {
                "original":  value,
                "suggested": fixed,
                "fix_type":  "url_query_params",
            }

    # --- Zátvorky v dc.title (cudzojaz. titul vmiešaný do hlavného poľa) ---
    title_val = row_data.get("dc.title")
    title_str = _scalar_str(title_val)
    if title_str and _BRACKETS_IN_TITLE_RE.search(title_str):
        issues["brackets_in_title"] = title_str

    # --- WoS identifikátor (utb.identifier.wok) musí začínať "000" ---
    wos_val = row_data.get("utb.identifier.wok")
    wos_str = _scalar_str(wos_val)
    if wos_str and wos_str.strip() and not wos_str.strip().startswith("0"):
        issues["invalid_wos_id"] = wos_str.strip()

    # --- Interní autori ⊆ všetci autori (len ak heuristika prebehla) ---
    internal_authors: list[str] = row_data.get("author_internal_names") or []
    all_authors:      list[str] = row_data.get("author_dc_names")       or []

    if internal_authors and all_authors:
        all_norms = {_normalize_name(a) for a in all_authors if a}
        not_found = []
        for ia in internal_authors:
            norm_ia       = _normalize_name(ia)
            surname_pfx   = norm_ia.split(",")[0].strip()[:6] if "," in norm_ia else norm_ia[:6]
            if surname_pfx and not any(surname_pfx in na for na in all_norms):
                not_found.append(ia)
        if not_found:
            issues["internal_not_in_authors"] = not_found

    # --- Interní autori existujú v registri ---
    if internal_authors and registry_names:
        not_in_registry = [ia for ia in internal_authors if ia not in registry_names]
        if not_in_registry:
            issues["authors_not_in_registry"] = not_in_registry

    status = "ok" if not issues else "has_issues"
    return status, issues, suggested_fixes


# -----------------------------------------------------------------------
# OBDID batch kontrola (remote)
# -----------------------------------------------------------------------

def check_obdid_batch(
    engine:        Engine,
    remote_engine: Engine,
) -> dict[int, list[int]]:
    """
    Skontroluje existenciu OBDID v remote veda.obd_publikace.id.

    Stĺpec "utb.identifier.obdid" je TEXT alebo TEXT[] v lokálnej DB.
    Stĺpec veda.obd_publikace.id je NUMERIC v remote DB.
    Vracia {resource_id: [invalid_obdid, ...]} pre záznamy s neplatným OBDID.
    """
    schema = settings.local_schema
    table  = settings.local_table

    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT resource_id, "utb.identifier.obdid"
            FROM "{schema}"."{table}"
            WHERE "utb.identifier.obdid" IS NOT NULL
        """)).fetchall()

    if not rows:
        return {}

    obdid_map: dict[int, list[int]] = {}
    for row in rows:
        raw        = row[1]
        candidates = raw if isinstance(raw, list) else [raw]
        valid_ints = [int(str(v).strip()) for v in candidates
                      if v is not None and str(v).strip().isdigit()]
        if valid_ints:
            obdid_map[row.resource_id] = valid_ints

    if not obdid_map:
        return {}

    all_ids = list({i for lst in obdid_map.values() for i in lst})

    # Oracle (cez FDW) má limit 1000 výrazov v ANY/IN – rozdelíme do chunkov.
    _CHUNK = 999
    valid_ids: set[int] = set()
    with remote_engine.connect() as conn:
        for offset in range(0, len(all_ids), _CHUNK):
            chunk = all_ids[offset : offset + _CHUNK]
            valid_ids.update(
                conn.execute(
                    text("SELECT id FROM publikace.veda.obd_publikace WHERE id = ANY(:ids)"),
                    {"ids": chunk},
                ).scalars()
            )

    return {
        rid: [i for i in ids if i not in valid_ids]
        for rid, ids in obdid_map.items()
        if any(i not in valid_ids for i in ids)
    }


# -----------------------------------------------------------------------
# Migrácia DB stĺpcov
# -----------------------------------------------------------------------

def setup_validation_columns(engine: Engine | None = None) -> None:
    """
    Validation stĺpce sú teraz v utb_processing_queue.
    Spusti 'queue-setup' namiesto tohto príkazu.
    """
    print("[INFO] Validation stĺpce sú v utb_processing_queue. Spusti 'queue-setup'.")
    print("[INFO] Príkaz validate-setup je zastaraný – môžeš ho ignorovať.")


# -----------------------------------------------------------------------
# Hlavný runner – validácia
# -----------------------------------------------------------------------

def run_validation(
    engine:        Engine | None = None,
    remote_engine: Engine | None = None,
    batch_size:    int           = 500,
    limit:         int           = 0,
    revalidate:    bool          = False,
) -> None:
    engine        = engine        or get_local_engine()
    remote_engine = remote_engine or get_remote_engine()
    schema = settings.local_schema
    table  = settings.local_table
    queue  = QUEUE_TABLE

    from src.authors.registry import get_author_registry
    registry_names: set[str] = {a.full_name for a in get_author_registry(engine)}

    # Stĺpce z hlavnej tabuľky (obsah)
    _MAIN_COLS = [c for c in _FETCH_COLS if c not in ("author_dc_names", "author_internal_names")]
    # Stĺpce z queue (výstupy heuristík)
    _QUEUE_COLS = ["author_dc_names", "author_internal_names"]

    main_cols_sql  = ", ".join(f'm."{c}" AS "{c}"' if "." in c else f'm."{c}"' for c in _MAIN_COLS)
    queue_cols_sql = ", ".join(f'q."{c}"' for c in _QUEUE_COLS)

    where_queue = "" if revalidate else "WHERE q.validation_status = 'not_checked'"

    with engine.connect() as conn:
        total = conn.execute(
            text(f'SELECT COUNT(*) FROM "{schema}"."{queue}" q {where_queue}')
        ).scalar_one()

    if limit > 0:
        total = min(total, limit)

    if total == 0:
        print("[INFO] Žiadne záznamy na validáciu.")
    else:
        print(f"[INFO] Záznamov na validáciu: {total}")

        select_sql = f"""
            SELECT m.resource_id, {main_cols_sql}, {queue_cols_sql}
            FROM "{schema}"."{table}" m
            JOIN "{schema}"."{queue}" q ON m.resource_id = q.resource_id
            {where_queue}
            ORDER BY m.resource_id
            LIMIT :lim
        """

        update_sql = f"""
            UPDATE "{schema}"."{queue}"
            SET
                validation_status          = %s,
                validation_flags           = %s::jsonb,
                validation_suggested_fixes = %s::jsonb,
                validation_version         = %s,
                validation_checked_at      = %s
            WHERE resource_id = %s
        """

        processed    = 0
        issues_count = 0

        while processed < total:
            batch = min(batch_size, total - processed)

            with engine.connect() as conn:
                rows = conn.execute(text(select_sql), {"lim": batch}).fetchall()

            if not rows:
                break

            params = []
            for row in rows:
                row_data = {col: getattr(row, col, None) for col in _FETCH_COLS}

                status, issues, suggested_fixes = validate_record(row_data, registry_names)
                if status == "has_issues":
                    issues_count += 1

                params.append((
                    status,
                    json.dumps(issues,          ensure_ascii=False),
                    json.dumps(suggested_fixes, ensure_ascii=False),
                    VALIDATION_VERSION,
                    datetime.now(timezone.utc),
                    row.resource_id,
                ))

            raw = engine.raw_connection()
            try:
                with raw.cursor() as cur:
                    cur.executemany(update_sql, params)
                raw.commit()
            finally:
                raw.close()

            processed += len(rows)
            print(f"  Spracované: {processed}/{total} | s problémami: {issues_count}")

        print(f"[OK] Validácia hotová. Spracovaných: {processed}, s problémami: {issues_count}")

    # --- OBDID batch kontrola ---
    print("[INFO] Kontrolujem OBDID voči remote DB...")
    try:
        invalid_obdids = check_obdid_batch(engine, remote_engine)
        if invalid_obdids:
            print(f"  Neplatné OBDID v {len(invalid_obdids)} záznamoch – zapisujem flag...")
            raw = engine.raw_connection()
            try:
                with raw.cursor() as cur:
                    for resource_id, bad_ids in invalid_obdids.items():
                        cur.execute(
                            f"""
                            UPDATE "{schema}"."{queue}"
                            SET
                                validation_status = CASE
                                    WHEN validation_status = 'ok' THEN 'has_issues'
                                    ELSE validation_status
                                END,
                                validation_flags = validation_flags
                                    || jsonb_build_object('obdid_not_in_remote', %s::jsonb)
                            WHERE resource_id = %s
                            """,
                            (json.dumps(bad_ids), resource_id),
                        )
                raw.commit()
            finally:
                raw.close()
            print(f"  [OK] OBDID flag zapísaný pre {len(invalid_obdids)} záznamov.")
        else:
            print("  [OK] Všetky OBDID nájdené v remote DB.")
    except Exception as exc:
        print(f"  [WARN] OBDID kontrola zlyhala: {exc}")


# -----------------------------------------------------------------------
# Aplikovanie opráv
# -----------------------------------------------------------------------

_ANSI_RED   = "\033[91m"
_ANSI_GREEN = "\033[92m"
_ANSI_RESET = "\033[0m"


def run_apply_fixes(
    engine:  Engine | None = None,
    preview: bool          = False,
    dry_run: bool          = False,
    limit:   int           = 0,
) -> None:
    """
    Aplikuje navrhnuté opravy z validation_suggested_fixes do skutočných stĺpcov.

    --preview / --dry-run: Vypíše farebný diff (červená = pôvodná, zelená = navrhnutá)
                           bez zápisu do DB.
    Po aplikovaní sa záznamy nastavia na validation_status = 'not_checked' (re-validácia).

    TODO (frontend): zobraziť diff farebne v UI pred potvrdením.
    """
    engine = engine or get_local_engine()
    schema = settings.local_schema
    table  = settings.local_table
    queue  = QUEUE_TABLE
    is_dry = preview or dry_run

    # Čítame návrhy z queue (nie z hlavnej tabuľky)
    with engine.connect() as conn:
        rows = conn.execute(text(f"""
            SELECT resource_id, validation_suggested_fixes
            FROM "{schema}"."{queue}"
            WHERE validation_suggested_fixes IS NOT NULL
              AND validation_suggested_fixes != '{{}}'::jsonb
            ORDER BY resource_id
            {f"LIMIT {limit}" if limit > 0 else ""}
        """)).fetchall()

    if not rows:
        print("[INFO] Žiadne záznamy s navrhnutými opravami.")
        return

    print(f"[INFO] Záznamy na opravu: {len(rows)}")
    applied = 0

    raw = None if is_dry else engine.raw_connection()
    try:
        for row in rows:
            resource_id = row.resource_id
            fixes: dict = row.validation_suggested_fixes or {}
            if not fixes:
                continue

            if is_dry:
                print(f"\n  resource_id {resource_id}:")
                for field, fix_data in fixes.items():
                    orig = json.dumps(fix_data.get("original",  ""), ensure_ascii=False)
                    sugg = json.dumps(fix_data.get("suggested", ""), ensure_ascii=False)
                    ftype = fix_data.get("fix_type", "")
                    print(f"    [{ftype}] {field}:")
                    print(f"      {_ANSI_RED}{orig}{_ANSI_RESET}")
                    print(f"      {_ANSI_GREEN}{sugg}{_ANSI_RESET}")
            else:
                with raw.cursor() as cur:
                    # Opravy obsahu idú do hlavnej tabuľky
                    for field, fix_data in fixes.items():
                        cur.execute(
                            f'UPDATE "{schema}"."{table}" SET "{field}" = %s WHERE resource_id = %s',
                            (fix_data["suggested"], resource_id),
                        )
                    # Reset statusu v queue
                    cur.execute(
                        f"""
                        UPDATE "{schema}"."{queue}"
                        SET
                            validation_suggested_fixes = '{{}}'::jsonb,
                            validation_status          = 'not_checked'
                        WHERE resource_id = %s
                        """,
                        (resource_id,),
                    )
                applied += 1

        if not is_dry:
            raw.commit()

    finally:
        if raw:
            raw.close()

    if is_dry:
        print(f"\n[DRY RUN] Žiadne zmeny. ({len(rows)} záznamov by bolo opravených)")
    else:
        print(f"[OK] Opravených: {applied}. Spusti 'validate' pre re-validáciu.")


# -----------------------------------------------------------------------
# Štatistiky
# -----------------------------------------------------------------------

def print_validation_status(engine: Engine | None = None) -> None:
    engine = engine or get_local_engine()
    schema = settings.local_schema
    queue  = QUEUE_TABLE

    with engine.connect() as conn:
        status_rows = conn.execute(text(f"""
            SELECT validation_status, COUNT(*) AS cnt
            FROM "{schema}"."{queue}"
            GROUP BY validation_status
            ORDER BY cnt DESC
        """)).fetchall()

    print("\n=== Štatistiky validácie ===")
    for r in status_rows:
        print(f"  {(r.validation_status or 'NULL'):20s} | {r.cnt:6d}")

    with engine.connect() as conn:
        pending = conn.execute(text(f"""
            SELECT COUNT(*) FROM "{schema}"."{queue}"
            WHERE validation_suggested_fixes IS NOT NULL
              AND validation_suggested_fixes != '{{}}'::jsonb
        """)).scalar_one()
    print(f"\n  Záznamy s čakajúcimi opravami: {pending}")

    with engine.connect() as conn:
        issue_rows = conn.execute(text(f"""
            SELECT validation_flags
            FROM "{schema}"."{queue}"
            WHERE validation_status = 'has_issues'
        """)).fetchall()

    if issue_rows:
        counters: dict[str, int] = {}
        for row in issue_rows:
            for key in (row[0] or {}):
                counters[key] = counters.get(key, 0) + 1
        print("\n  Rozpad typov problémov:")
        for key, count in sorted(counters.items(), key=lambda x: -x[1]):
            print(f"    {key:35s}: {count:6d}")
