"""
Parser pre stĺpec utb_fulltext_dates.

ARCHITEKTÚRA – label-first prístup:
  Namiesto hľadania dátumov v texte hľadáme najprv LABELY (known keyword patterns),
  rozdelíme text na segmenty podľa labelov a potom parsujeme dátum v každom segmente.

MDR FORMAT RESOLVER:
  Bodkové dátumy (A.B.YYYY) sú ambivalentné: môžu byť DD.MM.YYYY alebo MM.DD.YYYY.
  Resolver to rieši dvoma krokmi:
    1. Ak hodnota > 12 vynucuje interpretáciu (HIGH confidence).
    2. Inak: chronologické obmedzenie Received ≤ Accepted ≤ Published (MEDIUM).
    3. Ak obe interpretácie konzistentné → LOW, knižníkovi sa pošle upozornenie.
  Výsledok sa ukladá do ParsedDates.mdr_format a .mdr_confidence.
  LOW a INVALID → needs_llm = True.
"""
from __future__ import annotations
import re, unicodedata
from dataclasses import dataclass, field
from datetime import date
from typing import Optional

# ── MDR (Month-Day / Day-Month) konštanty ─────────────────────────────────────
MDR_FORMAT_DMY = "DMY"   # DD.MM.YYYY – európsky formát
MDR_FORMAT_MDY = "MDY"   # MM.DD.YYYY – americký formát

MDR_CONF_HIGH    = "high"    # Jednoznačné (hodnota > 12 vynucuje formát)
MDR_CONF_MEDIUM  = "medium"  # Určené chronologickým obmedzením
MDR_CONF_LOW     = "low"     # Nejednoznačné, obe interpretácie platné
MDR_CONF_INVALID = "invalid" # Žiadna platná interpretácia

# ── Jazykové mapy ─────────────────────────────────────────────────────────────
class DateCategory:
    RECEIVED="received"; REVIEWED="reviewed"; ACCEPTED="accepted"
    PUBLISHED_ONLINE="published_online"; PUBLISHED="published"
    EXTRA="extra"; UNKNOWN="unknown"

MONTHS_EN = {
    "january":1,"jan":1,"february":2,"feb":2,"march":3,"mar":3,
    "april":4,"apr":4,"may":5,"june":6,"jun":6,"july":7,"jul":7,
    "august":8,"aug":8,"september":9,"sep":9,"sept":9,
    "october":10,"oct":10,"november":11,"nov":11,"december":12,"dec":12,
    "enero":1,"febrero":2,"marzo":3,"abril":4,"mayo":5,"junio":6,
    "julio":7,"agosto":8,"septiembre":9,"octubre":10,"noviembre":11,"diciembre":12,
    "i":1,"ii":2,"iii":3,"iv":4,"v":5,"vi":6,
    "vii":7,"viii":8,"ix":9,"x":10,"xi":11,"xii":12,
}

DATE_REGEX_PATTERNS = [
    ("iso_ymd",          r"(?P<year>\d{4})-(?P<month>\d{1,2})-(?P<day>\d{1,2})"),
    ("dot_ymd",          r"(?P<year>\d{4})\.(?P<month>\d{1,2})\.(?P<day>\d{1,2})"),
    ("dot_dmy_spaced",   r"(?P<day>\d{1,2})\.\s*(?P<month>\d{1,2})\.\s*(?P<year>\d{4})"),
    ("dot_dmy",          r"(?P<day>\d{1,2})\.(?P<month>\d{1,2})\.(?P<year>\d{4})"),
    ("day_of_month_year",r"(?P<day>\d{1,2})(?:st|nd|rd|th)?\s+of\s+(?P<month>[A-Za-záčýž]+\.?)\s*,?\s*(?P<year>\d{4})"),
    ("day_month_year",   r"(?P<day>\d{1,2})(?:st|nd|rd|th)?\s+(?P<month>[A-Za-záčýžA-ZÁČÝŽ]+\.?)\s*,?\s*(?P<year>\d{4})"),
    ("month_day_year",   r"(?P<month>[A-Za-záčýžA-ZÁČÝŽ]+\.?)\s*,?\s*(?P<day>\d{1,2})(?:st|nd|rd|th)?\s*,\s*(?P<year>\d{4})"),
    ("month_year_only",  r"(?P<month>[A-Za-záčýžA-ZÁČÝŽ]+\.?)\s*,?\s*(?P<year>\d{4})"),
    ("year_only",        r"\{?\s*(?P<year>20\d{2}|19\d{2})\s*\}?"),
]

LABEL_MAP = {
    "received": DateCategory.RECEIVED, "manuscript received": DateCategory.RECEIVED,
    "paper received": DateCategory.RECEIVED, "article submitted": DateCategory.RECEIVED,
    "submitted": DateCategory.RECEIVED, "doslo": DateCategory.RECEIVED,
    "do redakce doslo": DateCategory.RECEIVED, "do redakce doslo dne": DateCategory.RECEIVED,
    "clanek prijat redakci": DateCategory.RECEIVED, "primljeno": DateCategory.RECEIVED,
    "fecha de recepcion": DateCategory.RECEIVED,
    "received in revised form": DateCategory.REVIEWED,
    "revised manuscript received": DateCategory.REVIEWED,
    "editorial decision": DateCategory.REVIEWED, "prijato k recenzi": DateCategory.REVIEWED,
    "accepted": DateCategory.ACCEPTED, "accepted for publication": DateCategory.ACCEPTED,
    "accepted for publication on": DateCategory.ACCEPTED,
    "accepted manuscript online": DateCategory.ACCEPTED,
    "accepted author version posted online": DateCategory.ACCEPTED,
    "accepted in revised form": DateCategory.ACCEPTED, "accepted on": DateCategory.ACCEPTED,
    "final acceptance": DateCategory.ACCEPTED, "approved for publication": DateCategory.ACCEPTED,
    "prijato do tisku": DateCategory.ACCEPTED, "prijato": DateCategory.ACCEPTED,
    "clanek prijat k publikaci": DateCategory.ACCEPTED,
    "fecha de aceptacion": DateCategory.ACCEPTED, "odobreno": DateCategory.ACCEPTED,
    "published online": DateCategory.PUBLISHED_ONLINE,
    "available online": DateCategory.PUBLISHED_ONLINE, "online": DateCategory.PUBLISHED_ONLINE,
    "version of record online": DateCategory.PUBLISHED_ONLINE,
    "previously published online": DateCategory.PUBLISHED_ONLINE,
    "e-published": DateCategory.PUBLISHED_ONLINE, "epublished": DateCategory.PUBLISHED_ONLINE,
    "zverejneno": DateCategory.PUBLISHED_ONLINE,
    "published": DateCategory.PUBLISHED, "date of publication": DateCategory.PUBLISHED,
    "article publication date": DateCategory.PUBLISHED, "first published": DateCategory.PUBLISHED,
    "publication in this collection": DateCategory.PUBLISHED,
    "revised": DateCategory.REVIEWED, "resubmitted": DateCategory.REVIEWED,
    "prepracovano": DateCategory.REVIEWED, "date of current version": DateCategory.EXTRA,
    "issue publication date": DateCategory.EXTRA, "date of issue": DateCategory.EXTRA,
}

def normalize_label(label):
    nfd = unicodedata.normalize("NFD", label.lower())
    no_acc = "".join(c for c in nfd if unicodedata.category(c) != "Mn")
    clean = re.sub(r"[:\.\s]+$","",no_acc)
    return re.sub(r"\s+"," ",clean).strip()

def match_label(raw_label):
    norm = normalize_label(raw_label)
    if not norm: return DateCategory.UNKNOWN
    if norm in LABEL_MAP: return LABEL_MAP[norm]
    for key in sorted(LABEL_MAP, key=len, reverse=True):
        if norm.startswith(key): return LABEL_MAP[key]
    for key in sorted(LABEL_MAP, key=len, reverse=True):
        if key in norm: return LABEL_MAP[key]
    return DateCategory.UNKNOWN

# ── Dátové štruktúry ──────────────────────────────────────────────────────────
@dataclass
class DateEntry:
    raw_label: str; raw_date: str; category: str; parsed: Optional[date]
    day_exact: bool = True; year_only: bool = False
    def to_iso(self): return self.parsed.isoformat() if self.parsed else None

@dataclass
class ParsedDates:
    resource_id: int; raw_text: str
    received:         Optional[date] = None
    reviewed:         Optional[date] = None
    accepted:         Optional[date] = None
    published_online: Optional[date] = None
    published:        Optional[date] = None
    all_entries:      list  = field(default_factory=list)
    flags:            dict  = field(default_factory=dict)
    needs_llm:        bool  = False
    status:           str   = "not_processed"
    # MDR (Month-Day / Day-Month) výsledok
    mdr_format:       Optional[str] = None  # "DMY" | "MDY" | None
    mdr_confidence:   Optional[str] = None  # "high" | "medium" | "low" | "invalid" | None

# ── Label-first rozdelenie ─────────────────────────────────────────────────────
_LABEL_PATTERNS = [
    r"Received\s+in\s+revised\s+form",
    r"Revised\s+manuscript\s+received", r"Manuscript\s+received",
    r"Paper\s+received", r"Article\s+submitted",
    r"Article\s+publication\s+date",
    r"Accepted\s+author\s+version\s+posted\s+online",
    r"Accepted\s+manuscript\s+online",
    r"Accepted\s+for\s+publication\s+on", r"Accepted\s+for\s+publication",
    r"Accepted\s+in\s+revised\s+form", r"Accepted\s+on",
    r"Final\s+acceptance", r"Approved\s+for\s+publication",
    r"Available\s+[Oo]nline", r"Published\s+[Oo]nline",
    r"Version\s+of\s+record\s+online",
    r"Previously\s+published\s+online",
    r"Date\s+of\s+[Pp]ublication", r"Date\s+of\s+current\s+version",
    r"Date\s+of\s+issue", r"Issue\s+publication\s+date",
    r"First\s+[Pp]ublished",
    r"Publication\s+in\s+this\s+collection",
    r"Editorial\s+decision",
    r"\d(?:st|nd|rd|th)\s+Revision",
    r"E-?published",
    r"Do\s+redakce\s+do(?:š|s)lo\s+dne", r"Do\s+redakce\s+do(?:š|s)lo",
    r"[Čc]l[aá]nek\s+p[rř]ijat\s+k\s+publikaci",
    r"[Čc]l[aá]nek\s+p[rř]ijat\s+redakc[íi]",
    r"P[rř]ijato\s+k\s+recenzi", r"P[rř]ijato\s+do\s+tisku",
    r"p[rř]epracov[aá]no", r"p[rř]ijato",
    r"Do(?:š|s)lo",
    r"zve[rř]ejn[eě]no",
    r"Primljeno", r"Odobreno",
    r"Fecha\s+de\s+recepci[oó]n", r"Fecha\s+de\s+aceptaci[oó]n",
    r"date\s+of\s+publication", r"date\s+of\s+current\s+version",
    r"Resubmitted", r"Submitted",
    r"Revised",
    r"Accepted", r"Received", r"Published", r"Online",
]
_LABEL_RE = re.compile(
    r"(?<!\w)(" + "|".join(_LABEL_PATTERNS) + r")(?:\s*:)?\s*",
    re.IGNORECASE,
)

def _clean_input(raw):
    text = raw.strip().lstrip('{"').rstrip('"}')
    text = text.replace('""', '"')
    text = re.sub(r'[\n\r\t]+',' ',text)
    return re.sub(r' {2,}',' ',text).strip()

def _split_into_segments(text):
    matches = list(_LABEL_RE.finditer(text))
    if not matches: return []
    segments = []
    for i, m in enumerate(matches):
        label_raw = m.group(1).strip()
        date_start = m.end()
        date_end = matches[i+1].start() if i+1 < len(matches) else len(text)
        raw_date = re.sub(r'[.;,\s]+$','', text[date_start:date_end].strip())
        segments.append((label_raw, raw_date))
    return segments

def _try_parse_date(text):
    """Parsuje neparametrický (nie bodkový A.B.YYYY) dátum. Vracia (date, day_exact, year_only) alebo None."""
    text = text.strip()
    if re.search(r'00th|20xx|&&|0000', text, re.IGNORECASE): return None
    for pattern_name, pattern in DATE_REGEX_PATTERNS:
        m = re.search(pattern, text, re.IGNORECASE)
        if not m: continue
        g = m.groupdict()
        year_str, month_str, day_str = g.get("year"), g.get("month"), g.get("day")
        if not year_str: continue
        try: year = int(year_str)
        except: continue
        if not (1990 <= year <= 2030): continue
        if pattern_name == "year_only":
            try: return date(year,1,1), False, True
            except: continue
        month = 0
        if month_str:
            mn = month_str.lower().rstrip('.')
            month = MONTHS_EN.get(mn, 0)
            if not month:
                try: month = int(month_str)
                except: pass
        if not (1 <= month <= 12): continue
        if pattern_name == "month_year_only" or not day_str:
            try: return date(year,month,1), False, False
            except: continue
        day = 0
        if day_str:
            dc = re.sub(r'(?:st|nd|rd|th)$','',day_str.strip())
            try: day = int(dc)
            except: continue
        if not (1 <= day <= 31): continue
        try: return date(year,month,day), True, False
        except: continue
    return None

# ── MDR Format Resolver ────────────────────────────────────────────────────────

# Regex pre bodkový dátum A.B.YYYY (európsky alebo americký)
_DOT_DATE_RE = re.compile(r'(\d{1,2})\.\s*(\d{1,2})\.\s*(\d{4})')

# Poradie kategórií pre chronologické obmedzenie
_CAT_ORDER = [
    DateCategory.RECEIVED,
    DateCategory.REVIEWED,
    DateCategory.ACCEPTED,
    DateCategory.PUBLISHED_ONLINE,
    DateCategory.PUBLISHED,
]


def _try_parse_dot_both(text: str) -> tuple[Optional[date], Optional[date]] | None:
    """
    Ak text obsahuje bodkovaný dátum A.B.YYYY, vracia (dmy, mdy):
      dmy = date(year, B, A)  – DD.MM.YYYY (európsky)
      mdy = date(year, A, B)  – MM.DD.YYYY (americký)
    Každá zložka môže byť None ak daná interpretácia dáva neplatný dátum.
    Vracia None ak text neobsahuje bodkový dátum vo formáte A.B.YYYY.
    """
    m = _DOT_DATE_RE.search(text.strip())
    if not m:
        return None

    a, b, year = int(m.group(1)), int(m.group(2)), int(m.group(3))

    if not (1990 <= year <= 2030):
        return None

    # DMY: day=a, month=b
    dmy: Optional[date] = None
    if 1 <= a <= 31 and 1 <= b <= 12:
        try:
            dmy = date(year, b, a)
        except ValueError:
            pass  # napr. 30. február

    # MDY: month=a, day=b
    mdy: Optional[date] = None
    if 1 <= a <= 12 and 1 <= b <= 31:
        try:
            mdy = date(year, a, b)
        except ValueError:
            pass

    return (dmy, mdy)


def resolve_mdr_format(
    candidates: list[tuple[str, Optional[date], Optional[date]]],
) -> tuple[Optional[str], Optional[str], dict]:
    """
    Určí formát bodkových dátumov (DMY vs MDY) a úroveň spoľahlivosti.

    Args:
        candidates: list[(category, dmy_date, mdy_date)]
                    pre každý bodkový dátum v zázname

    Returns:
        (format, confidence, librarian_flags)
          format:     "DMY" | "MDY" | None
          confidence: "high" | "medium" | "low" | "invalid" | None
          librarian_flags: dict s detailmi pre upozornenie knižníka
    """
    if not candidates:
        return None, None, {}

    # Klasifikácia každého kandidáta
    forced_dmy: list[tuple[str, date]] = []       # (cat, dmy)  – len DMY platné
    forced_mdy: list[tuple[str, date]] = []       # (cat, mdy)  – len MDY platné
    invalid:    list[str]              = []       # cat         – ani DMY ani MDY
    ambiguous:  list[tuple[str, date, date]] = [] # (cat, dmy, mdy) – obe platné

    for cat, dmy, mdy in candidates:
        if dmy is None and mdy is None:
            invalid.append(cat)
        elif dmy is not None and mdy is None:
            forced_dmy.append((cat, dmy))
        elif dmy is None and mdy is not None:
            forced_mdy.append((cat, mdy))
        else:
            ambiguous.append((cat, dmy, mdy))

    lib: dict = {}

    # Upozornenie na neplatné dátumy (napr. 7.30.2002 v DMY interpretácii)
    if invalid:
        lib["mdr_invalid_dates"] = {
            "categories": invalid,
            "note": "Tieto dátumy nemajú platnú interpretáciu v jednom alebo oboch formátoch",
        }

    # Konflikt: niektoré dátumy vynucujú DMY, iné MDY → chyba v dátach
    if forced_dmy and forced_mdy:
        lib["mdr_format_conflict"] = {
            "forced_dmy": [str(d) for _, d in forced_dmy],
            "forced_mdy": [str(d) for _, d in forced_mdy],
            "note": (
                "UPOZORNENIE PRE KNIŽNÍKA: Niektoré dátumy sú jednoznačne DD.MM.YYYY "
                "a iné jednoznačne MM.DD.YYYY – pravdepodobná chyba pri zadávaní"
            ),
        }
        return None, MDR_CONF_INVALID, lib

    # Vynútený formát jednou skupinou
    if forced_dmy:
        lib["mdr_format_resolved"] = {
            "format": MDR_FORMAT_DMY,
            "confidence": MDR_CONF_HIGH,
            "forced_by": [str(d) for _, d in forced_dmy],
            "note": "Formát určený jednoznačne – aspoň jeden dátum má deň > 12",
        }
        if ambiguous:
            lib["mdr_format_resolved"]["ambiguous_also_resolved"] = [
                str(dmy) for _, dmy, _ in ambiguous
            ]
        return MDR_FORMAT_DMY, MDR_CONF_HIGH, lib

    if forced_mdy:
        lib["mdr_format_resolved"] = {
            "format": MDR_FORMAT_MDY,
            "confidence": MDR_CONF_HIGH,
            "forced_by": [str(d) for _, d in forced_mdy],
            "note": "Formát určený jednoznačne – aspoň jedna hodnota nemôže byť deň v DMY interpretácii",
        }
        if ambiguous:
            lib["mdr_format_resolved"]["ambiguous_also_resolved"] = [
                str(mdy) for _, _, mdy in ambiguous
            ]
        return MDR_FORMAT_MDY, MDR_CONF_HIGH, lib

    # Všetky dátumy sú ambivalentné → chronologické obmedzenie
    if not ambiguous:
        # Len neplatné dátumy, žiadne platné
        return None, MDR_CONF_INVALID, lib

    if len(ambiguous) == 1:
        # Len jeden bodkový dátum, chronológiu nemožno overiť
        cat, dmy, mdy = ambiguous[0]
        lib["mdr_ambiguous"] = {
            "note": (
                "UPOZORNENIE PRE KNIŽNÍKA: Formát dátumu nie je možné určiť "
                "– len jeden bodkový dátum, obe interpretácie sú platné"
            ),
            "dmy_interpretation": str(dmy),
            "mdy_interpretation": str(mdy),
            "category": cat,
        }
        return None, MDR_CONF_LOW, lib

    # Zostavíme sekvenciu podľa poradia kategórií
    def _ordered_seq(use_dmy: bool) -> list[date]:
        seen: set[str] = set()
        entries: list[tuple[int, date]] = []
        for cat, dmy_d, mdy_d in ambiguous:
            if cat not in _CAT_ORDER or cat in seen:
                continue
            d = dmy_d if use_dmy else mdy_d
            if d:
                entries.append((_CAT_ORDER.index(cat), d))
                seen.add(cat)
        entries.sort(key=lambda x: x[0])
        return [d for _, d in entries]

    def _is_nondecreasing(seq: list[date]) -> bool:
        return all(seq[i] <= seq[i + 1] for i in range(len(seq) - 1))

    dmy_seq = _ordered_seq(use_dmy=True)
    mdy_seq = _ordered_seq(use_dmy=False)
    dmy_ok  = _is_nondecreasing(dmy_seq) if len(dmy_seq) >= 2 else True
    mdy_ok  = _is_nondecreasing(mdy_seq) if len(mdy_seq) >= 2 else True

    def _seq_dict(use_dmy: bool) -> dict:
        seen: set[str] = set()
        out: dict = {}
        for cat, dmy_d, mdy_d in ambiguous:
            if cat not in _CAT_ORDER or cat in seen:
                continue
            d = dmy_d if use_dmy else mdy_d
            if d:
                out[cat] = str(d)
                seen.add(cat)
        return out

    if dmy_ok and not mdy_ok:
        lib["mdr_format_resolved"] = {
            "format": MDR_FORMAT_DMY,
            "confidence": MDR_CONF_MEDIUM,
            "note": (
                "UPOZORNENIE PRE KNIŽNÍKA: Formát DD.MM.YYYY bol určený chronologickým "
                "obmedzením (Received ≤ Accepted ≤ Published). MDY interpretácia "
                "by porušila chronológiu."
            ),
            "dmy_sequence": _seq_dict(use_dmy=True),
            "mdy_would_violate": _seq_dict(use_dmy=False),
        }
        return MDR_FORMAT_DMY, MDR_CONF_MEDIUM, lib

    if mdy_ok and not dmy_ok:
        lib["mdr_format_resolved"] = {
            "format": MDR_FORMAT_MDY,
            "confidence": MDR_CONF_MEDIUM,
            "note": (
                "UPOZORNENIE PRE KNIŽNÍKA: Formát MM.DD.YYYY bol určený chronologickým "
                "obmedzením. DMY interpretácia by porušila chronológiu."
            ),
            "mdy_sequence": _seq_dict(use_dmy=False),
            "dmy_would_violate": _seq_dict(use_dmy=True),
        }
        return MDR_FORMAT_MDY, MDR_CONF_MEDIUM, lib

    if not dmy_ok and not mdy_ok:
        lib["mdr_chrono_error"] = {
            "note": (
                "UPOZORNENIE PRE KNIŽNÍKA: Ani DD.MM.YYYY ani MM.DD.YYYY interpretácia "
                "neuspokojuje chronologické obmedzenie. Pravdepodobná chyba v dátumoch."
            ),
            "dmy_sequence": _seq_dict(use_dmy=True),
            "mdy_sequence": _seq_dict(use_dmy=False),
        }
        return None, MDR_CONF_INVALID, lib

    # Obe interpretácie chronologicky konzistentné → skutočná nejednoznačnosť
    lib["mdr_ambiguous"] = {
        "note": (
            "UPOZORNENIE PRE KNIŽNÍKA: Formát dátumu nie je možné určiť automaticky. "
            "Obe interpretácie (DD.MM.YYYY aj MM.DD.YYYY) sú chronologicky konzistentné."
        ),
        "dmy_sequence": _seq_dict(use_dmy=True),
        "mdy_sequence": _seq_dict(use_dmy=False),
    }
    return None, MDR_CONF_LOW, lib


# ── Chronologická validácia ────────────────────────────────────────────────────
def _validate_chronology(parsed):
    warnings = []
    seq = [("received",parsed.received),("reviewed",parsed.reviewed),
           ("accepted",parsed.accepted),("published_online",parsed.published_online),
           ("published",parsed.published)]
    prev_l, prev_d = None, None
    for l, d in seq:
        if d is None: continue
        if prev_d and d < prev_d:
            delta = (prev_d - d).days
            sev = "ERROR" if delta >= 30 else "WARNING"
            warnings.append(f"{sev}: {l}={d} < {prev_l}={prev_d} ({delta} dní)")
        prev_l, prev_d = l, d
    return warnings


# ── Hlavná funkcia ────────────────────────────────────────────────────────────
def parse_fulltext_dates(resource_id, raw_text, dc_issued=None):
    result = ParsedDates(resource_id=resource_id, raw_text=raw_text)
    flags  = {}
    text   = _clean_input(raw_text)

    if not text:
        result.status = "empty"; result.flags = flags; return result

    # Špeciálny prípad: len rok
    if re.fullmatch(r'\{?\s*"?(\d{4})"?\s*\}?', text.strip()):
        yr_m = re.search(r'\d{4}', text)
        if yr_m and 1990 <= int(yr_m.group()) <= 2030:
            flags["year_only_dates"] = [text.strip()]
            result.needs_llm = True; result.status = "year_only"
            result.flags = flags; return result

    segments = _split_into_segments(text)
    if not segments:
        flags["no_labels_found"] = True; flags["raw_text_preview"] = text[:200]
        result.needs_llm = True; result.status = "no_labels"
        result.flags = flags; return result

    # ── Prechod 1: zbieranie bodkových kandidátov pre MDR resolver ──────────
    mdr_candidates: list[tuple[str, Optional[date], Optional[date]]] = []
    for label_raw, date_text in segments:
        category = match_label(label_raw)
        both = _try_parse_dot_both(date_text)
        if both is not None:
            dmy, mdy = both
            mdr_candidates.append((category, dmy, mdy))

    # ── MDR rozlíšenie formátu ───────────────────────────────────────────────
    mdr_format, mdr_confidence, mdr_flags = resolve_mdr_format(mdr_candidates)
    flags.update(mdr_flags)

    result.mdr_format     = mdr_format
    result.mdr_confidence = mdr_confidence

    # Pre LOW a INVALID → LLM fallback
    if mdr_confidence in (MDR_CONF_LOW, MDR_CONF_INVALID):
        result.needs_llm = True

    # Efektívny formát: DMY ako default (európsky kontext UTB), ak nie je určený inak
    use_dmy = (mdr_format != MDR_FORMAT_MDY)

    # ── Prechod 2: parsovanie so správnym formátom ───────────────────────────
    placeholders   = []; unparseable  = []; unknown_labels = []
    month_year_only = []; extra_entries = []

    for label_raw, date_text in segments:
        category = match_label(label_raw)

        # Placeholder?
        if re.search(r'00th|20xx|&&\s*&&|0000', date_text, re.IGNORECASE):
            placeholders.append(f"{label_raw!r}: {date_text[:60]!r}")
            result.all_entries.append(DateEntry(label_raw, date_text, category, None))
            continue

        # Bodkový dátum – použijeme MDR-resolved formát
        both = _try_parse_dot_both(date_text)
        if both is not None:
            dmy, mdy = both
            parsed_date = dmy if use_dmy else mdy

            # Ak preferred interpretácia dáva None, skúsime druhú (napr. 7.30.2002 v DMY režime)
            if parsed_date is None:
                parsed_date = mdy if use_dmy else dmy

            if parsed_date is None:
                if re.search(r'\d', date_text):
                    unparseable.append(f"{label_raw!r}: {date_text[:60]!r} [mdr_invalid]")
                result.all_entries.append(DateEntry(label_raw, date_text, category, None))
                continue

            entry = DateEntry(label_raw, date_text, category, parsed_date, True, False)
        else:
            # Nebodkový dátum – existujúci parser
            pr = _try_parse_date(date_text)
            if pr is None:
                if label_raw and re.search(r'\d', date_text):
                    unparseable.append(f"{label_raw!r}: {date_text[:60]!r}")
                result.all_entries.append(DateEntry(label_raw, date_text, category, None))
                continue

            parsed_date, day_exact, is_year_only = pr
            if not day_exact and not is_year_only:
                month_year_only.append(f"{label_raw!r}: {date_text!r} → {parsed_date}")
            entry = DateEntry(label_raw, date_text, category, parsed_date, day_exact, is_year_only)

        result.all_entries.append(entry)

        if category == DateCategory.UNKNOWN:
            unknown_labels.append(label_raw); extra_entries.append(entry); continue

        if   category == DateCategory.RECEIVED:
            if result.received is None: result.received = parsed_date
            else: flags.setdefault("multiple_received",[]).append({"label":label_raw,"date":parsed_date.isoformat()})
        elif category == DateCategory.REVIEWED:
            if result.reviewed is None: result.reviewed = parsed_date
            else: flags.setdefault("multiple_reviewed",[]).append({"label":label_raw,"date":parsed_date.isoformat()})
        elif category == DateCategory.ACCEPTED:
            if result.accepted is None: result.accepted = parsed_date
        elif category == DateCategory.PUBLISHED_ONLINE:
            if result.published_online is None: result.published_online = parsed_date
        elif category == DateCategory.PUBLISHED:
            if result.published is None: result.published = parsed_date
        elif category == DateCategory.EXTRA:
            extra_entries.append(entry)

    if extra_entries:
        flags["extra_dates"] = [{"label":e.raw_label,"date":e.to_iso(),"raw":e.raw_date}
                                 for e in extra_entries if e.parsed]

    if placeholders:    flags["placeholder_dates"]  = placeholders;  result.needs_llm = True
    if unparseable:     flags["unparseable_dates"]   = unparseable;   result.needs_llm = True
    if unknown_labels:  flags["unknown_labels"]      = list(dict.fromkeys(unknown_labels)); result.needs_llm = True
    if month_year_only: flags["month_year_only"]     = month_year_only

    cw = _validate_chronology(result)
    if cw:
        flags["chrono_warnings"] = cw
        if any(w.startswith("ERROR") for w in cw): result.needs_llm = True

    has_any = any([result.received, result.reviewed, result.accepted,
                   result.published_online, result.published])
    if result.needs_llm:  result.status = "needs_llm"
    elif has_any:         result.status = "processed"
    else:                 result.status = "no_dates"; result.needs_llm = True
    result.flags = flags
    return result


# ── Patch: match_label rozšírenie pre ordinal revisions ──────────────────────
_ORIG_MATCH_LABEL = match_label

def match_label(raw_label):
    """
    Rozšírená verzia match_label:
    - Pridáva podporu pre "1st Revision", "2nd Revision" atď.
    - Tieto sa mapujú na DateCategory.REVIEWED
    """
    if re.search(r'\d(?:st|nd|rd|th)\s+revision', raw_label, re.IGNORECASE):
        return DateCategory.REVIEWED
    return _ORIG_MATCH_LABEL(raw_label)
