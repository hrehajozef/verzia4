"""
Mapovanie textových labelov na kategórie dátumov a vzory formátov dátumov.

Každá publikácia môže mať dátumy v rôznych jazykoch a rôznych formátoch.
Tento modul definuje:
  1. LABEL_MAP – slovník "normalizovaný_label → kategória"
  2. DATE_PATTERNS – zoradený zoznam regex vzorov pre rôzne formáty dátumov
  3. DateCategory – enumerácia kategórií dátumov

Kategórie (mapujú na výstupné stĺpce v DB):
  received         → utb_date_received
  reviewed         → utb_date_reviewed
  accepted         → utb_date_accepted
  published_online → utb_date_published_online
  published        → utb_date_published
  extra            → utb_date_extra (ostatné identifikované dátumy)
"""

from __future__ import annotations

# -----------------------------------------------------------------------
# Kategórie dátumov
# -----------------------------------------------------------------------

class DateCategory:
    RECEIVED         = "received"
    REVIEWED         = "reviewed"
    ACCEPTED         = "accepted"
    PUBLISHED_ONLINE = "published_online"
    PUBLISHED        = "published"
    EXTRA            = "extra"       # identifikovaný dátum bez jasnej kategórie
    UNKNOWN          = "unknown"     # label nájdený, kategória neurčená → LLM


# -----------------------------------------------------------------------
# Mapovanie labelov na kategórie
#
# Kľúč:   normalizovaný label (lowercase, bez diakritiky, ořezaný)
# Hodnota: DateCategory.* konštanta
#
# Vzor: keď sa normalizovaný vstupný label začína alebo presne zodpovedá
# niektorému kľúču, použije sa daná kategória.
# Zoradenie nie je dôležité – používa sa funkcia match_label() v parser.py.
# -----------------------------------------------------------------------

LABEL_MAP: dict[str, str] = {

    # === RECEIVED (prvé doručenie do redakcie) ===
    "received":                                    DateCategory.RECEIVED,
    "manuscript received":                         DateCategory.RECEIVED,
    "paper received":                              DateCategory.RECEIVED,
    "article submitted":                           DateCategory.RECEIVED,
    "submitted":                                   DateCategory.RECEIVED,
    "submission":                                  DateCategory.RECEIVED,
    "date submitted":                              DateCategory.RECEIVED,
    # České/slovenské varianty
    "doslo":                                       DateCategory.RECEIVED,   # Došlo:
    "do redakce doslo":                            DateCategory.RECEIVED,   # Do redakce došlo
    "do redakce doslo dne":                        DateCategory.RECEIVED,   # Do redakce došlo dne
    "clanek prijat redakci":                       DateCategory.RECEIVED,   # Článek přijat redakcí
    # Chorvátske
    "primljeno":                                   DateCategory.RECEIVED,   # Chorv. "prijaté"
    # Španielske
    "fecha de recepcion":                          DateCategory.RECEIVED,   # Fecha de recepción

    # === REVIEWED (po recenzii, pred prijatím) ===
    # "Received in revised form" znamená, že autor prepracoval článok → reviewed
    "received in revised form":                    DateCategory.REVIEWED,
    "received in revised form:":                   DateCategory.REVIEWED,
    "revised manuscript received":                 DateCategory.REVIEWED,
    "editorial decision":                          DateCategory.REVIEWED,
    # České varianty
    "prijato k recenzi":                           DateCategory.REVIEWED,   # Přijato k recenzi
    "clanek prijat k recenzi":                     DateCategory.REVIEWED,

    # === ACCEPTED (finálne prijatie) ===
    "accepted":                                    DateCategory.ACCEPTED,
    "accepted for publication":                    DateCategory.ACCEPTED,
    "accepted for publication on":                 DateCategory.ACCEPTED,
    "accepted manuscript online":                  DateCategory.ACCEPTED,
    "accepted author version posted online":       DateCategory.ACCEPTED,
    "accepted in revised form":                    DateCategory.ACCEPTED,
    "accepted on":                                 DateCategory.ACCEPTED,
    "final acceptance":                            DateCategory.ACCEPTED,
    "approved for publication":                    DateCategory.ACCEPTED,
    # České varianty
    "prijato do tisku":                            DateCategory.ACCEPTED,   # Přijato do tisku
    "prijato":                                     DateCategory.ACCEPTED,   # přijato
    "clanek prijat k publikaci":                   DateCategory.ACCEPTED,   # Článek přijat k publikaci
    # Španielske
    "fecha de aceptacion":                         DateCategory.ACCEPTED,   # Fecha de aceptación
    # Chorvátske
    "odobreno":                                    DateCategory.ACCEPTED,   # "schválené"

    # === PUBLISHED ONLINE (online pred tlačou) ===
    "published online":                            DateCategory.PUBLISHED_ONLINE,
    "available online":                            DateCategory.PUBLISHED_ONLINE,
    "published online:":                           DateCategory.PUBLISHED_ONLINE,
    "online":                                      DateCategory.PUBLISHED_ONLINE,
    # "Version of record" = finálna verzia online = published online
    "version of record online":                    DateCategory.PUBLISHED_ONLINE,
    "previously published online":                 DateCategory.PUBLISHED_ONLINE,
    "e-published":                                 DateCategory.PUBLISHED_ONLINE,
    "epublished":                                  DateCategory.PUBLISHED_ONLINE,
    # České
    "zverejneno":                                  DateCategory.PUBLISHED_ONLINE,  # zveřejněno

    # === PUBLISHED (tlačené vydanie / finálna publikácia) ===
    "published":                                   DateCategory.PUBLISHED,
    "date of publication":                         DateCategory.PUBLISHED,
    "article publication date":                    DateCategory.PUBLISHED,
    "publication date":                            DateCategory.PUBLISHED,
    "first published":                             DateCategory.PUBLISHED,
    "publication in this collection":              DateCategory.PUBLISHED,

    # === REVIEWED varianty (revízie patria do utb_date_reviewed) ===
    "revised":                                     DateCategory.REVIEWED,
    "resubmitted":                                 DateCategory.REVIEWED,
    "1st revision":                                DateCategory.REVIEWED,
    "2nd revision":                                DateCategory.REVIEWED,
    "3rd revision":                                DateCategory.REVIEWED,
    "prepracovano":                                DateCategory.REVIEWED,
    # === EXTRA (identifikovaný dátum, ale kategória menej jasná) ===
    # Dátum aktuálnej verzie (technický IEEE metadátum)
    "date of current version":                     DateCategory.EXTRA,
    # Dátum vydania čísla časopisu (nie článku)
    "issue publication date":                      DateCategory.EXTRA,
    "date of issue":                               DateCategory.EXTRA,
}


# -----------------------------------------------------------------------
# Normalizačná funkcia pre labely
#
# Používa sa pri matchovaní vstupných labelov s LABEL_MAP.
# Odstraňuje diakritiku, normalizuje medzery, prevádza na lowercase.
# -----------------------------------------------------------------------

import re as _re
import unicodedata as _ud


def normalize_label(label: str) -> str:
    """
    Normalizuje label pre porovnanie s LABEL_MAP.
    Postup: lowercase → bez diakritiky → komprimované medzery → bez bodky/dvojbodky na konci.
    """
    nfd    = _ud.normalize("NFD", label.lower())
    no_acc = "".join(c for c in nfd if _ud.category(c) != "Mn")
    clean  = _re.sub(r"[:\.\s]+$", "", no_acc)  # ořez trailing : . mezery
    return _re.sub(r"\s+", " ", clean).strip()


def match_label(raw_label: str) -> str:
    """
    Nájde kategóriu pre daný raw label.

    Poradie porovnávania (od najšpecifickejšieho):
    1. Presná zhoda normalizovaného labelu
    2. Začína normalizovaný label niektorým kľúčom? (prefix match)
    3. Obsahuje normalizovaný label niektorý kľúč? (substring match)

    Vracia DateCategory.UNKNOWN ak žiadna zhoda.
    """
    norm = normalize_label(raw_label)
    if not norm:
        return DateCategory.UNKNOWN

    # 1. Presná zhoda
    if norm in LABEL_MAP:
        return LABEL_MAP[norm]

    # 2. Začiatok (napr. "received august" začína na "received")
    #    Zoradíme kľúče od dlhších – dlhší kľúč má prednosť
    for key in sorted(LABEL_MAP, key=len, reverse=True):
        if norm.startswith(key):
            return LABEL_MAP[key]

    # 3. Substring (napr. "manuscript received october" obsahuje "received")
    for key in sorted(LABEL_MAP, key=len, reverse=True):
        if key in norm:
            return LABEL_MAP[key]

    return DateCategory.UNKNOWN


# -----------------------------------------------------------------------
# Vzory formátov dátumov
#
# Každý vzor je tuple (regex, funkcia_na_parsovanie).
# Vzory sú zoradené od najšpecifickejších k najvšeobecnejším.
# Všetky regex majú pomenované skupiny pre jasnosť.
# -----------------------------------------------------------------------

# Mesiace – anglické plné názvy a skratky
_MONTHS_EN = {
    "january": 1,  "jan": 1,
    "february": 2, "feb": 2,
    "march": 3,    "mar": 3,
    "april": 4,    "apr": 4,
    "may": 5,
    "june": 6,     "jun": 6,
    "july": 7,     "jul": 7,
    "august": 8,   "aug": 8,
    "september": 9,"sep": 9,  "sept": 9,
    "october": 10, "oct": 10,
    "november": 11,"nov": 11,
    "december": 12,"dec": 12,
    # Španielske mesiace (vyskytujú sa v dátach)
    "enero": 1, "febrero": 2, "marzo": 3, "abril": 4, "mayo": 5,
    "junio": 6, "julio": 7, "agosto": 8, "septiembre": 9, "octubre": 10,
    "noviembre": 11, "diciembre": 12,
    # Rímske číslice (Chorvátsko: "02. VII. 2019")
    "i": 1, "ii": 2, "iii": 3, "iv": 4, "v": 5, "vi": 6,
    "vii": 7, "viii": 8, "ix": 9, "x": 10, "xi": 11, "xii": 12,
}

MONTHS_EN = _MONTHS_EN  # exportuj pre použitie v parser.py


# Regex pre jednotlivé časti dátumu
_DAY   = r"(?P<day>\d{1,2})(?:st|nd|rd|th)?"          # 1, 01, 1st, 21st
_DAY_OF= r"(?P<day>\d{1,2})(?:st|nd|rd|th)?\s+of"     # "5th of May"
_MONTH_NAME = r"(?P<month>[A-Za-záčýž]+\.?)"           # January, Jan, Ene., VII
_YEAR  = r"(?P<year>\d{4})"                             # 2018
_SEP   = r"[\s,\.]+"                                    # oddeľovač

# Zoradené vzory – ORDER MATTERS (dlhšie/špecifickejšie musia byť skôr)
DATE_REGEX_PATTERNS: list[tuple[str, str]] = [
    # --- ISO formáty ---
    # YYYY-MM-DD  →  2018-06-30
    ("iso_ymd",          r"(?P<year>\d{4})-(?P<month>\d{1,2})-(?P<day>\d{1,2})"),
    # YYYY.MM.DD  →  2018.07.02
    ("dot_ymd",          r"(?P<year>\d{4})\.(?P<month>\d{1,2})\.(?P<day>\d{1,2})"),

    # --- DD.MM.YYYY formáty (európske) ---
    # DD. MM. YYYY  →  02. 09. 2017  (s medzerami)
    ("dot_dmy_spaced",   r"(?P<day>\d{1,2})\.\s*(?P<month>\d{1,2})\.\s*(?P<year>\d{4})"),
    # DD.MM.YYYY  →  15.02.2018  (bez medzier)
    ("dot_dmy",          r"(?P<day>\d{1,2})\.(?P<month>\d{1,2})\.(?P<year>\d{4})"),

    # --- Slovné mesiace ---
    # "5th of May, 2020"  (with "of")
    ("day_of_month_year","(?P<day>\\d{1,2})(?:st|nd|rd|th)?\\s+of\\s+(?P<month>[A-Za-záčýž]+\\.?)\\s*,?\\s*(?P<year>\\d{4})"),
    # "07 November 2017"  alebo  "15th October 2018"
    ("day_month_year",   "(?P<day>\\d{1,2})(?:st|nd|rd|th)?\\s+(?P<month>[A-Za-záčýž]+\\.?)\\s*,?\\s*(?P<year>\\d{4})"),
    # "November 07, 2017"  alebo  "May 18, 2018"  alebo  "September, 18th, 2017"
    ("month_day_year",   "(?P<month>[A-Za-záčýž]+\\.?)\\s*,?\\s*(?P<day>\\d{1,2})(?:st|nd|rd|th)?\\s*,\\s*(?P<year>\\d{4})"),
    # "February 2020"  (len mesiac + rok, bez dňa)
    ("month_year_only",  "(?P<month>[A-Za-záčýž]+\\.?)\\s*,?\\s*(?P<year>\\d{4})"),

    # --- Len rok ---
    # {2009}  alebo  samostatný 4-ciferný rok
    ("year_only",        r"\{?(?P<year>20\d{2}|19\d{2})\}?"),
]
