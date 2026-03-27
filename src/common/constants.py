"""Zdieľané konštanty pre pipeline spracovanie."""

from __future__ import annotations

from dataclasses import dataclass


class HeuristicStatus:
    NOT_PROCESSED = "not_processed"
    PROCESSED     = "processed"
    ERROR         = "error"


class LLMStatus:
    NOT_PROCESSED    = "not_processed"
    PROCESSED        = "processed"
    ERROR            = "error"
    VALIDATION_ERROR = "validation_error"


class ValidationStatus:
    NOT_CHECKED = "not_checked"
    OK          = "ok"
    HAS_ISSUES  = "has_issues"


class DateLLMStatus:
    NOT_PROCESSED    = "not_processed"
    PROCESSED        = "processed"
    ERROR            = "error"
    VALIDATION_ERROR = "validation_error"


class FlagKey:
    NO_WOS_DATA                  = "no_wos_data"
    PARSE_WARNINGS               = "wos_parse_warnings"
    MULTIPLE_UTB_BLOCKS          = "multiple_utb_blocks"
    UNMATCHED_UTB_AUTHORS        = "utb_authors_unmatched"
    MATCHED_UTB_AUTHORS          = "utb_authors_found_count"
    WOS_FACULTY_NOT_IN_REGISTRY  = "wos_faculty_not_in_registry"
    MULTIPLE_FACULTIES_AMBIGUOUS = "multiple_faculties_ambiguous"
    PATH_B_LOW_CONFIDENCE        = "path_b_low_confidence_matches"  # fuzzy zhody bez WoS na review
    ERROR                        = "error"


@dataclass(frozen=True)
class OutputColumn:
    name:        str
    sql_type:    str
    default_sql: str | None = None


OUTPUT_COLUMNS: tuple[OutputColumn, ...] = (
    OutputColumn("author_flags",                   "JSONB",   "'{}'::jsonb"),
    OutputColumn("author_heuristic_status",        "TEXT",    f"'{HeuristicStatus.NOT_PROCESSED}'"),
    OutputColumn("author_heuristic_version",       "TEXT"),
    OutputColumn("author_heuristic_processed_at",  "TIMESTAMPTZ"),
    OutputColumn("author_needs_llm",               "BOOLEAN", "FALSE"),
    OutputColumn("author_dc_names",                "TEXT[]"),
    OutputColumn("author_internal_names",          "TEXT[]"),
    OutputColumn("author_faculty",                 "TEXT[]"),
    OutputColumn("author_ou",                      "TEXT[]"),
    OutputColumn("author_llm_result",              "JSONB"),
    OutputColumn("author_llm_status",              "TEXT",    f"'{LLMStatus.NOT_PROCESSED}'"),
    OutputColumn("author_llm_processed_at",        "TIMESTAMPTZ"),
)

# -----------------------------------------------------------------------
# Fakulty UTB:  faculty_id → plný anglický názov
# -----------------------------------------------------------------------
FACULTIES: dict[str, str] = {
    "FLKR": "Faculty of Logistics and Crisis Management",
    "FT":   "Faculty of Technology",
    "FAME": "Faculty of Management and Economics",
    "FAI":  "Faculty of Applied Informatics",
    "FHS":  "Faculty of Humanities",
    "FMK":  "Faculty of Multimedia Communications",
    "UI":   "University Institute",   # Centre of Polymer Systems a iné univerzitné ústavy
}

# -----------------------------------------------------------------------
# Oddelenia / ústavy UTB:  plný názov → faculty_id
# -----------------------------------------------------------------------
DEPARTMENTS: dict[str, str] = {
    # --- University Institute (UI) ---
    "Centre of Polymer Systems":                                        "UI",
    "University Institute":                                             "UI",

    # --- FLKR ---
    "Department of Logistics":                                          "FLKR",
    "Department of Crisis Management":                                  "FLKR",
    "Department of Population Protection":                              "FLKR",
    "Department of Environmental Security":                             "FLKR",
    "Department of Health Care and Population Protection":              "FLKR",

    # --- FT ---
    "Department of Food Analysis and Chemistry":                        "FT",
    "Department of Physics and Materials Engineering":                  "FT",
    "Department of Chemistry":                                          "FT",
    "Department of Environmental Protection Engineering":               "FT",
    "Department of Polymer Engineering":                                "FT",
    "Department of Food Technology":                                    "FT",
    "Department of Fat, Surfactant and Cosmetics Technology":           "FT",
    "Department of Production Engineering":                             "FT",

    # --- FAME ---
    "Department of Economics":                                          "FAME",
    "Department of Management and Marketing":                           "FAME",
    "Department of Business Administration":                            "FAME",
    "Department of Industrial Engineering and Information Systems":     "FAME",
    "Department of Finance and Accounting":                             "FAME",
    "Department of Regional Development, Public Sector Administration and Law": "FAME",
    "Department of Statistics and Quantitative Methods":                "FAME",
    "Department of Physical Training":                                  "FAME",
    "Center for Applied Economic Research":                             "FAME",

    # --- FAI ---
    "Department of Informatics and Artificial Intelligence":            "FAI",
    "Department of Computer and Communication Systems":                 "FAI",
    "Department of Automation and Control Engineering":                 "FAI",
    "Department of Electronics and Measurements":                       "FAI",
    "Department of Security Engineering":                               "FAI",
    "Department of Mathematics":                                        "FAI",
    "Department of Process Control":                                    "FAI",
    "Centre for Security, Information and Advanced Technologies (CEBIA-Tech)": "FAI",
    "ICT Technology Park":                                              "FAI",

    # --- FHS ---
    "Department of Modern Languages and Literatures":                   "FHS",
    "Language Centre":                                                  "FHS",
    "Department of Pedagogical Sciences":                               "FHS",
    "Department of School Education":                                   "FHS",
    "Department of Health Care Sciences":                               "FHS",
    "Research Centre of FHS":                                           "FHS",
    "Education Support Centre":                                         "FHS",

    # --- FMK ---
    "Animation":                                                        "FMK",
    "Arts Management":                                                  "FMK",
    "Audiovisual Arts":                                                  "FMK",
    "Department of Marketing Communications":                           "FMK",
    "Department of Theoretical Studies":                                "FMK",
    "Digital Design":                                                   "FMK",
    "Fashion Design":                                                   "FMK",
    "Game Design":                                                      "FMK",
    "Glass":                                                            "FMK",
    "Graphic Design":                                                   "FMK",
    "Industrial Design":                                                "FMK",
    "Jewellery Design":                                                 "FMK",
    "Photography":                                                      "FMK",
    "Product Design":                                                   "FMK",
    "Shoe Design":                                                      "FMK",
    "Spatial Design":                                                   "FMK",
}

# -----------------------------------------------------------------------
# WoS SKRATKY → (plný_názov_oddelenia, faculty_id)
#
# WoS skracuje názvy inštitúcií – toto je lookup tabuľka pre prevod
# skrátených WoS názvov na plné anglické názvy.
# Zoradené od dlhších k kratším, aby dlhší match mal prednosť.
# -----------------------------------------------------------------------
WOS_ABBREV_MAP: dict[str, tuple[str, str]] = {
    # University Institute / Centre of Polymer Systems
    "ctr polymer syst":         ("Centre of Polymer Systems",                                      "UI"),
    "univ inst":                ("University Institute",                                            "UI"),
    "inst nanomat adv technol & innovat": ("Centre of Polymer Systems",                            "UI"),

    # FT – Faculty of Technology
    "fac technol":              ("Faculty of Technology",                                           "FT"),
    "dept polymer engn":        ("Department of Polymer Engineering",                               "FT"),
    "dept polymer":             ("Department of Polymer Engineering",                               "FT"),
    "dept phys & mat engn":     ("Department of Physics and Materials Engineering",                 "FT"),
    "dept phys mat engn":       ("Department of Physics and Materials Engineering",                 "FT"),
    "dept food anal & chem":    ("Department of Food Analysis and Chemistry",                       "FT"),
    "dept food anal chem":      ("Department of Food Analysis and Chemistry",                       "FT"),
    "dept food technol":        ("Department of Food Technology",                                   "FT"),
    "dept food sci":            ("Department of Food Technology",                                   "FT"),
    "dept fat surfactant & cosmet technol": ("Department of Fat, Surfactant and Cosmetics Technology", "FT"),
    "dept fat surfactant cosmet technol":   ("Department of Fat, Surfactant and Cosmetics Technology", "FT"),
    "dept environm protect engn": ("Department of Environmental Protection Engineering",            "FT"),
    "dept environ protect engn":  ("Department of Environmental Protection Engineering",            "FT"),
    "dept prod engn":           ("Department of Production Engineering",                            "FT"),
    "dept chem":                ("Department of Chemistry",                                         "FT"),
    "vavreckova":               ("Faculty of Technology",                                           "FT"),
    "nam t g masaryka":         ("Faculty of Technology",                                           "FT"),
    "nam tg masaryka":          ("Faculty of Technology",                                           "FT"),

    # FAME – Faculty of Management and Economics
    "fac management & econ":    ("Faculty of Management and Economics",                            "FAME"),
    "fac management econ":      ("Faculty of Management and Economics",                            "FAME"),
    "fac management":           ("Faculty of Management and Economics",                            "FAME"),
    "fac econ":                 ("Faculty of Management and Economics",                            "FAME"),
    "dept business adm":        ("Department of Business Administration",                          "FAME"),
    "dept business":            ("Department of Business Administration",                          "FAME"),
    "dept econ":                ("Department of Economics",                                        "FAME"),
    "dept management":          ("Department of Management and Marketing",                         "FAME"),
    "dept ind engn & inf syst": ("Department of Industrial Engineering and Information Systems",   "FAME"),
    "dept stat":                ("Department of Statistics and Quantitative Methods",              "FAME"),
    "ctr appl econ res":        ("Center for Applied Economic Research",                           "FAME"),
    "mostni":                   ("Faculty of Management and Economics",                            "FAME"),
    "mostni 5139":              ("Faculty of Management and Economics",                            "FAME"),

    # FAI – Faculty of Applied Informatics
    "fac appl informat":        ("Faculty of Applied Informatics",                                 "FAI"),
    "dept informat & artificial intelligence": ("Department of Informatics and Artificial Intelligence", "FAI"),
    "dept informat artif intelligen": ("Department of Informatics and Artificial Intelligence",    "FAI"),
    "dept automat & control engn": ("Department of Automation and Control Engineering",            "FAI"),
    "dept automat control engn":   ("Department of Automation and Control Engineering",            "FAI"),
    "dept comp & commun syst":  ("Department of Computer and Communication Systems",               "FAI"),
    "dept electron":            ("Department of Electronics and Measurements",                      "FAI"),
    "dept secur engn":          ("Department of Security Engineering",                             "FAI"),
    "dept math":                ("Department of Mathematics",                                       "FAI"),
    "dept proc control":        ("Department of Process Control",                                   "FAI"),
    "cebia":                    ("Centre for Security, Information and Advanced Technologies (CEBIA-Tech)", "FAI"),

    # FLKR – Faculty of Logistics and Crisis Management
    "fac logist":               ("Faculty of Logistics and Crisis Management",                     "FLKR"),
    "dept logist":              ("Department of Logistics",                                        "FLKR"),
    "dept crisis management":   ("Department of Crisis Management",                               "FLKR"),
    "dept hlth care & populat": ("Department of Health Care and Population Protection",           "FLKR"),
    "uherske hradiste":         ("Faculty of Logistics and Crisis Management",                     "FLKR"),

    # FHS – Faculty of Humanities
    "fac humanities":           ("Faculty of Humanities",                                          "FHS"),
    "dept pedag sci":           ("Department of Pedagogical Sciences",                             "FHS"),
    "dept mod languages":       ("Department of Modern Languages and Literatures",                 "FHS"),
    "dept modern languages":    ("Department of Modern Languages and Literatures",                 "FHS"),
    "language ctr":             ("Language Centre",                                                "FHS"),
    "res ctr fhs":              ("Research Centre of FHS",                                         "FHS"),
    "dept hlth care sci":       ("Department of Health Care Sciences",                             "FHS"),

    # FMK – Faculty of Multimedia Communications
    "fac multimedia":           ("Faculty of Multimedia Communications",                           "FMK"),
    "dept marketing commun":    ("Department of Marketing Communications",                         "FMK"),
    "dept theoret stud":        ("Department of Theoretical Studies",                              "FMK"),
}

# -----------------------------------------------------------------------
# Normalizačné pomôcky
# -----------------------------------------------------------------------
import re as _re
import unicodedata as _ud


def _norm(s: str) -> str:
    nfd = _ud.normalize("NFD", s)
    ascii_ = "".join(c for c in nfd if _ud.category(c) != "Mn")
    return _re.sub(r"\s+", " ", ascii_.lower()).strip()


# Normalizovaný WOS_ABBREV_MAP pre rýchle vyhľadávanie
WOS_ABBREV_NORM: dict[str, tuple[str, str]] = {
    _norm(k): v for k, v in WOS_ABBREV_MAP.items()
}

# Primárny slovník plných názvov: norm → (plný_názov, faculty_id)
DEPT_NORM_MAP: dict[str, tuple[str, str]] = {
    _norm(dept): (dept, fid)
    for dept, fid in DEPARTMENTS.items()
}

# Kombinovaný keyword map: WoS skratky + plné názvy oddelení
DEPT_KEYWORD_MAP: dict[str, tuple[str, str]] = {}

# Najprv pridaj WoS skratky (kratšie, ale špecifické)
for k, v in WOS_ABBREV_NORM.items():
    DEPT_KEYWORD_MAP[k] = v

# Potom plné názvy oddelení (normalizované) – majú prednosť pri rovnakej dĺžke
for dept, fid in DEPARTMENTS.items():
    key = _norm(dept)
    DEPT_KEYWORD_MAP[key] = (dept, fid)
    # Verzia bez prvého slova "department" / "centre" / "center"
    words = key.split()
    if len(words) > 2:
        short = " ".join(words[1:])
        if short not in DEPT_KEYWORD_MAP:
            DEPT_KEYWORD_MAP[short] = (dept, fid)

# -----------------------------------------------------------------------
# Fallback pravidlá pre fakultu (keď nepoznáme konkrétne oddelenie)
# Zoradené od najšpecifickejších k najvšeobecnejším.
# -----------------------------------------------------------------------
# -----------------------------------------------------------------------
# České názvy fakúlt (z remote DB obd_prac) → faculty_id
# Normalizovaná verzia pre robustné vyhľadávanie (bez diakritiky, lowercase).
# Používa sa pri overovaní WoS fakulty voči remote DB registru.
# -----------------------------------------------------------------------
CZECH_FACULTY_MAP: dict[str, str] = {
    "Fakulta technologická":                    "FT",
    "Fakulta managementu a ekonomiky":          "FAME",
    "Fakulta aplikované informatiky":           "FAI",
    "Fakulta logistiky a krizového řízení":     "FLKR",
    "Fakulta humanitních studií":               "FHS",
    "Fakulta multimediálních komunikací":       "FMK",
}

CZECH_FACULTY_MAP_NORM: dict[str, str] = {
    _norm(k): v for k, v in CZECH_FACULTY_MAP.items()
}

# Inverzný slovník: anglický názov fakulty → faculty_id
FACULTY_ENGLISH_TO_ID: dict[str, str] = {v: k for k, v in FACULTIES.items()}


FACULTY_KEYWORD_RULES: tuple[tuple[tuple[str, ...], str], ...] = (
    (
        ("ctr polymer syst", "univ inst", "polymer syst",
         "inst nanomat"),
        "UI",
    ),
    (
        ("fac technol", "dept polymer", "dept chem", "dept food",
         "dept phys", "polymer engn", "vavreckova", "nam t g masaryka",
         "nam tg masaryka", "dept environ"),
        "FT",
    ),
    (
        ("fac management", "fac econ", "dept business", "dept econ",
         "dept management", "dept financ", "mostni", "ctr appl econ"),
        "FAME",
    ),
    (
        ("fac appl informat", "appl informat", "dept informat",
         "dept automat", "dept electron", "dept secur engn",
         "dept math", "dept proc control", "cebia"),
        "FAI",
    ),
    (
        ("fac logist", "crisis management", "dept logist",
         "uherske hradiste", "dept hlth care & populat"),
        "FLKR",
    ),
    (
        ("fac humanities", "dept pedag", "dept hlth care sci",
         "dept lang", "language ctr", "res ctr fhs"),
        "FHS",
    ),
    (
        ("fac multimedia", "multimedia commun", "dept marketing commun",
         "dept theoret"),
        "FMK",
    ),
)