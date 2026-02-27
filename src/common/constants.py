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


class FlagKey:
    NO_WOS_DATA          = "no_wos_data"
    PARSE_WARNINGS       = "wos_parse_warnings"
    MULTIPLE_UTB_BLOCKS  = "multiple_utb_blocks"
    UNMATCHED_UTB_AUTHORS = "utb_authors_unmatched"
    MATCHED_UTB_AUTHORS  = "utb_authors_found_count"
    ERROR                = "error"


@dataclass(frozen=True)
class OutputColumn:
    name:        str
    sql_type:    str
    default_sql: str | None = None


# Výstupné stĺpce pridané do lokálnej tabuľky.
# TEXT[] = PostgreSQL array – psycopg3 preloží Python list automaticky.
OUTPUT_COLUMNS: tuple[OutputColumn, ...] = (
    OutputColumn("flags",                          "JSONB",   "'{}'::jsonb"),
    OutputColumn("heuristic_status",               "TEXT",    f"'{HeuristicStatus.NOT_PROCESSED}'"),
    OutputColumn("heuristic_version",              "TEXT"),
    OutputColumn("heuristic_processed_at",         "TIMESTAMPTZ"),
    OutputColumn("needs_llm",                      "BOOLEAN", "FALSE"),
    # Všetci autori publikácie – kópia z dc.contributor.author
    OutputColumn("dc_contributor_author",          "TEXT[]"),
    # Interní UTB autori ako PostgreSQL pole
    OutputColumn("utb_contributor_internalauthor", "TEXT[]"),
    # Fakulty a oddelenia interných autorov – PostgreSQL polia
    OutputColumn("utb_faculty",                    "TEXT[]"),
    OutputColumn("utb_ou",                         "TEXT[]"),
    OutputColumn("llm_result",                     "JSONB"),
    OutputColumn("llm_status",                     "TEXT",    f"'{LLMStatus.NOT_PROCESSED}'"),
    OutputColumn("llm_processed_at",               "TIMESTAMPTZ"),
)

# -----------------------------------------------------------------------
# Fakulty UTB:  faculty_id  →  plný anglický názov
# -----------------------------------------------------------------------
FACULTIES: dict[str, str] = {
    "FLKR": "Faculty of Logistics and Crisis Management",
    "FT":   "Faculty of Technology",
    "FAME": "Faculty of Management and Economics",
    "FAI":  "Faculty of Applied Informatics",
    "FHS":  "Faculty of Humanities",
    "FMK":  "Faculty of Multimedia Communications",
}

# -----------------------------------------------------------------------
# Oddelenia / ústavy UTB:  plný názov  →  faculty_id
# -----------------------------------------------------------------------
DEPARTMENTS: dict[str, str] = {
    # FLKR
    "Department of Logistics":                  "FLKR",
    "Department of Crisis Management":          "FLKR",
    "Department of Population Protection":      "FLKR",
    "Department of Environmental Security":     "FLKR",
    # FT
    "Department of Food Analysis and Chemistry":           "FT",
    "Department of Physics and Materials Engineering":     "FT",
    "Department of Chemistry":                             "FT",
    "Department of Environmental Protection Engineering":  "FT",
    "Department of Polymer Engineering":                   "FT",
    "Department of Food Technology":                       "FT",
    "Department of Fat, Surfactant and Cosmetics Technology": "FT",
    "Department of Production Engineering":                "FT",
    # FAME
    "Department of Economics":                                              "FAME",
    "Department of Management and Marketing":                               "FAME",
    "Department of Business Administration":                                "FAME",
    "Department of Industrial Engineering and Information Systems":         "FAME",
    "Department of Finance and Accounting":                                 "FAME",
    "Department of Regional Development, Public Sector Administration and Law": "FAME",
    "Department of Statistics and Quantitative Methods":                    "FAME",
    "Department of Physical Training":                                      "FAME",
    "Center for Applied Economic Research":                                 "FAME",
    # FAI
    "Department of Informatics and Artificial Intelligence":                "FAI",
    "Department of Computer and Communication Systems":                     "FAI",
    "Department of Automation and Control Engineering":                     "FAI",
    "Department of Electronics and Measurements":                           "FAI",
    "Department of Security Engineering":                                   "FAI",
    "Department of Mathematics":                                            "FAI",
    "Department of Process Control":                                        "FAI",
    "Centre for Security, Information and Advanced Technologies (CEBIA – Tech)": "FAI",
    "ICT Technology Park":                                                  "FAI",
    # FHS
    "Department of Modern Languages and Literatures":   "FHS",
    "Language Centre":                                  "FHS",
    "Department of Pedagogical Sciences":               "FHS",
    "Department of School Education":                   "FHS",
    "Department of Health Care Sciences":               "FHS",
    "Research Centre of FHS":                           "FHS",
    "Education Support Centre":                         "FHS",
    # FMK
    "Animation":                            "FMK",
    "Arts Management":                      "FMK",
    "Audiovisual Arts":                     "FMK",
    "Department of Marketing Communications": "FMK",
    "Department of Theoretical Studies":    "FMK",
    "Digital Design":                       "FMK",
    "Fashion Design":                       "FMK",
    "Game Design":                          "FMK",
    "Glass":                                "FMK",
    "Graphic Design":                       "FMK",
    "Industrial Design":                    "FMK",
    "Jewellery Design":                     "FMK",
    "Photography":                          "FMK",
    "Product Design":                       "FMK",
    "Shoe Design":                          "FMK",
    "Spatial Design":                       "FMK",
}

# -----------------------------------------------------------------------
# Pomocné lookup: skrátený normalizovaný kľúč  →  (plný_názov, faculty_id)
# Generuje sa automaticky z DEPARTMENTS pri importe modulu.
# -----------------------------------------------------------------------
import re as _re
import unicodedata as _ud


def _norm(s: str) -> str:
    nfd = _ud.normalize("NFD", s)
    ascii_ = "".join(c for c in nfd if _ud.category(c) != "Mn")
    return _re.sub(r"\s+", " ", ascii_.lower()).strip()


# Primárny slovník: normalizovaný plný názov → (plný_názov, faculty_id)
DEPT_NORM_MAP: dict[str, tuple[str, str]] = {
    _norm(dept): (dept, fid)
    for dept, fid in DEPARTMENTS.items()
}

# Kľúčové slová skrátené pre heuristické vyhľadávanie v texte afiliácie
# Každý záznam: normalizované_kľúčové_slovo → (plný_názov, faculty_id)
DEPT_KEYWORD_MAP: dict[str, tuple[str, str]] = {}
for _dept, _fid in DEPARTMENTS.items():
    _key = _norm(_dept)
    DEPT_KEYWORD_MAP[_key] = (_dept, _fid)
    # Pridaj aj verziu bez prvého slova (napr. "Dept " / "Department ")
    _words = _key.split()
    if len(_words) > 2:
        _short = " ".join(_words[1:])       # bez "department" / "dept"
        if _short not in DEPT_KEYWORD_MAP:
            DEPT_KEYWORD_MAP[_short] = (_dept, _fid)

# Kľúčové slová fakúlt pre heuristický fallback (keď oddelenie nenájdeme)
# Formát:  zoznam kľúčových slov  →  faculty_id
FACULTY_KEYWORD_RULES: tuple[tuple[tuple[str, ...], str], ...] = (
    (
        ("fac technol", "dept polymer", "dept chem", "dept food",
         "dept phys", "polymer engn", "vavreckova", "nam t g masaryka"),
        "FT",
    ),
    (
        ("fac management", "fac econ", "dept business", "dept econ",
         "dept management", "dept financ", "mostni", "mostni 5139"),
        "FAME",
    ),
    (
        ("fac appl informat", "appl informat", "dept informat",
         "dept automat", "dept electron", "dept secur engn",
         "dept math", "dept proc control", "cebia"),
        "FAI",
    ),
    (
        ("fac logist", "crisis management", "logist",
         "uherske hradiste", "dept logist"),
        "FLKR",
    ),
    (
        ("fac humanities", "dept pedag", "dept hlth", "dept lang",
         "language centre", "humanities"),
        "FHS",
    ),
    (
        ("fac multimedia", "multimedia commun", "dept marketing commun",
         "dept theoret"),
        "FMK",
    ),
)