from src.authors.parsers.scopus import parse_scopus_affiliation


def test_parse_scopus_affiliation_extracts_author_name_from_new_format():
    parsed = parse_scopus_affiliation(
        "Belas J., Tomas Bata University in Zlin, Faculty of Management and Economics, Zlin, Czech Republic;"
        " Dvorsky J., University of Zilina, Zilina, Slovakia"
    )

    assert len(parsed.blocks) == 2
    assert parsed.blocks[0].author_name == "Belas J."
    assert parsed.blocks[0].affiliation.startswith("Tomas Bata University in Zlin")
    assert parsed.utb_blocks[0].author_name == "Belas J."
    assert parsed.has_authors is True


def test_parse_scopus_affiliation_keeps_old_format_backward_compatible():
    parsed = parse_scopus_affiliation(
        "Department of Economics, Tomas Bata University in Zlin, Zlin, Czech Republic"
    )

    assert len(parsed.blocks) == 1
    assert parsed.blocks[0].author_name is None
    assert parsed.blocks[0].affiliation == (
        "Department of Economics, Tomas Bata University in Zlin, Zlin, Czech Republic"
    )
    assert parsed.has_authors is False


def test_parse_scopus_affiliation_splits_multiple_affiliations_per_author():
    """Skutočný príklad z DB: Di Martino má dve afiliácie zlúčené čiarkou."""
    text = (
        "Di Martino A., Research School of Chemistry & Applied Biomedical Sciences, "
        "Tomsk Polytechnic University, Lenin Av. 30, Tomsk, 634050, Russian Federation, "
        "Centre of Polymer Systems, University Institute, Tomas Bata University in Zlin, "
        "Tr. T. Bati 5678, Zlin, 760 01, Czech Republic"
    )
    parsed = parse_scopus_affiliation(text)

    # Jeden záznam, dve afiliácie → dva bloky s rovnakým menom autora
    assert len(parsed.blocks) == 2
    assert all(b.author_name == "Di Martino A." for b in parsed.blocks)

    # Prvá afiliácia končí Russian Federation, druhá Czech Republic
    assert "Tomsk Polytechnic University" in parsed.blocks[0].affiliation
    assert "Tomas Bata University in Zlin" in parsed.blocks[1].affiliation

    # UTB sa správne deteguje len v druhom bloku
    assert parsed.blocks[0].is_utb is False
    assert parsed.blocks[1].is_utb is True
    assert len(parsed.utb_blocks) == 1
    assert parsed.utb_blocks[0].department == "Centre of Polymer Systems"


def test_parse_scopus_affiliation_real_db_example():
    """Plný 7-autorský záznam z DB – overenie ktorí autori sú UTB."""
    text = (
        "Di Martino A., Research School of Chemistry & Applied Biomedical Sciences, "
        "Tomsk Polytechnic University, Lenin Av. 30, Tomsk, 634050, Russian Federation, "
        "Centre of Polymer Systems, University Institute, Tomas Bata University in Zlin, "
        "Tr. T. Bati 5678, Zlin, 760 01, Czech Republic; "
        "Drannikov A., Research School of Chemistry & Applied Biomedical Sciences, "
        "Tomsk Polytechnic University, Lenin Av. 30, Tomsk, 634050, Russian Federation; "
        "Surgutskaia N.S., Centre of Polymer Systems, University Institute, "
        "Tomas Bata University in Zlin, Tr. T. Bati 5678, Zlin, 760 01, Czech Republic; "
        "Ozaltin K., Centre of Polymer Systems, University Institute, "
        "Tomas Bata University in Zlin, Tr. T. Bati 5678, Zlin, 760 01, Czech Republic; "
        "Postnikov P.S., Research School of Chemistry & Applied Biomedical Sciences, "
        "Tomsk Polytechnic University, Lenin Av. 30, Tomsk, 634050, Russian Federation; "
        "Marina T.E., Research School of Chemistry & Applied Biomedical Sciences, "
        "Tomsk Polytechnic University, Lenin Av. 30, Tomsk, 634050, Russian Federation; "
        "Sedlarik V., Centre of Polymer Systems, University Institute, "
        "Tomas Bata University in Zlin, Tr. T. Bati 5678, Zlin, 760 01, Czech Republic"
    )
    parsed = parse_scopus_affiliation(text)

    assert parsed.utb_authors == ["Di Martino A.", "Surgutskaia N.S.", "Ozaltin K.", "Sedlarik V."]
    assert parsed.all_authors == [
        "Di Martino A.", "Drannikov A.", "Surgutskaia N.S.", "Ozaltin K.",
        "Postnikov P.S.", "Marina T.E.", "Sedlarik V.",
    ]


def test_parse_scopus_affiliation_handles_marina_with_two_initials():
    """Meno typu 'Marina T.E.' – posledný token sú zlúčené iniciály."""
    parsed = parse_scopus_affiliation(
        "Marina T.E., Tomas Bata University in Zlin, Czech Republic"
    )
    assert parsed.blocks[0].author_name == "Marina T.E."
    assert parsed.blocks[0].is_utb is True


def test_parse_scopus_affiliation_handles_two_word_surname():
    """'Di Martino A.' – dvojslovné priezvisko + iniciála."""
    parsed = parse_scopus_affiliation(
        "Di Martino A., Tomas Bata University in Zlin, Czech Republic"
    )
    assert parsed.blocks[0].author_name == "Di Martino A."
    assert parsed.blocks[0].is_utb is True


def test_parse_scopus_affiliation_does_not_misdetect_institution_as_name():
    """Riadok začínajúci 'University of...' nie je menom autora."""
    parsed = parse_scopus_affiliation(
        "University of South Bohemia in Ceske Budejovice, "
        "Branisovska 31a, Ceske Budejovice, 370 05, Czech Republic"
    )
    assert parsed.blocks[0].author_name is None
