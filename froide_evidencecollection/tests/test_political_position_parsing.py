from datetime import date

import pytest

from froide_evidencecollection.json_importer import (
    _parse_month,
    parse_level,
    parse_organization_name,
    parse_role,
)


class TestParseRole:
    @pytest.mark.parametrize(
        "label,expected",
        [
            ("Mitglied des Bundestags", "Mitglied"),
            ("Vorsitzender des Kreisverbands Konstanz", "Vorsitzende*r"),
            ("Vorsitzende des Landesverbands Berlin", "Vorsitzende*r"),
            (
                "Stellvertretender Vorsitzender der Landtagsfraktion",
                "Stellvertretende*r Vorsitzende*r",
            ),
            ("Bundesvorsitzender der Jungen Alternative", "Bundesvorsitzende*r"),
            ("Landesvorsitzender", "Landesvorsitzende*r"),
            ("Ehrenvorsitzender des Bundesverbands", "Ehrenvorsitzende*r"),
            ("Fraktionsvorsitzender", "Fraktionsvorsitzende*r"),
            ("Bundessprecher", "Bundessprecher*in"),
            (
                "Stellvertretende Bundessprecherin",
                "Stellvertretende*r Bundessprecher*in",
            ),
            (
                "Stellvertretender Sprecher des Bundesverbands",
                "Stellvertretende*r Sprecher*in",
            ),
            (
                "Innenpolitischer Sprecher der Bundestagsfraktion",
                "Innenpolitische*r Sprecher*in",
            ),
            (
                "Parlamentarischer Geschäftsführer",
                "Parlamentarische*r Geschäftsführer*in",
            ),
            ("Bürgermeister der Stadt Altenberg", "Bürgermeister*in"),
            ("Kandidatin für die Europawahl auf Listenplatz 9", "Kandidat*in"),
            ("Abgeordneter des Landtags Sachsen-Anhalt", "Abgeordnete*r"),
            ("Präsident des Bundesschiedsgerichts", "Präsident*in"),
            ("Stadtrat Erfurt", "Stadtrat*rätin"),
        ],
    )
    def test_canonical_role(self, label, expected):
        assert parse_role(label) == expected

    def test_more_specific_rule_wins(self):
        # "Vorstandsmitglied" must not be swallowed by the "Mitglied" rule.
        assert (
            parse_role("Vorstandsmitglied im Kreisverband Helmstedt")
            == "Vorstandsmitglied"
        )

    def test_no_match_returns_empty(self):
        assert parse_role("Schatzmeister des Ortsvereins") == ""

    def test_blank(self):
        assert parse_role("") == ""


class TestParseLevel:
    @pytest.mark.parametrize(
        "label,expected",
        [
            ("Mitglied des Europäischen Parlaments", "AfD-Europafraktion"),
            ("Kandidat für die Europawahl", "AfD-Europafraktion"),
            ("Mitglied des Bundestags", "AfD-Bundespartei"),
            ("Mitglied des Landtags Bayern", "AfD-Landesverbände"),
            ("Vorsitzender des Landesverbands Bayern", "AfD-Landesverbände"),
            ("Mitglied des Kreistags Görlitz", "AfD-Kreisverbände"),
            ("Bürgermeister der Stadt Altenberg", "AfD-Kreisverbände"),
            ("Mitglied des Gemeinderats Finsing", "AfD-Kreisverbände"),
        ],
    )
    def test_canonical_level(self, label, expected):
        assert parse_level(label) == expected

    def test_bund_beats_kreis_for_federal_seat_in_a_district(self):
        # "Landkreis" must not pull a Bundestag candidacy down to Kreis/Land.
        assert (
            parse_level("Kandidatin für Bundestagswahl im Landkreis Rosenheim")
            == "AfD-Bundespartei"
        )

    def test_kreis_beats_land_for_landkreis_place_qualifier(self):
        assert (
            parse_level("Vorsitzender des Kreisverbands Landkreis Leipzig")
            == "AfD-Kreisverbände"
        )

    def test_no_level_token_returns_empty(self):
        assert parse_level("Fraktionsvorsitzender") == ""


class TestParseOrganizationName:
    @pytest.mark.parametrize(
        "label,expected",
        [
            ("Vorsitzender des Kreisverbands Cottbus", "kreisverband cottbus"),
            ("Vorsitzender des Bezirksverbands Schwaben", "bezirksverband schwaben"),
            (
                "Stellvertretender Vorsitzender des Landesverbands Bayern",
                "landesverband bayern",
            ),
            # The board ("Landesvorstand") denotes the same org as the Verband.
            ("Mitglied im Landesvorstand Bayern", "landesverband bayern"),
            ("Mitglied des Bundesvorstands", "bundesverband"),
            # Genitive "-es" form.
            ("Stellvertretender Sprecher des Bundesverbandes", "bundesverband"),
            # A bare leading "Vorstands" is skipped; the prefixed body wins.
            (
                "Mitglied des Vorstands des Kreisverbands Dachau",
                "kreisverband dachau",
            ),
        ],
    )
    def test_candidate_name(self, label, expected):
        assert parse_organization_name(label) == expected

    @pytest.mark.parametrize(
        "label",
        [
            "Bundessprecher",
            "Landesvorsitzender",
            "Mitglied der Bundesprogrammkommission",
            "Präsident des Bundesschiedsgerichts",
            "",
        ],
    )
    def test_no_verband_returns_empty(self, label):
        assert parse_organization_name(label) == ""


class TestParseMonth:
    def test_start_is_first_of_month(self):
        assert _parse_month("2017-10") == date(2017, 10, 1)

    def test_end_is_last_of_month(self):
        assert _parse_month("2025-03", end=True) == date(2025, 3, 31)

    def test_end_handles_leap_year(self):
        assert _parse_month("2024-02", end=True) == date(2024, 2, 29)

    @pytest.mark.parametrize("value", ["", None, "2017", "2017-10-05", "garbage"])
    def test_unparseable_is_none(self, value):
        assert _parse_month(value) is None
