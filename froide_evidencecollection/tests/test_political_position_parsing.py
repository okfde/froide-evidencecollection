import pytest

from froide_evidencecollection.json_importer import (
    parse_level,
    parse_role,
    segment_positions,
)


class TestParseRole:
    @pytest.mark.parametrize(
        "label,expected",
        [
            ("einfaches Mitglied", "Mitglied"),
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
            # Dump acronyms for parliamentary mandates.
            ("MdB aus Bayern", "Abgeordnete*r"),
            ("MdL in Thüringen", "Abgeordnete*r"),
            ("MdEP", "Abgeordnete*r"),
            ("MdA Berlin", "Abgeordnete*r"),
            ("ehem. MdB aus Hessen", "Abgeordnete*r"),
            # Council seats keep their own role rather than "Abgeordnete*r".
            ("Kreisrat im Ostalbkreis in Baden-Württemberg", "Kreisrat*rätin"),
            ("Kreistagsabgeordneter in Vorpommern-Rügen", "Kreisrat*rätin"),
            ("Gemeinderat in Stuttgart", "Gemeinderat*rätin"),
            ("Bezirksrat Oberbayern", "Bezirksrat*rätin"),
            ("Stadtbezirksbeirat Plauen in Sachsen", "Beirat*rätin"),
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

    def test_only_plain_membership_maps_to_mitglied(self):
        assert parse_role("einfaches Mitglied") == "Mitglied"
        assert parse_role("Mitglied im Kreistag") == "Kreisrat*rätin"

    def test_board_membership_maps_to_vorstandsmitglied(self):
        assert parse_role("Mitglied des Fraktionsvorstands") == "Vorstandsmitglied"
        assert parse_role("Mitglied im Landesvorstand") == "Vorstandsmitglied"
        assert parse_role("Ex-Mitglied im Bundesvorstand") == "Vorstandsmitglied"

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
            # Dump acronyms pin the level.
            ("MdEP", "AfD-Europafraktion"),
            ("MdB aus Sachsen", "AfD-Bundespartei"),
            ("MdL in Bayern", "AfD-Landesverbände"),
            ("MdA Berlin", "AfD-Landesverbände"),
            ("Vorsitzende KV Rotenburg (Wümme) in Niedersachsen", "AfD-Kreisverbände"),
            ("Vorsitzender OV Haar in Bayern", "AfD-Kreisverbände"),
            ("Vorsitzender BV Berlin-Lichtenberg", "AfD-Kreisverbände"),
            ("Bürgermeister von Altenberg in Sachsen", "AfD-Kreisverbände"),
            # Abgeordnetenhaus candidacy without the MdA acronym.
            (
                "Kanditat zur Wahl des Abgeordnetenhauses in Berlin",
                "AfD-Landesverbände",
            ),
        ],
    )
    def test_canonical_level(self, label, expected):
        assert parse_level(label) == expected

    def test_mandate_acronym_beats_secondary_office(self):
        # An MdB who also chairs a Kreisverband stays at the federal level.
        assert (
            parse_level("MdB aus Bayern und Vorsitzender des KV Ebersberg")
            == "AfD-Bundespartei"
        )

    def test_local_office_beats_abgeordnetenhaus_candidacy(self):
        # A Bezirksverband chair who is merely a candidate for the Abgeordnetenhaus
        # keeps the local level rather than being pulled up to Land.
        assert (
            parse_level(
                "Vorsitzender BV Berlin-Lichtenberg, Kandidat bei der Wahl "
                "zum Berliner Abgeordnetenhaus 2026"
            )
            == "AfD-Kreisverbände"
        )

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


class TestSegmentPositions:
    @pytest.mark.parametrize(
        "label,expected",
        [
            (
                "MdB aus Sachsen und Ehrenvorsitzender",
                ["MdB aus Sachsen", "Ehrenvorsitzender"],
            ),
            (
                "MdB aus Rheinland-Pfalz, umweltpolitischer Sprecher und "
                "Ex-Mitglied im Landesvorstand",
                [
                    "MdB aus Rheinland-Pfalz",
                    "umweltpolitischer Sprecher",
                    "Ex-Mitglied im Landesvorstand",
                ],
            ),
            # "und" inside "Bund"/"Bundestagsfraktion" must not trigger a split.
            (
                "Sprecher der Bundestagsfraktion im Bund",
                ["Sprecher der Bundestagsfraktion im Bund"],
            ),
            ("", []),
            ("   ", []),
        ],
    )
    def test_segments(self, label, expected):
        assert segment_positions(label) == expected
