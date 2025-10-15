from datetime import date
from unittest import mock

import pytest

from froide_evidencecollection.abgeordnetenwatch import (
    CANDIDATE_ROLE_UUID,
    MANDATE_ROLE_UUID,
    AbgeordnetenwatchDataFetcher,
    AbgeordnetenwatchImporter,
    CandidacyImporter,
    ElectionImporter,
    ImportError,
    LegislativePeriodImporter,
    MandateImporter,
    ParliamentImporter,
    PoliticianImporter,
)
from froide_evidencecollection.models import (
    Affiliation,
    Election,
    LegislativePeriod,
    Parliament,
    Person,
)
from froide_evidencecollection.tests.factories import (
    OrganizationFactory,
    RoleFactory,
)


@pytest.fixture(scope="session")
def aw_api_response():
    def _create_response(data):
        return {
            "meta": {
                "result": {
                    "count": len(data),
                    "total": len(data),
                    "page": 0,
                    "results_per_page": 100,
                },
            },
            "data": data,
        }

    return _create_response


@pytest.fixture(scope="session")
def aw_parliament_response(aw_api_response):
    data = [
        {
            "id": 1,
            "label_external_long": "EU-Parlament",
            "api_url": "https://www.abgeordnetenwatch.de/api/v2/parliaments/1",
            "abgeordnetenwatch_url": "https://www.abgeordnetenwatch.de/eu",
        }
    ]

    return aw_api_response(data)


@pytest.fixture(scope="session")
def aw_election_response(aw_api_response):
    data = [
        {
            "id": 151,
            "label": "EU-Parlament Wahl 2024",
            "parliament": {"id": 1},
            "start_date_period": "2024-04-29",
            "end_date_period": "2024-06-09",
            "election_date": "2024-06-10",
        }
    ]

    return aw_api_response(data)


@pytest.fixture(scope="session")
def aw_legislative_period_response(aw_api_response):
    def _create_legislative_period_response(with_values=True):
        data = [
            {
                "id": 155,
                "label": "EU-Parlament 2024 - 2029",
                "parliament": {"id": 1},
                "start_date_period": "2024-07-02",
                "end_date_period": "2029-07-01",
                "previous_period": {"id": 151} if with_values else None,
            }
        ]
        return aw_api_response(data)

    return _create_legislative_period_response


@pytest.fixture(scope="session")
def aw_politician_response(aw_api_response):
    def _create_politician_response(with_values=True):
        data = [
            {
                "id": 12346,
                "first_name": "Maxi",
                "last_name": "Musterfrau",
                "qid_wikidata": "Q123456" if with_values else None,
                "field_title": "Dr." if with_values else None,
            }
        ]
        return aw_api_response(data)

    return _create_politician_response


@pytest.fixture(scope="session")
def aw_candidacy_response(aw_api_response):
    def _create_candidacy_response(with_values=True):
        data = [
            {
                "id": 67890,
                "politician": {"id": 12346},
                "parliament_period": {"id": 151},
                "organization": {"id": 1},
                "start_date": "2024-05-01" if with_values else None,
                "end_date": "2024-05-30" if with_values else None,
                "info": "Info zur Kandidatur" if with_values else None,
                "api_url": "https://www.abgeordnetenwatch.de/api/v2/candidacies-mandates/67890",
            }
        ]
        return aw_api_response(data)

    return _create_candidacy_response


@pytest.fixture(scope="session")
def aw_mandate_response(aw_api_response):
    def _create_mandate_response(with_values=True):
        data = [
            {
                "id": 54321,
                "politician": {"id": 12346},
                "parliament_period": {"id": 155},
                "organization": {"id": 1},
                "start_date": "2025-01-01" if with_values else None,
                "end_date": "2025-09-30" if with_values else None,
                "info": "Info zum Mandat" if with_values else None,
                "api_url": "https://www.abgeordnetenwatch.de/api/v2/candidacies-mandates/54321",
            }
        ]
        return aw_api_response(data)

    return _create_mandate_response


@pytest.fixture()
def aw_mock_response(
    fxt_mock_response,
    aw_parliament_response,
    aw_election_response,
    aw_legislative_period_response,
    aw_politician_response,
    aw_candidacy_response,
    aw_mandate_response,
):
    def _create_mock_response(with_values=True):
        data_responses = {
            "parliaments": aw_parliament_response,
            "parliament-periods": aw_legislative_period_response(
                with_values=with_values
            ),
            "politicians": aw_politician_response(with_values=with_values),
            "candidacies-mandates": aw_mandate_response(with_values=with_values),
        }

        def side_effect(url, *args, **kwargs):
            params = kwargs.get("params", {})

            for key, response in data_responses.items():
                if key in url:
                    if key == "parliament-periods":
                        if params["type"] == "election":
                            return fxt_mock_response(aw_election_response)
                    elif key == "candidacies-mandates":
                        if params["type"] == "candidacy":
                            return fxt_mock_response(
                                aw_candidacy_response(with_values=with_values)
                            )
                    return fxt_mock_response(response)

        return side_effect

    return _create_mock_response


@pytest.fixture
def organization():
    return OrganizationFactory(
        organization_name="Fraktion im EU-Parlament",
    )


@pytest.fixture
def parliament(organization):
    return Parliament.objects.create(
        aw_id=1, name="EU-Parlament", fraction=organization
    )


@pytest.fixture
def election(parliament):
    return parliament.elections.create(
        aw_id=151,
        name="EU-Parlament Wahl 2024",
        start_date=date(2024, 4, 29),
        end_date=date(2024, 6, 9),
    )


@pytest.fixture
def legislative_period(parliament, election):
    return parliament.legislative_periods.create(
        aw_id=155,
        name="EU-Parlament 2024 - 2029",
        election=election,
        start_date=date(2024, 7, 2),
        end_date=date(2029, 7, 1),
    )


@pytest.fixture
def person():
    return Person.objects.create(
        aw_id=12346,
        first_name="Maxi",
        last_name="Musterfrau",
        title="Dr.",
        wikidata_id="Q123456",
    )


@pytest.fixture
def mandate_role():
    return RoleFactory(sync_uuid=MANDATE_ROLE_UUID, name="Abgeordnete*r")


@pytest.fixture
def candidate_role():
    return RoleFactory(sync_uuid=CANDIDATE_ROLE_UUID, name="Kandidatur")


@pytest.fixture
def mandate_affiliation(legislative_period, parliament, person, mandate_role):
    return Affiliation.objects.create(
        aw_id=54321,
        person=person,
        organization=parliament.fraction,
        role=mandate_role,
        start_date=date(2025, 1, 1),
        end_date=date(2025, 9, 30),
        comment="Info zum Mandat",
    )


@pytest.fixture
def candidate_affiliation(legislative_period, parliament, person, candidate_role):
    return Affiliation.objects.create(
        aw_id=67890,
        person=person,
        organization=parliament.fraction,
        role=candidate_role,
        start_date=date(2024, 5, 1),
        end_date=date(2024, 5, 30),
        comment="Info zur Kandidatur",
    )


class TestParliamentImporter:
    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.abgeordnetenwatch.requests.get")
    def test_run(self, mock_get, aw_mock_response, organization):
        mock_get.side_effect = aw_mock_response()

        importer = ParliamentImporter()
        importer.run()

        mock_get.assert_called_once()

        parliaments = Parliament.objects.all()
        assert parliaments.count() == 1
        assert parliaments[0].aw_id == 1
        assert parliaments[0].name == "EU-Parlament"
        assert parliaments[0].fraction == organization

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.abgeordnetenwatch.requests.get")
    def test_run_with_existing_parliament(
        self,
        mock_get,
        aw_mock_response,
        parliament,
    ):
        mock_get.side_effect = aw_mock_response()

        importer = ParliamentImporter()
        importer.run()

        mock_get.assert_called_once()

        parliaments = Parliament.objects.all()
        assert parliaments.count() == 1
        assert parliaments[0] == parliament

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.abgeordnetenwatch.requests.get")
    def test_run_with_missing_fraction(self, mock_get, aw_mock_response):
        mock_get.side_effect = aw_mock_response()

        importer = ParliamentImporter()
        msg = "Error finding fraction: No matching fraction found for parliament EU-Parlament"
        with pytest.raises(ImportError, match=msg):
            importer.run()

        mock_get.assert_called_once()

        assert Parliament.objects.exists() is False

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.abgeordnetenwatch.requests.get")
    def test_run_with_multiple_fractions(
        self,
        mock_get,
        aw_mock_response,
    ):
        OrganizationFactory(
            organization_name="Fraktion im EU-Parlament",
        )
        OrganizationFactory(
            organization_name="Fraktion im EU-Parlament",
        )

        mock_get.side_effect = aw_mock_response()

        importer = ParliamentImporter()
        msg = (
            "Error finding fraction: Multiple matching fractions found for parliament EU-Parlament:"
            " Fraktion im EU-Parlament, Fraktion im EU-Parlament"
        )
        with pytest.raises(ImportError, match=msg):
            importer.run()

        mock_get.assert_called_once()

        assert Parliament.objects.exists() is False


class TestElectionImporter:
    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.abgeordnetenwatch.requests.get")
    def test_run(self, mock_get, aw_mock_response, parliament):
        mock_get.side_effect = aw_mock_response()

        importer = ElectionImporter()
        importer.run()

        mock_get.assert_called_once()

        elections = Election.objects.all()
        assert len(elections) == 1
        assert elections[0].aw_id == 151
        assert elections[0].name == "EU-Parlament Wahl 2024"
        assert elections[0].parliament == parliament
        assert elections[0].start_date == date(2024, 4, 29)
        assert elections[0].end_date == date(2024, 6, 10)

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.abgeordnetenwatch.requests.get")
    def test_run_with_existing_election(
        self,
        mock_get,
        aw_mock_response,
        election,
    ):
        mock_get.side_effect = aw_mock_response()

        importer = ElectionImporter()
        importer.run()

        mock_get.assert_called_once()

        elections = Election.objects.all()
        assert len(elections) == 1
        assert elections[0] == election

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.abgeordnetenwatch.requests.get")
    def test_run_with_missing_parliament(
        self,
        mock_get,
        aw_mock_response,
    ):
        mock_get.side_effect = aw_mock_response()

        importer = ElectionImporter()
        msg = "Parliament with abgeordnetenwatch ID 1 not found"

        with pytest.raises(ImportError, match=msg):
            importer.run()

        mock_get.assert_called_once()

        elections = Election.objects.all()
        assert len(elections) == 0


class TestLegislativePeriodImporter:
    @pytest.mark.django_db
    @pytest.mark.parametrize("with_values", [True, False])
    @mock.patch("froide_evidencecollection.abgeordnetenwatch.requests.get")
    def test_run(self, mock_get, with_values, aw_mock_response, parliament, election):
        mock_get.side_effect = aw_mock_response(with_values=with_values)

        importer = LegislativePeriodImporter()
        importer.run()

        mock_get.assert_called_once()

        periods = LegislativePeriod.objects.all()
        assert periods.count() == 1
        assert periods[0].aw_id == 155
        assert periods[0].name == "EU-Parlament 2024 - 2029"
        assert periods[0].parliament == parliament
        if with_values:
            assert periods[0].election == election
        else:
            assert periods[0].election is None
        assert periods[0].start_date == date(2024, 7, 2)
        assert periods[0].end_date == date(2029, 7, 1)

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.abgeordnetenwatch.requests.get")
    def test_run_with_existing_period(
        self,
        mock_get,
        aw_mock_response,
        legislative_period,
    ):
        mock_get.side_effect = aw_mock_response()

        importer = LegislativePeriodImporter()
        importer.run()

        mock_get.assert_called_once()

        periods = LegislativePeriod.objects.all()
        assert periods.count() == 1
        assert periods[0] == legislative_period

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.abgeordnetenwatch.requests.get")
    def test_run_with_missing_parliament(
        self,
        mock_get,
        aw_mock_response,
    ):
        mock_get.side_effect = aw_mock_response()

        importer = LegislativePeriodImporter()
        msg = "Parliament with abgeordnetenwatch ID 1 not found"

        with pytest.raises(ImportError, match=msg):
            importer.run()

        mock_get.assert_called_once()

        assert LegislativePeriod.objects.exists() is False

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.abgeordnetenwatch.requests.get")
    def test_run_with_already_linked_election(
        self,
        mock_get,
        aw_mock_response,
        election,
    ):
        # Existing legislative period linked to the election.
        LegislativePeriod.objects.create(
            aw_id=234,
            name="Andere Legislaturperiode",
            parliament=election.parliament,
            election=election,
            start_date=date(2020, 1, 1),
            end_date=date(2024, 1, 1),
        )

        mock_get.side_effect = aw_mock_response()

        importer = LegislativePeriodImporter()

        msg = "Election EU-Parlament Wahl 2024 is already linked to legislative period Andere Legislaturperiode"
        with pytest.raises(ImportError, match=msg):
            importer.run()

        mock_get.assert_called_once()

        assert LegislativePeriod.objects.count() == 1


class TestPoliticianImporter:
    @pytest.mark.django_db
    @pytest.mark.parametrize("with_values", [True, False])
    @mock.patch("froide_evidencecollection.abgeordnetenwatch.requests.get")
    def test_create(self, mock_get, aw_mock_response, with_values):
        mock_get.side_effect = aw_mock_response(with_values=with_values)

        importer = PoliticianImporter()
        importer.run()

        mock_get.assert_called_once()

        persons = Person.objects.all()
        assert persons.count() == 1
        assert persons[0].aw_id == 12346
        assert persons[0].first_name == "Maxi"
        assert persons[0].last_name == "Musterfrau"
        if with_values:
            assert persons[0].wikidata_id == "Q123456"
            assert persons[0].title == "Dr."
        else:
            assert persons[0].wikidata_id is None
            assert persons[0].title == ""

        stats = importer.stats.to_dict()
        assert stats["Person"] == {
            "created": [
                {
                    "id": persons[0].id,
                    "fields": {
                        "also_known_as": [],
                        "aw_id": 12346,
                        "external_id": None,
                        "first_name": "Maxi",
                        "last_name": "Musterfrau",
                        "status": None,
                        "sync_uuid": str(persons[0].sync_uuid),
                        "title": "Dr." if with_values else "",
                        "wikidata_id": "Q123456" if with_values else None,
                    },
                }
            ],
            "updated": [],
            "deleted": [],
            "skipped": [],
        }

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.abgeordnetenwatch.requests.get")
    def test_update_if_value_is_not_set(self, mock_get, aw_mock_response, person):
        # Ensure existing person has no wikidata_id to test update.
        person.wikidata_id = None
        person.save(sync=True)

        mock_get.side_effect = aw_mock_response()

        importer = PoliticianImporter()
        importer.run()

        mock_get.assert_called_once()

        persons = Person.objects.all()
        assert persons.count() == 1
        updated_person = persons[0]
        assert updated_person.id == person.id
        assert updated_person.aw_id == 12346
        assert updated_person.first_name == "Maxi"
        assert updated_person.last_name == "Musterfrau"
        assert updated_person.title == "Dr."
        assert updated_person.wikidata_id == "Q123456"
        assert updated_person.is_synced is False

        stats = importer.stats.to_dict()
        assert stats["Person"] == {
            "created": [],
            "updated": [
                {
                    "id": updated_person.id,
                    "diff": {
                        "wikidata_id": {
                            "old": None,
                            "new": "Q123456",
                        },
                    },
                },
            ],
            "deleted": [],
            "skipped": [],
        }

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.abgeordnetenwatch.requests.get")
    def test_no_update_if_value_is_set(self, mock_get, aw_mock_response, person):
        assert person.wikidata_id == "Q123456"

        mock_get.side_effect = aw_mock_response(with_values=False)

        importer = PoliticianImporter()
        importer.run()

        mock_get.assert_called_once()

        persons = Person.objects.all()
        assert persons.count() == 1
        assert persons[0] == person
        assert persons[0].wikidata_id == "Q123456"

        stats = importer.stats.to_dict()
        assert stats == {}

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.abgeordnetenwatch.requests.get")
    def test_no_update_if_no_changes(self, mock_get, aw_mock_response, person):
        mock_get.side_effect = aw_mock_response()

        importer = PoliticianImporter()
        importer.run()

        mock_get.assert_called_once()

        persons = Person.objects.all()
        assert persons.count() == 1
        assert persons[0] == person

        stats = importer.stats.to_dict()
        assert stats == {}


class TestCandidacyImporter:
    @pytest.mark.django_db
    @pytest.mark.parametrize("with_values", [True, False])
    @pytest.mark.parametrize("with_existing_person", [False, True])
    @mock.patch("froide_evidencecollection.utils.get_today")
    @mock.patch("froide_evidencecollection.abgeordnetenwatch.requests.get")
    def test_create(
        self,
        mock_get,
        mock_today,
        with_existing_person,
        with_values,
        aw_mock_response,
        legislative_period,
        candidate_role,
    ):
        if with_existing_person:
            Person.objects.create(
                aw_id=12346,
                first_name="Maxi",
                last_name="Musterfrau",
                title="Dr.",
                wikidata_id="Q123456",
            )

        mock_get.side_effect = aw_mock_response(with_values=with_values)
        mock_today.return_value = date(2025, 10, 8)

        importer = CandidacyImporter()
        importer.run()

        # Create Affiliation and Person if not existing.
        assert mock_get.call_count == 2 if not with_existing_person else 1

        affiliations = Affiliation.objects.all()
        assert affiliations.count() == 1
        affiliation = affiliations[0]
        assert affiliation.aw_id == 67890
        assert affiliation.person.aw_id == 12346
        assert affiliation.organization == legislative_period.parliament.fraction
        assert affiliation.role.id == candidate_role.id
        assert affiliation.start_date == (
            date(2024, 5, 1) if with_values else date(2024, 4, 29)
        )
        assert affiliation.end_date == (
            date(2024, 5, 30) if with_values else date(2024, 6, 9)
        )
        assert affiliation.comment == ("Info zur Kandidatur" if with_values else "")

        stats = importer.stats.to_dict()
        assert stats["Affiliation"] == {
            "created": [
                {
                    "id": affiliation.id,
                    "fields": {
                        "aw_id": 67890,
                        "comment": "Info zur Kandidatur" if with_values else "",
                        "end_date": "2024-05-30" if with_values else "2024-06-09",
                        "end_date_string": "2024-05-30"
                        if with_values
                        else "2024-06-09",
                        "external_id": None,
                        "organization": affiliation.organization.id,
                        "person": affiliation.person.id,
                        "reference_url": "",
                        "role": candidate_role.id,
                        "start_date": "2024-05-01" if with_values else "2024-04-29",
                        "start_date_string": "2024-05-01"
                        if with_values
                        else "2024-04-29",
                        "sync_uuid": str(affiliation.sync_uuid),
                    },
                }
            ],
            "updated": [],
            "deleted": [],
            "skipped": [],
        }

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.abgeordnetenwatch.requests.get")
    def test_update_if_value_is_not_set(
        self, mock_get, aw_mock_response, candidate_affiliation
    ):
        candidate_affiliation.start_date = None
        candidate_affiliation.end_date = None
        candidate_affiliation.comment = ""
        candidate_affiliation.save(sync=True)

        mock_get.side_effect = aw_mock_response()

        importer = CandidacyImporter()
        importer.run()

        assert mock_get.call_count == 1

        affiliations = Affiliation.objects.all()
        assert affiliations.count() == 1
        assert affiliations[0] == candidate_affiliation
        assert affiliations[0].start_date == date(2024, 5, 1)
        assert affiliations[0].end_date == date(2024, 5, 30)
        assert affiliations[0].comment == "Info zur Kandidatur"

        stats = importer.stats.to_dict()
        assert stats["Affiliation"] == {
            "created": [],
            "updated": [
                {
                    "id": candidate_affiliation.id,
                    "diff": {
                        "start_date": {
                            "old": None,
                            "new": "2024-05-01",
                        },
                        "end_date": {
                            "old": None,
                            "new": "2024-05-30",
                        },
                        "comment": {
                            "old": "",
                            "new": "Info zur Kandidatur",
                        },
                    },
                }
            ],
            "deleted": [],
            "skipped": [],
        }

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.abgeordnetenwatch.requests.get")
    def test_no_update_if_value_is_set(
        self,
        mock_get,
        aw_mock_response,
        candidate_affiliation,
    ):
        candidate_affiliation.reference_url = "https://example.com"
        candidate_affiliation.comment = "Kommentar"
        candidate_affiliation.save(sync=True)
        assert candidate_affiliation.start_date == date(2024, 5, 1)
        assert candidate_affiliation.end_date == date(2024, 5, 30)

        mock_get.side_effect = aw_mock_response(with_values=False)

        importer = CandidacyImporter()
        importer.run()

        assert mock_get.call_count == 1

        affiliations = Affiliation.objects.all()
        assert affiliations.count() == 1
        assert affiliations[0] == candidate_affiliation
        assert affiliations[0].comment == "Kommentar"
        assert affiliations[0].reference_url == "https://example.com"
        # Dates should not be changed to fallback from election if already set.
        assert affiliations[0].start_date == date(2024, 5, 1)
        assert affiliations[0].end_date == date(2024, 5, 30)

        stats = importer.stats.to_dict()
        assert stats == {}

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.abgeordnetenwatch.requests.get")
    def test_no_update_if_no_changes(
        self,
        mock_get,
        aw_mock_response,
        candidate_affiliation,
    ):
        mock_get.side_effect = aw_mock_response()

        importer = CandidacyImporter()
        importer.run()

        assert mock_get.call_count == 1

        affiliations = Affiliation.objects.all()
        assert affiliations.count() == 1
        assert affiliations[0] == candidate_affiliation

        stats = importer.stats.to_dict()
        assert stats == {}

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.abgeordnetenwatch.requests.get")
    def test_run_with_missing_candidate_role(
        self,
        mock_get,
        aw_mock_response,
    ):
        msg = f"Role with sync UUID {CANDIDATE_ROLE_UUID} not found"

        with pytest.raises(ImportError, match=msg):
            CandidacyImporter()

        mock_get.assert_not_called()

        assert Affiliation.objects.exists() is False

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.abgeordnetenwatch.requests.get")
    def test_run_with_missing_election(
        self,
        mock_get,
        aw_mock_response,
        candidate_role,
    ):
        mock_get.side_effect = aw_mock_response()

        importer = CandidacyImporter()
        msg = "Election with abgeordnetenwatch ID 151 not found"

        with pytest.raises(ImportError, match=msg):
            importer.run()

        # Calls to Mandate and Politician API.
        assert mock_get.call_count == 2

        assert Affiliation.objects.exists() is False

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.abgeordnetenwatch.requests.get")
    def test_run_with_no_candidacies_returned_from_api(
        self,
        mock_get,
        fxt_mock_response,
        candidate_role,
    ):
        empty_reponse = {
            "meta": {
                "result": {
                    "count": 0,
                    "total": 0,
                    "page": 0,
                    "results_per_page": 500,
                },
                "data": [],
            }
        }

        mock_get.return_value = fxt_mock_response(empty_reponse)

        importer = CandidacyImporter()
        importer.run()

        # Only call Candidacy API, not Politician API if no candidacies are returned.
        assert mock_get.call_count == 1

        assert Affiliation.objects.exists() is False
        assert Person.objects.exists() is False

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.abgeordnetenwatch.CANDIDATE_ROLE_UUID", None)
    @mock.patch("froide_evidencecollection.abgeordnetenwatch.requests.get")
    def test_init_with_missing_candidate_role_config(self, mock_get):
        msg = "No candidacy role UUID configured for abgeordnetenwatch.de candidacy import"

        with pytest.raises(ImportError, match=msg):
            CandidacyImporter()

        assert mock_get.call_count == 0

        assert Affiliation.objects.exists() is False
        assert Person.objects.exists() is False

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.abgeordnetenwatch.PARTY_ID", None)
    @mock.patch("froide_evidencecollection.abgeordnetenwatch.requests.get")
    def test_run_with_missing_party_id_config(self, mock_get, candidate_role):
        msg = "No party ID configured for abgeordnetenwatch.de candidacy import"

        with pytest.raises(ImportError, match=msg):
            CandidacyImporter()

        assert mock_get.call_count == 0

        assert Affiliation.objects.exists() is False
        assert Person.objects.exists() is False


class TestMandateImporter:
    @pytest.mark.django_db
    @pytest.mark.parametrize("with_values", [True, False])
    @mock.patch("froide_evidencecollection.utils.get_today")
    @mock.patch("froide_evidencecollection.abgeordnetenwatch.requests.get")
    def test_create(
        self,
        mock_get,
        mock_today,
        with_values,
        aw_mock_response,
        legislative_period,
        candidate_affiliation,
        mandate_role,
    ):
        mock_get.side_effect = aw_mock_response(with_values=with_values)
        mock_today.return_value = date(2025, 10, 8)

        importer = MandateImporter()
        importer.run()

        assert mock_get.call_count == 1

        affiliations = Affiliation.objects.all()
        assert affiliations.count() == 2
        assert candidate_affiliation in affiliations

        mandate = affiliations.exclude(id=candidate_affiliation.id).first()
        assert mandate.aw_id == 54321
        assert mandate.person.aw_id == 12346
        assert mandate.organization == legislative_period.parliament.fraction
        assert mandate.role.id == mandate_role.id
        assert mandate.start_date == (
            date(2025, 1, 1) if with_values else date(2024, 7, 2)
        )
        assert mandate.end_date == (date(2025, 9, 30) if with_values else None)
        assert mandate.comment == ("Info zum Mandat" if with_values else "")

        stats = importer.stats.to_dict()
        assert stats["Affiliation"] == {
            "created": [
                {
                    "id": mandate.id,
                    "fields": {
                        "aw_id": 54321,
                        "comment": "Info zum Mandat" if with_values else "",
                        "end_date": "2025-09-30" if with_values else None,
                        "end_date_string": "2025-09-30" if with_values else "",
                        "external_id": None,
                        "organization": mandate.organization.id,
                        "person": mandate.person.id,
                        "reference_url": "",
                        "role": mandate_role.id,
                        "start_date": "2025-01-01" if with_values else "2024-07-02",
                        "start_date_string": "2025-01-01"
                        if with_values
                        else "2024-07-02",
                        "sync_uuid": str(mandate.sync_uuid),
                    },
                }
            ],
            "updated": [],
            "deleted": [],
            "skipped": [],
        }

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.abgeordnetenwatch.requests.get")
    def test_update_if_value_is_not_set(
        self, mock_get, aw_mock_response, candidate_affiliation, mandate_affiliation
    ):
        mandate_affiliation.start_date = None
        mandate_affiliation.end_date = None
        mandate_affiliation.comment = ""
        mandate_affiliation.save(sync=True)

        mock_get.side_effect = aw_mock_response()

        importer = MandateImporter()
        importer.run()

        assert mock_get.call_count == 1

        affiliations = Affiliation.objects.all()
        assert affiliations.count() == 2
        assert candidate_affiliation in affiliations
        assert mandate_affiliation in affiliations

        mandate_affiliation.refresh_from_db()
        assert mandate_affiliation.start_date == date(2025, 1, 1)
        assert mandate_affiliation.end_date == date(2025, 9, 30)
        assert mandate_affiliation.comment == "Info zum Mandat"

        stats = importer.stats.to_dict()
        assert stats["Affiliation"] == {
            "created": [],
            "updated": [
                {
                    "id": mandate_affiliation.id,
                    "diff": {
                        "start_date": {
                            "old": None,
                            "new": "2025-01-01",
                        },
                        "end_date": {
                            "old": None,
                            "new": "2025-09-30",
                        },
                        "comment": {
                            "old": "",
                            "new": "Info zum Mandat",
                        },
                    },
                }
            ],
            "deleted": [],
            "skipped": [],
        }

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.abgeordnetenwatch.requests.get")
    def test_no_update_if_value_is_set(
        self,
        mock_get,
        aw_mock_response,
        mandate_affiliation,
    ):
        mandate_affiliation.comment = "Kommentar"
        mandate_affiliation.reference_url = "https://example.com"
        mandate_affiliation.save(sync=True)
        assert mandate_affiliation.start_date == date(2025, 1, 1)
        assert mandate_affiliation.end_date == date(2025, 9, 30)

        mock_get.side_effect = aw_mock_response()

        importer = MandateImporter()
        importer.run()

        assert mock_get.call_count == 1

        affiliations = Affiliation.objects.all()
        assert affiliations.count() == 1
        assert affiliations[0] == mandate_affiliation
        assert affiliations[0].comment == "Kommentar"
        assert affiliations[0].reference_url == "https://example.com"
        # Dates should not be changed to fallback from legislative period if already set.
        assert affiliations[0].start_date == date(2025, 1, 1)
        assert affiliations[0].end_date == date(2025, 9, 30)

        stats = importer.stats.to_dict()
        assert stats == {}

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.abgeordnetenwatch.requests.get")
    def test_no_update_if_no_changes(
        self,
        mock_get,
        aw_mock_response,
        mandate_affiliation,
    ):
        mock_get.side_effect = aw_mock_response()

        importer = MandateImporter()
        importer.run()

        assert mock_get.call_count == 1

        affiliations = Affiliation.objects.all()
        assert affiliations.count() == 1
        assert affiliations[0] == mandate_affiliation

        stats = importer.stats.to_dict()
        assert stats == {}

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.abgeordnetenwatch.requests.get")
    def test_run_with_missing_mandate_role(
        self,
        mock_get,
        aw_mock_response,
    ):
        msg = f"Role with sync UUID {MANDATE_ROLE_UUID} not found"

        with pytest.raises(ImportError, match=msg):
            MandateImporter()

        mock_get.assert_not_called()

        assert Affiliation.objects.exists() is False

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.abgeordnetenwatch.requests.get")
    def test_run_with_missing_legislative_period(
        self,
        mock_get,
        aw_mock_response,
        mandate_role,
    ):
        mock_get.side_effect = aw_mock_response()

        importer = MandateImporter()
        msg = "LegislativePeriod with abgeordnetenwatch ID 155 not found"

        with pytest.raises(ImportError, match=msg):
            importer.run()

        # Calls to Mandate and Politician API.
        assert mock_get.call_count == 2

        assert Affiliation.objects.exists() is False

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.abgeordnetenwatch.requests.get")
    def test_run_with_no_mandates_returned_from_api(
        self,
        mock_get,
        fxt_mock_response,
        mandate_role,
    ):
        empty_reponse = {
            "meta": {
                "result": {
                    "count": 0,
                    "total": 0,
                    "page": 0,
                    "results_per_page": 500,
                },
                "data": [],
            }
        }

        mock_get.return_value = fxt_mock_response(empty_reponse)

        importer = MandateImporter()
        importer.run()

        # Only call Mandate API, not Politician API if no mandates are returned.
        assert mock_get.call_count == 1

        assert Affiliation.objects.exists() is False
        assert Person.objects.exists() is False

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.abgeordnetenwatch.MANDATE_ROLE_UUID", None)
    @mock.patch("froide_evidencecollection.abgeordnetenwatch.requests.get")
    def test_init_with_missing_mandate_role_config(self, mock_get):
        msg = "No mandate role UUID configured for abgeordnetenwatch.de mandate import"

        with pytest.raises(ImportError, match=msg):
            MandateImporter()

        assert mock_get.call_count == 0

        assert Affiliation.objects.exists() is False
        assert Person.objects.exists() is False

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.abgeordnetenwatch.FRACTIONS", None)
    @mock.patch("froide_evidencecollection.abgeordnetenwatch.requests.get")
    def test_run_with_missing_fraction_config(self, mock_get, mandate_role):
        msg = "No fractions configured for abgeordnetenwatch.de mandate import"

        with pytest.raises(ImportError, match=msg):
            MandateImporter()

        assert mock_get.call_count == 0

        assert Affiliation.objects.exists() is False
        assert Person.objects.exists() is False


class TestAbgeordnetenwatchImporter:
    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.utils.get_today")
    @mock.patch("froide_evidencecollection.abgeordnetenwatch.requests.get")
    def test_run(
        self,
        mock_get,
        mock_today,
        aw_mock_response,
        organization,
        mandate_role,
        candidate_role,
    ):
        mock_get.side_effect = aw_mock_response()
        mock_today.return_value = date(2025, 10, 8)

        importer = AbgeordnetenwatchImporter()
        importer.run()

        assert mock_get.call_count == 6

        parliaments = Parliament.objects.all()
        assert parliaments.count() == 1
        assert parliaments[0].aw_id == 1
        assert parliaments[0].name == "EU-Parlament"
        assert parliaments[0].fraction == organization

        elections = Election.objects.all()
        assert elections.count() == 1
        assert elections[0].aw_id == 151
        assert elections[0].name == "EU-Parlament Wahl 2024"
        assert elections[0].parliament == parliaments[0]
        assert elections[0].start_date == date(2024, 4, 29)
        assert elections[0].end_date == date(2024, 6, 10)

        periods = LegislativePeriod.objects.all()
        assert periods.count() == 1
        assert periods[0].aw_id == 155
        assert periods[0].name == "EU-Parlament 2024 - 2029"
        assert periods[0].parliament == parliaments[0]
        assert periods[0].election == elections[0]
        assert periods[0].start_date == date(2024, 7, 2)
        assert periods[0].end_date == date(2029, 7, 1)

        persons = Person.objects.all()
        assert persons.count() == 1
        assert persons[0].aw_id == 12346
        assert persons[0].first_name == "Maxi"
        assert persons[0].last_name == "Musterfrau"
        assert persons[0].wikidata_id == "Q123456"
        assert persons[0].title == "Dr."

        affiliations = Affiliation.objects.all()
        assert affiliations.count() == 2

        candidacy = affiliations[0]
        assert candidacy.aw_id == 67890
        assert candidacy.person == persons[0]
        assert candidacy.organization == organization
        assert candidacy.role == candidate_role
        assert candidacy.start_date == date(2024, 5, 1)
        assert candidacy.end_date == date(2024, 5, 30)

        mandate = affiliations[1]
        assert mandate.aw_id == 54321
        assert mandate.person == persons[0]
        assert mandate.organization == organization
        assert mandate.role == mandate_role
        assert mandate.start_date == date(2025, 1, 1)
        assert mandate.end_date == date(2025, 9, 30)

        stats = importer.log_stats()
        assert len(stats) == 2
        assert stats["Person"] == {
            "created": [
                {
                    "id": persons[0].id,
                    "fields": {
                        "also_known_as": [],
                        "aw_id": 12346,
                        "external_id": None,
                        "first_name": "Maxi",
                        "last_name": "Musterfrau",
                        "status": None,
                        "sync_uuid": str(persons[0].sync_uuid),
                        "title": "Dr.",
                        "wikidata_id": "Q123456",
                    },
                }
            ],
            "updated": [],
            "deleted": [],
            "skipped": [],
        }
        assert stats["Affiliation"] == {
            "created": [
                {
                    "id": candidacy.id,
                    "fields": {
                        "aw_id": 67890,
                        "comment": "Info zur Kandidatur",
                        "end_date": "2024-05-30",
                        "end_date_string": "2024-05-30",
                        "external_id": None,
                        "organization": candidacy.organization.id,
                        "person": candidacy.person.id,
                        "reference_url": "",
                        "role": candidate_role.id,
                        "start_date": "2024-05-01",
                        "start_date_string": "2024-05-01",
                        "sync_uuid": str(candidacy.sync_uuid),
                    },
                },
                {
                    "id": mandate.id,
                    "fields": {
                        "aw_id": 54321,
                        "comment": "Info zum Mandat",
                        "end_date": "2025-09-30",
                        "end_date_string": "2025-09-30",
                        "external_id": None,
                        "organization": mandate.organization.id,
                        "person": mandate.person.id,
                        "reference_url": "",
                        "role": mandate_role.id,
                        "start_date": "2025-01-01",
                        "start_date_string": "2025-01-01",
                        "sync_uuid": str(mandate.sync_uuid),
                    },
                },
            ],
            "updated": [],
            "deleted": [],
            "skipped": [],
        }

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.abgeordnetenwatch.requests.get")
    def test_run_only_setup(self, mock_get, aw_mock_response, organization):
        mock_get.side_effect = aw_mock_response()

        importer = AbgeordnetenwatchImporter(only_setup=True)
        importer.run()

        assert mock_get.call_count == 3

        parliaments = Parliament.objects.all()
        assert parliaments.count() == 1
        assert parliaments[0].aw_id == 1
        assert parliaments[0].name == "EU-Parlament"
        assert parliaments[0].fraction == organization

        elections = Election.objects.all()
        assert elections.count() == 1
        assert elections[0].aw_id == 151
        assert elections[0].name == "EU-Parlament Wahl 2024"
        assert elections[0].parliament == parliaments[0]
        assert elections[0].start_date == date(2024, 4, 29)
        assert elections[0].end_date == date(2024, 6, 10)

        periods = LegislativePeriod.objects.all()
        assert periods.count() == 1
        assert periods[0].aw_id == 155
        assert periods[0].name == "EU-Parlament 2024 - 2029"
        assert periods[0].parliament == parliaments[0]
        assert periods[0].election == elections[0]
        assert periods[0].start_date == date(2024, 7, 2)
        assert periods[0].end_date == date(2029, 7, 1)

        assert Person.objects.exists() is False
        assert Affiliation.objects.exists() is False

        stats = importer.log_stats()
        assert len(stats) == 0


@pytest.fixture
def multi_page_responses():
    """Fixture that simulates multiple pages of API responses."""

    def create_response(page):
        if page == 0:
            # First page: 2 entries
            return {
                "data": [
                    {"id": 1, "label": "Item 1"},
                    {"id": 2, "label": "Item 2"},
                ],
                "meta": {
                    "result": {
                        "count": 2,
                        "total": 5,
                        "page": 0,
                        "results_per_page": 2,
                    }
                },
            }
        elif page == 1:
            # Second page: 2 more entries
            return {
                "data": [
                    {"id": 3, "label": "Item 3"},
                    {"id": 4, "label": "Item 4"},
                ],
                "meta": {
                    "result": {
                        "count": 2,
                        "total": 5,
                        "page": 1,
                        "results_per_page": 2,
                    }
                },
            }
        else:
            # Last page: 1 entry
            return {
                "data": [
                    {"id": 5, "label": "Item 5"},
                ],
                "meta": {
                    "result": {
                        "count": 1,
                        "total": 5,
                        "page": 2,
                        "results_per_page": 2,
                    }
                },
            }

    return create_response


class TestAbgeordnetenwatchDataFetcher:
    @mock.patch("froide_evidencecollection.abgeordnetenwatch.RESULTS_PER_PAGE", 2)
    @mock.patch("froide_evidencecollection.abgeordnetenwatch.requests.get")
    def test_iter_rows_multiple_pages(
        self, mock_get, fxt_mock_response, multi_page_responses
    ):
        """Test that iter_rows correctly handles multiple pages of results."""

        # MockResponse based on the provided offset.
        def side_effect(url, params=None, **kwargs):
            page = params.get("page", 0)
            return fxt_mock_response(multi_page_responses(page))

        mock_get.side_effect = side_effect

        fetcher = AbgeordnetenwatchDataFetcher("test-entity")
        results = list(fetcher.iter_rows())

        assert mock_get.call_count == 3
        calls = mock_get.call_args_list

        # First call
        assert calls[0][1]["params"] == {
            "sort_by": "id",
            "sort_direction": "asc",
            "page": 0,
            "pager_limit": 2,
        }

        # Second call
        assert calls[1][1]["params"] == {
            "sort_by": "id",
            "sort_direction": "asc",
            "page": 1,
            "pager_limit": 2,
        }

        # Third call
        assert calls[2][1]["params"] == {
            "sort_by": "id",
            "sort_direction": "asc",
            "page": 2,
            "pager_limit": 2,
        }

        assert len(results) == 5
        assert [item["id"] for item in results] == [1, 2, 3, 4, 5]

    @mock.patch("froide_evidencecollection.abgeordnetenwatch.RESULTS_PER_PAGE", 2)
    @mock.patch("froide_evidencecollection.abgeordnetenwatch.requests.get")
    def test_iter_rows_with_extra_params(
        self, mock_get, fxt_mock_response, multi_page_responses
    ):
        """Test that iter_rows correctly includes extra parameters in each request."""

        # MockResponse based on the provided offset.
        def side_effect(url, params=None, **kwargs):
            page = params.get("page", 0)
            return fxt_mock_response(multi_page_responses(page))

        mock_get.side_effect = side_effect

        fetcher = AbgeordnetenwatchDataFetcher("test-entity")
        extra_params = {"filter": "value", "another": "parameter"}
        results = list(fetcher.iter_rows(extra_params))

        assert mock_get.call_count == 3
        calls = mock_get.call_args_list

        # First call with extra parameters.
        assert calls[0][1]["params"] == {
            "sort_by": "id",
            "sort_direction": "asc",
            "page": 0,
            "pager_limit": 2,
            "filter": "value",
            "another": "parameter",
        }

        # Second call with extra parameters.
        assert calls[1][1]["params"] == {
            "sort_by": "id",
            "sort_direction": "asc",
            "page": 1,
            "pager_limit": 2,
            "filter": "value",
            "another": "parameter",
        }

        # Third call with extra parameters.
        assert calls[2][1]["params"] == {
            "sort_by": "id",
            "sort_direction": "asc",
            "page": 2,
            "pager_limit": 2,
            "filter": "value",
            "another": "parameter",
        }

        assert len(results) == 5
        assert [item["id"] for item in results] == [1, 2, 3, 4, 5]
