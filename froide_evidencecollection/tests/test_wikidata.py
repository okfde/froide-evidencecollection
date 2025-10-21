from unittest import mock

import pytest

from froide_evidencecollection.models import Person
from froide_evidencecollection.wikidata import ImportError, WikidataImporter


@pytest.fixture
def wikidata_response(fxt_mock_response):
    data = {
        "results": {
            "bindings": [
                {
                    "item": {"value": "http://www.wikidata.org/entity/Q123"},
                    "aw_id": {"value": "123"},
                },
                {
                    "item": {"value": "http://www.wikidata.org/entity/Q234"},
                    "aw_id": {"value": "456"},
                },
            ]
        }
    }

    return data


@pytest.fixture
def wikidata_mock_response(fxt_mock_response, wikidata_response):
    return fxt_mock_response(wikidata_response)


class TestWikidataImporter:
    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.wikidata.BATCH_SIZE", 1)
    @mock.patch("froide_evidencecollection.wikidata.requests.get")
    def test_run(self, mock_get, wikidata_mock_response):
        # Create test persons without wikidata_id.
        person1 = Person(aw_id=123)
        person1.save(sync=True)
        person2 = Person(aw_id=456)
        person2.save(sync=True)

        mock_get.return_value = wikidata_mock_response

        importer = WikidataImporter()
        importer.run()

        # Two calls due to BATCH_SIZE=1.
        assert mock_get.call_count == 2

        person1.refresh_from_db()
        person2.refresh_from_db()

        assert person1.wikidata_id == "Q123"
        assert person1.is_synced is False
        assert person2.wikidata_id == "Q234"
        assert person2.is_synced is False

        stats = importer.log_stats()
        assert stats["Person"] == {
            "created": [],
            "updated": [
                {
                    "id": person1.id,
                    "diff": {"wikidata_id": {"old": None, "new": "Q123"}},
                },
                {
                    "id": person2.id,
                    "diff": {"wikidata_id": {"old": None, "new": "Q234"}},
                },
            ],
            "skipped": [],
            "deleted": [],
        }

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.wikidata.requests.get")
    def test_run_with_existing_wikidata_id(self, mock_get, wikidata_mock_response):
        person = Person(aw_id=123, wikidata_id="Q123")
        person.save(sync=True)

        mock_get.return_value = wikidata_mock_response

        importer = WikidataImporter()
        importer.run()

        mock_get.assert_not_called()

        person.refresh_from_db()

        assert Person.objects.count() == 1
        # person1 should remain unchanged.
        assert person.wikidata_id == "Q123"
        assert person.is_synced is True

        stats = importer.log_stats()
        assert stats == {}

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.wikidata.requests.get")
    def test_run_with_existing_wikidata_id_on_other_instance(
        self, mock_get, wikidata_mock_response
    ):
        # Create test persons, one with existing wikidata_id.
        person1 = Person(aw_id=123)
        person1.save(sync=True)
        person2 = Person(first_name="Horst", wikidata_id="Q123")
        person2.save(sync=True)

        mock_get.return_value = wikidata_mock_response

        importer = WikidataImporter()
        msg = "Some Wikidata IDs are already assigned to exising persons: Horst \\(Q123\\)"
        with pytest.raises(ImportError, match=msg):
            importer.run()

        mock_get.assert_called_once()

        person1.refresh_from_db()
        person2.refresh_from_db()

        # person1 should not be updated due to the error.
        assert person1.wikidata_id is None
        assert person1.is_synced is True
        # person2 should remain unchanged.
        assert person2.wikidata_id == "Q123"
        assert person2.is_synced is True

        stats = importer.log_stats()
        assert stats == {}

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.wikidata.requests.get")
    def test_run_without_aw_ids(self, mock_get):
        importer = WikidataImporter()
        importer.run()

        mock_get.assert_not_called()

        stats = importer.log_stats()
        assert stats == {}
