from unittest import mock
from uuid import UUID

import pytest

from froide_evidencecollection.importer import ImportError, RoleImporter
from froide_evidencecollection.models import Role


@pytest.fixture
def fxt_mock_response():
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data

        def raise_for_status(self):
            if self.status_code != 200:
                raise Exception(f"HTTP Error: {self.status_code}")

    def _make_mock_response(json_data, status_code=200):
        return MockResponse(json_data, status_code)

    return _make_mock_response


class TestRoleImporter:
    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.importer.requests.get")
    def test_importing_new_record_without_sync_uuid(self, mock_get, fxt_mock_response):
        """
        Test importing a data record from NocoDB that does not yet exist in the local database.

        The record does not have a Sync-UUID, so a new Sync-UUID should be created,
        and the object should not be marked as synced.
        """
        mock_get.return_value = fxt_mock_response(
            {
                "list": [
                    {
                        "Id": 37,
                        "CreatedAt": "2025-08-29 14:45:05+00:00",
                        "UpdatedAt": "2025-08-29 14:45:11+00:00",
                        "Bezeichnung": "Mitglied",
                        "Zugehörigkeiten": 0,
                        "Sync-UUID": None,
                    }
                ]
            }
        )

        importer = RoleImporter(Role)
        importer.run()

        assert Role.objects.count() == 1
        role = Role.objects.first()
        assert role.external_id == 37
        assert role.name == "Mitglied"
        assert role.sync_uuid is not None
        assert role.synced_at is None
        assert role.is_synced is False

        assert importer.stats.to_dict() == {
            "Role": {
                "created": [
                    {
                        "fields": {
                            "external_id": role.external_id,
                            "name": role.name,
                            "sync_uuid": str(role.sync_uuid),
                        },
                        "id": role.id,
                    },
                ],
                "updated": [],
                "skipped": [],
                "deleted": [],
            }
        }

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.importer.requests.get")
    def test_import_new_record_with_sync_uuid(self, mock_get, fxt_mock_response):
        """
        Test importing a data record from NocoDB that does not yet exist in the local database but has a Sync-UUID.
        This is the case if the local database was reset after the record was synced before.

        The object should be created with the provided Sync-UUID and marked as synced.
        """
        mock_get.return_value = fxt_mock_response(
            {
                "list": [
                    {
                        "Id": 37,
                        "CreatedAt": "2025-08-29 14:50:05+00:00",
                        "UpdatedAt": "2025-08-29 14:55:11+00:00",
                        "Bezeichnung": "Mitglied",
                        "Zugehörigkeiten": 0,
                        "Sync-UUID": "123e4567-e89b-12d3-a456-426614174000",
                    }
                ]
            },
            200,
        )

        importer = RoleImporter(Role)
        importer.run()

        assert Role.objects.count() == 1
        role = Role.objects.first()
        assert role.external_id == 37
        assert role.name == "Mitglied"
        assert role.sync_uuid == UUID("123e4567-e89b-12d3-a456-426614174000")
        assert role.synced_at == role.updated_at
        assert role.is_synced is True

        assert importer.stats.to_dict() == {
            "Role": {
                "created": [
                    {
                        "fields": {
                            "external_id": role.external_id,
                            "name": role.name,
                            "sync_uuid": str(role.sync_uuid),
                        },
                        "id": role.id,
                    },
                ],
                "updated": [],
                "skipped": [],
                "deleted": [],
            }
        }

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.importer.requests.get")
    @pytest.mark.parametrize("sync", [False, True])
    def test_importing_existing_record_without_changes(
        self, mock_get, sync, fxt_mock_response
    ):
        """
        Test importing a data record from NocoDB that already exists in the local database
        and has not changed.

        The object should remain unchanged including the `synced_at` date.
        """
        sync_uuid = UUID("123e4567-e89b-12d3-a456-426614174000")
        existing_role = Role(
            external_id=37,
            name="Mitglied",
            sync_uuid=sync_uuid,
        )
        existing_role.save(sync=sync)
        updated_at = existing_role.updated_at
        synced_at = existing_role.synced_at

        mock_get.return_value = fxt_mock_response(
            {
                "list": [
                    {
                        "Id": 37,
                        "CreatedAt": "2025-08-29 14:45:05+00:00",
                        "UpdatedAt": "2025-08-29 14:45:11+00:00",
                        "Bezeichnung": "Mitglied",
                        "Zugehörigkeiten": 0,
                        "Sync-UUID": str(sync_uuid),
                    }
                ]
            },
            200,
        )

        importer = RoleImporter(Role)
        importer.run()

        assert Role.objects.count() == 1
        role = Role.objects.first()
        assert role == existing_role
        assert role.external_id == 37
        assert role.name == "Mitglied"
        assert role.sync_uuid == sync_uuid
        assert role.updated_at == updated_at
        assert role.synced_at == synced_at
        assert role.is_synced is sync

        assert importer.stats.to_dict() == {}

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.importer.requests.get")
    @pytest.mark.parametrize("sync", [False, True])
    def test_importing_existing_record_with_changes(
        self, mock_get, sync, fxt_mock_response
    ):
        """
        Test importing a data record from NocoDB that already exists in the local database and has changed.

        The object should be updated with the new data and marked as synced.
        """
        sync_uuid = UUID("123e4567-e89b-12d3-a456-426614174000")
        existing_role = Role(
            external_id=37,
            name="Mitglied",
            sync_uuid=sync_uuid,
        )
        existing_role.save(sync=sync)
        updated_at = existing_role.updated_at

        mock_get.return_value = fxt_mock_response(
            {
                "list": [
                    {
                        "Id": 37,
                        "CreatedAt": "2025-08-29 14:50:05+00:00",
                        "UpdatedAt": "2025-08-29 14:55:11+00:00",
                        "Bezeichnung": "Neuer Name",
                        "Zugehörigkeiten": 0,
                        "Sync-UUID": str(sync_uuid),
                    }
                ]
            },
            200,
        )

        importer = RoleImporter(Role)
        importer.run()

        assert Role.objects.count() == 1
        role = Role.objects.first()
        assert role == existing_role
        assert role.external_id == 37
        assert role.name == "Neuer Name"
        assert role.sync_uuid == sync_uuid
        assert role.updated_at > updated_at
        assert role.synced_at == role.updated_at
        assert role.is_synced is True

        assert importer.stats.to_dict() == {
            "Role": {
                "created": [],
                "updated": [
                    {
                        "diff": {
                            "name": {
                                "new": "Neuer Name",
                                "old": "Mitglied",
                            },
                        },
                        "id": role.id,
                    },
                ],
                "skipped": [],
                "deleted": [],
            }
        }

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.importer.requests.get")
    @pytest.mark.parametrize("sync", [False, True])
    def test_import_existing_without_sync_uuid(self, mock_get, sync, fxt_mock_response):
        """
        Test importing a data record from NocoDB that does not have a Sync-UUID, but an object with the same `external_id` already exists in the local database.

        In this case, the Sync-UUID of the existing object should not be overwritten,
        and the object should remain unsynced.
        """
        sync_uuid = UUID("123e4567-e89b-12d3-a456-426614174000")
        existing_role = Role.objects.create(
            external_id=37,
            name="Mitglied",
            sync_uuid=sync_uuid,
        )
        existing_role.save(sync=sync)
        updated_at = existing_role.updated_at
        synced_at = existing_role.synced_at

        mock_get.return_value = fxt_mock_response(
            {
                "list": [
                    {
                        "Id": 37,
                        "CreatedAt": "2025-08-29 14:45:05+00:00",
                        "UpdatedAt": "2025-08-29 14:45:11+00:00",
                        "Bezeichnung": "Mitglied",
                        "Zugehörigkeiten": 0,
                        "Sync-UUID": None,
                    }
                ]
            },
            200,
        )

        assert existing_role.sync_uuid is not None

        importer = RoleImporter(Role)
        importer.run()

        assert Role.objects.count() == 1
        role = Role.objects.first()
        assert role == existing_role
        assert role.external_id == 37
        assert role.name == "Mitglied"
        assert role.sync_uuid == sync_uuid
        assert role.updated_at == updated_at
        assert role.synced_at == synced_at
        assert role.is_synced is sync

        assert importer.stats.to_dict() == {
            "Role": {
                "created": [],
                "updated": [],
                "skipped": [
                    f"Role with ID {role.id} has no sync UUID in import data, skipping update"
                ],
                "deleted": [],
            }
        }

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.importer.requests.get")
    @pytest.mark.parametrize("sync", [False, True])
    def test_import_existing_with_conflicting_sync_uuid(
        self, mock_get, sync, fxt_mock_response
    ):
        """
        Test importing a data record from NocoDB that has a Sync-UUID, but an object with the same `external_id` already
        exists in the local database with a different Sync-UUID.

        In this case, an exception should be raised to avoid data inconsistency.
        """
        sync_uuid = UUID("123e4567-e89b-12d3-a456-426614174000")
        existing_role = Role.objects.create(
            external_id=37,
            name="Mitglied",
            sync_uuid=sync_uuid,
        )
        existing_role.save(sync=sync)
        updated_at = existing_role.updated_at
        synced_at = existing_role.synced_at

        mock_get.return_value = fxt_mock_response(
            {
                "list": [
                    {
                        "Id": 37,
                        "CreatedAt": "2025-08-29 14:45:05+00:00",
                        "UpdatedAt": "2025-08-29 14:45:11+00:00",
                        "Bezeichnung": "Mitglied",
                        "Zugehörigkeiten": 0,
                        "Sync-UUID": "987e6543-e21b-12d3-a456-426614174999",
                    }
                ]
            },
            200,
        )

        importer = RoleImporter(Role)

        with pytest.raises(ImportError) as excinfo:
            importer.run()
        assert "Sync UUID conflict" in str(excinfo.value)

        assert Role.objects.count() == 1
        role = Role.objects.first()
        assert role == existing_role
        assert role.external_id == 37
        assert role.name == "Mitglied"
        assert role.sync_uuid == sync_uuid
        assert role.updated_at == updated_at
        assert role.synced_at == synced_at
        assert role.is_synced is sync

        assert importer.stats.to_dict() == {}
