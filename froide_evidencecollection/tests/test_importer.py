from unittest import mock
from uuid import UUID, uuid4

import pytest

from froide_evidencecollection.importer import (
    AffiliationImporter,
    ImportError,
    NocoDBImporter,
    OrganizationImporter,
    PersonImporter,
    RoleImporter,
)
from froide_evidencecollection.models import (
    Affiliation,
    InstitutionalLevel,
    Organization,
    OrganizationStatus,
    Person,
    PersonStatus,
    Role,
)

from .factories import GeoRegionFactory, OrganizationFactory, PersonFactory, RoleFactory


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


class TestPersonImporter:
    @pytest.mark.django_db
    @pytest.mark.parametrize(
        "with_values",
        [
            False,  # All optional values are None or empty.
            True,  # All optional values are set.
        ],
    )
    @mock.patch("froide_evidencecollection.importer.requests.get")
    def test_import(self, mock_get, with_values, fxt_mock_response):
        external_id = 42
        first_name = "Vorname"
        last_name = "Nachname"

        sync_uuid = uuid4() if with_values else None
        also_known_as = "Spitzname,Alias" if with_values else None
        title = "Dr." if with_values else None
        wikidata_id = "Q123" if with_values else None
        aw_id = 123 if with_values else None
        status = "Aktiv" if with_values else None

        field_data = {
            "Id": external_id,
            "Vorname(n)": first_name,
            "Nachname": last_name,
            "Titel": title,
            "Spitzname": also_known_as,
            "Wikidata-ID": wikidata_id,
            "abgeordnetenwatch.de Politiker-ID": aw_id,
            "Sync-UUID": str(sync_uuid) if sync_uuid else None,
            "Status (Person)": status,
        }

        mock_data = {"list": [field_data]}
        mock_get.return_value = fxt_mock_response(mock_data)

        importer = PersonImporter(Person)
        importer.run()

        assert mock_get.call_count == 1

        instances = Person.objects.all()
        assert instances.count() == 1
        instance = instances.first()

        assert instance.external_id == external_id
        assert instance.is_synced is (sync_uuid is not None)
        assert instance.first_name == "Vorname"
        assert instance.last_name == "Nachname"
        assert instance.title == (title or "")
        assert instance.wikidata_id == wikidata_id
        assert instance.aw_id == aw_id

        if also_known_as:
            assert instance.also_known_as == also_known_as.split(",")
        else:
            assert instance.also_known_as == []

        if sync_uuid:
            assert str(instance.sync_uuid) == str(sync_uuid)
        else:
            # If no sync UUID was provided, a new one should have been created automatically
            assert instance.sync_uuid is not None
            assert instance.sync_uuid != sync_uuid

        if status:
            assert instance.status.name == status
            assert PersonStatus.objects.count() == 1
        else:
            assert instance.status is None
            assert PersonStatus.objects.count() == 0

        person_status = PersonStatus.objects.get(name=status) if status else None

        stats = importer.stats.to_dict()
        assert "Person" in stats
        assert stats["Person"] == {
            "created": [
                {
                    "id": instance.id,
                    "fields": {
                        "also_known_as": also_known_as.split(",")
                        if also_known_as
                        else [],
                        "aw_id": aw_id,
                        "external_id": external_id,
                        "first_name": "Vorname",
                        "last_name": "Nachname",
                        "status": person_status.id if person_status else None,
                        "title": (title or ""),
                        "verband": None,
                        "wikidata_id": wikidata_id,
                        "sync_uuid": str(instance.sync_uuid),
                    },
                }
            ],
            "updated": [],
            "skipped": [],
            "deleted": [],
        }


class TestOrganizationImporter:
    @pytest.mark.django_db
    @pytest.mark.parametrize(
        "with_values",
        [
            False,  # All optional values are None or empty.
            True,  # All optional values are provided.
        ],
    )
    @mock.patch("froide_evidencecollection.importer.requests.get")
    def test_import(
        self,
        mock_get,
        with_values,
        fxt_mock_response,
    ):
        external_id = 42
        orga_name = "Testorganisation"
        level_name = "Bundesebene"

        sync_uuid = uuid4() if with_values else None
        also_known_as = "Abk,Alias" if with_values else None
        wikidata_id = "Q123" if with_values else None
        regions = ["Berlin", "Brandenburg"] if with_values else []
        special_regions = ["Ausland"] if with_values else []
        status = "Verboten" if with_values else None

        geo_regions = []
        if regions:
            for region in regions:
                geo_region = GeoRegionFactory(name=region)
                geo_regions.append(geo_region)

        field_data = {
            "Id": external_id,
            "Organisationsname": orga_name,
            "Institutionsebene": level_name,
            "Sync-UUID": str(sync_uuid) if sync_uuid else None,
            "Status (Organisation)": status,
            "Abkürzung": also_known_as,
            "Wikidata-ID": wikidata_id,
            "Region(en)": ",".join(regions + special_regions) or None,
        }

        mock_data = {"list": [field_data]}
        mock_get.return_value = fxt_mock_response(mock_data)

        importer = OrganizationImporter(Organization)
        importer.run()

        assert mock_get.call_count == 1

        institutional_level = InstitutionalLevel.objects.get(name=level_name)

        instances = Organization.objects.filter(external_id=external_id)
        assert instances.count() == 1
        instance = instances.first()

        assert instance.external_id == external_id
        assert instance.is_synced is (sync_uuid is not None)
        assert instance.organization_name == orga_name
        assert instance.institutional_level == institutional_level
        assert instance.wikidata_id == wikidata_id

        if also_known_as:
            assert instance.also_known_as == also_known_as.split(",")
        else:
            assert instance.also_known_as == []

        if sync_uuid:
            assert str(instance.sync_uuid) == str(sync_uuid)
        else:
            # If no sync UUID was provided, a new one should have been created automatically.
            assert instance.sync_uuid is not None
            assert instance.sync_uuid != sync_uuid

        org_status = OrganizationStatus.objects.get(name=status) if status else None

        if status:
            assert instance.status == org_status
            assert OrganizationStatus.objects.count() == 1
        else:
            assert instance.status is None
            assert OrganizationStatus.objects.count() == 0

        assert list(instance.regions.all()) == geo_regions
        assert instance.special_regions == special_regions

        stats = importer.stats.to_dict()
        assert "Organization" in stats
        assert stats["Organization"] == {
            "created": [
                {
                    "id": instance.id,
                    "fields": {
                        "also_known_as": also_known_as.split(",")
                        if also_known_as
                        else [],
                        "external_id": external_id,
                        "institutional_level": institutional_level.id,
                        "organization_name": "Testorganisation",
                        "regions": [r.id for r in geo_regions],
                        "special_regions": special_regions,
                        "status": org_status.id if org_status else None,
                        "verband": None,
                        "wikidata_id": wikidata_id,
                        "sync_uuid": str(instance.sync_uuid),
                    },
                }
            ],
            "updated": [],
            "skipped": [],
            "deleted": [],
        }


class TestAffiliationImporter:
    @pytest.mark.django_db
    @pytest.mark.parametrize(
        "with_values",
        [
            False,  # All optional values are None or empty.
            True,  # All optional values are set.
        ],
    )
    @mock.patch("froide_evidencecollection.importer.requests.get")
    def test_import(self, mock_get, with_values, fxt_mock_response):
        person = PersonFactory(external_id=1)
        person.save(sync=True)
        organization = OrganizationFactory(external_id=2)
        organization.save(sync=True)
        role = RoleFactory(external_id=3)
        role.save(sync=True)

        external_id = 42
        sync_uuid = uuid4() if with_values else None
        start_date = "2023-01-01" if with_values else None
        end_date = "2023-12-31" if with_values else None
        reference_url = "https://example.com" if with_values else None
        comment = "Test comment" if with_values else None
        aw_id = 123 if with_values else None

        field_data = {
            "Id": external_id,
            "Personen und Organisationen_id": 1,
            "Funktion": {"Id": 3},
            "Personen und Organisationen_id1": 2,
            "Sync-UUID": str(sync_uuid) if sync_uuid else None,
            "Begonnen am": start_date,
            "Ausgeübt bis": end_date,
            "Referenz-URL": reference_url,
            "Kommentar/Notiz": comment,
            "abgeordnetenwatch.de-ID": aw_id,
        }

        mock_data = {"list": [field_data]}
        mock_get.return_value = fxt_mock_response(mock_data)

        importer = AffiliationImporter(Affiliation)
        importer.run()

        assert mock_get.call_count == 1

        instances = Affiliation.objects.filter(external_id=external_id)
        assert instances.count() == 1
        instance = instances.first()

        assert instance.external_id == external_id
        assert instance.is_synced is (sync_uuid is not None)
        assert instance.person_id == person.id
        assert instance.organization_id == organization.id
        assert instance.role_id == role.id
        assert instance.start_date_string == (start_date or "")
        assert instance.end_date_string == (end_date or "")
        assert instance.start_date is None
        assert instance.end_date is None
        assert instance.reference_url == (reference_url or "")
        assert instance.comment == (comment or "")
        assert instance.aw_id == aw_id

        if sync_uuid:
            assert str(instance.sync_uuid) == str(sync_uuid)
        else:
            # If no sync UUID was provided, a new one should have been created automatically
            assert instance.sync_uuid is not None
            assert instance.sync_uuid != sync_uuid

        assert instance.person_id == person.id
        assert instance.organization_id == organization.id
        assert instance.role_id == role.id

        stats = importer.stats.to_dict()
        assert "Affiliation" in stats
        assert stats["Affiliation"] == {
            "created": [
                {
                    "id": instance.id,
                    "fields": {
                        "person": person.id,
                        "organization": organization.id,
                        "role": role.id,
                        "start_date": None,
                        "end_date": None,
                        "start_date_string": (start_date or ""),
                        "end_date_string": (end_date or ""),
                        "reference_url": (reference_url or ""),
                        "comment": comment or "",
                        "aw_id": aw_id,
                        "external_id": external_id,
                        "sync_uuid": str(instance.sync_uuid),
                    },
                }
            ],
            "updated": [],
            "skipped": [],
            "deleted": [],
        }


class TestNocoDBImporter:
    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.importer.TableImporter.run")
    def test_importer_runs_all_table_importers(self, mock_run):
        """
        Test that the NocoDBImporter runs all configured TableImporters.
        """
        importer = NocoDBImporter()
        importer.run()

        assert mock_run.call_count == 4

        assert importer.stats.to_dict() == {}
