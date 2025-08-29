import uuid
from unittest import mock

import pytest

from froide_evidencecollection.exporter import NocoDBExporter, TableExporter
from froide_evidencecollection.models import (
    Affiliation,
    InstitutionalLevel,
    Organization,
    OrganizationStatus,
    Person,
    PersonStatus,
    Role,
)

from .factories import (
    AffiliationFactory,
    GeoRegionFactory,
    OrganizationFactory,
    PersonFactory,
    RoleFactory,
    syncable_model_factories,
)


class TestInstanceToPayload:
    @pytest.mark.django_db
    @pytest.mark.parametrize("include_id", [False, True])
    @pytest.mark.parametrize(
        "field_data",
        [
            {},
            {
                "sync_uuid": "123e4567-e89b-12d3-a456-426614174000",
                "external_id": 38,
            },
        ],
    )
    def test_instance_to_payload_for_role_model(self, field_data, include_id):
        exporter = TableExporter(Role)

        field_data["name"] = "Name"

        role = Role.objects.create(**field_data)

        payload = exporter.instance_to_payload(role, include_id=include_id)

        expected_payload = {
            "Bezeichnung": field_data.get("name", ""),
            "Sync-UUID": field_data.get("sync_uuid") or str(role.sync_uuid),
        }

        if include_id:
            expected_payload["Id"] = field_data.get("external_id")

        assert payload == expected_payload

    @pytest.mark.django_db
    @pytest.mark.parametrize("include_id", [False, True])
    @pytest.mark.parametrize("status", [None, "Aktiv"])
    @pytest.mark.parametrize(
        "field_data",
        [
            {},
            {
                "title": "Dr.",
                "also_known_as": ["Spitzname"],
                "wikidata_id": "Q123",
                "aw_id": 123,
                "sync_uuid": "123e4567-e89b-12d3-a456-426614174000",
                "external_id": 38,
            },
        ],
    )
    def test_instance_to_payload_for_person_model(self, field_data, status, include_id):
        exporter = TableExporter(Person)

        field_data["first_name"] = "Vorname"
        field_data["last_name"] = "Nachname"

        person = Person.objects.create(**field_data)

        if status:
            person.status = PersonStatus.objects.create(name=status)
            person.save()

        payload = exporter.instance_to_payload(person, include_id=include_id)

        expected_payload = {
            "Vorname(n)": field_data.get("first_name", ""),
            "Nachname": field_data.get("last_name", ""),
            "Titel": field_data.get("title"),
            "Spitzname": ",".join(field_data.get("also_known_as", [])) or None,
            "Wikidata-ID": field_data.get("wikidata_id"),
            "abgeordnetenwatch.de Politiker-ID": field_data.get("aw_id"),
            "Status (Person)": status,
            "Sync-UUID": field_data.get("sync_uuid") or str(person.sync_uuid),
            "Typ": "Person",
        }

        if include_id:
            expected_payload["Id"] = field_data.get("external_id")

        assert payload == expected_payload

    @pytest.mark.django_db
    @pytest.mark.parametrize("include_id", [False, True])
    @pytest.mark.parametrize("status", [None, "Verboten"])
    @pytest.mark.parametrize("special_regions", [[], ["Ausland"]])
    @pytest.mark.parametrize("regions", [[], ["Berlin", "Brandenburg"]])
    @pytest.mark.parametrize(
        "field_data",
        [
            {},
            {
                "also_known_as": ["Abkürzung"],
                "wikidata_id": "Q123",
                "sync_uuid": "123e4567-e89b-12d3-a456-426614174000",
                "external_id": 38,
            },
        ],
    )
    def test_instance_to_payload_for_organization_model(
        self, field_data, regions, special_regions, status, include_id
    ):
        exporter = TableExporter(Organization)

        institutional_level = InstitutionalLevel.objects.create(name="Bundesebene")
        field_data["institutional_level"] = institutional_level
        field_data["organization_name"] = "Name"

        organization = Organization.objects.create(**field_data)

        if regions:
            for region in regions:
                geo_region = GeoRegionFactory(name=region)
                organization.regions.add(geo_region)

        if special_regions:
            for region in special_regions:
                organization.special_regions.append(region)

        if status:
            organization.status = OrganizationStatus.objects.create(name=status)
            organization.save()

        payload = exporter.instance_to_payload(organization, include_id=include_id)

        expected_payload = {
            "Organisationsname": field_data.get("organization_name", ""),
            "Institutionsebene": institutional_level.name,
            "Abkürzung": ",".join(field_data.get("also_known_as", [])) or None,
            "Wikidata-ID": field_data.get("wikidata_id"),
            "Status (Organisation)": status,
            "Region(en)": ",".join(regions + special_regions) or None,
            "Sync-UUID": field_data.get("sync_uuid") or str(organization.sync_uuid),
            "Typ": "Organisation",
        }

        if include_id:
            expected_payload["Id"] = field_data.get("external_id")

        assert payload == expected_payload

    @pytest.mark.django_db
    @pytest.mark.parametrize("include_id", [False, True])
    @pytest.mark.parametrize(
        "field_data",
        [
            {},
            {
                "start_date": "2023-01-01",
                "end_date": "2023-12-31",
                "start_date_string": "2023-01-01",
                "end_date_string": "2023-12-31",
                "reference_url": "https://example.com",
                "comment": "Kommentar",
                "aw_id": 123,
                "sync_uuid": "123e4567-e89b-12d3-a456-426614174000",
                "external_id": 38,
            },
        ],
    )
    def test_instance_to_payload_for_affiliation_model(self, field_data, include_id):
        exporter = TableExporter(Affiliation)

        field_data["person"] = PersonFactory()
        field_data["organization"] = OrganizationFactory()
        field_data["role"] = RoleFactory()

        affiliation = Affiliation.objects.create(**field_data)

        payload = exporter.instance_to_payload(affiliation, include_id=include_id)

        expected_payload = {
            "Begonnen am": field_data.get("start_date_string"),
            "Ausgeübt bis": field_data.get("end_date_string"),
            "Referenz-URL": field_data.get("reference_url"),
            "Kommentar/Notiz": field_data.get("comment"),
            "abgeordnetenwatch.de-ID": field_data.get("aw_id"),
            "Sync-UUID": field_data.get("sync_uuid") or str(affiliation.sync_uuid),
        }

        if include_id:
            expected_payload["Id"] = field_data.get("external_id")

        assert payload == expected_payload


class TestGetRelations:
    @pytest.mark.django_db
    @pytest.mark.parametrize(
        "factory", [PersonFactory, OrganizationFactory, RoleFactory]
    )
    def test_get_relations(self, factory):
        instance = factory()
        exporter = TableExporter(instance.__class__)
        relations = exporter.get_changed_relations(instance)

        assert relations == {}

    @pytest.mark.django_db
    def test_get_relations_for_affiliation(self):
        affiliation = AffiliationFactory()

        affiliation.person.external_id = 1
        affiliation.person.save()
        affiliation.organization.external_id = 2
        affiliation.organization.save()
        affiliation.role.external_id = 3
        affiliation.role.save()

        exporter = TableExporter(Affiliation)
        relations = exporter.get_changed_relations(affiliation)

        expected_relations = {
            "123": 1,
            "456": 2,
            "789": 3,
        }

        assert relations == expected_relations

    @pytest.mark.django_db
    def test_get_relations_when_external_id_is_none(self):
        affiliation = AffiliationFactory()

        exporter = TableExporter(Affiliation)

        # Exception is thrown because related objects do not have an external_id.
        with pytest.raises(ValueError, match="has no external ID"):
            exporter.get_changed_relations(affiliation)


class TestTableExporter:
    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.exporter.requests.post")
    def test_creating_new_record_in_nocodb(self, mock_post, fxt_mock_response):
        """
        Test exporting a record to NocoDB that has no external_id yet.

        The record should be created in NocoDB and the external_id should be updated.
        """
        role = Role.objects.create(name="Vorstand", sync_uuid=uuid.uuid4())

        mock_post.return_value = fxt_mock_response([{"Id": 42}], 200)

        exporter = TableExporter(Role)
        exporter.run()

        # Check if the POST request was made correctly.
        mock_post.assert_called_once()

        # Verify the role was updated.
        role.refresh_from_db()
        assert role.external_id == 42
        assert role.synced_at is not None
        assert role.is_synced is True

        # Check export stats.
        assert exporter.stats.to_dict() == {
            "Role": {
                "created": [
                    {
                        "fields": {
                            "name": "Vorstand",
                            "sync_uuid": str(role.sync_uuid),
                            "external_id": 42,
                        },
                        "id": role.id,
                    }
                ],
                "updated": [],
                "failed_links": [],
            }
        }

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.exporter.requests.patch")
    def test_updating_existing_record_in_nocodb(self, mock_patch, fxt_mock_response):
        """
        Test exporting an updated record to NocoDB that already has an external_id.

        The record should be updated in NocoDB and marked as synced.
        """
        role = Role(name="Alt", external_id=37, sync_uuid=uuid.uuid4())
        # Role has been synced before.
        role.save(sync=True)

        # Update the instance.
        role.name = "Neu"
        role.save()

        # Ensure it's marked as not synced.
        assert role.is_synced is False

        mock_patch.return_value = fxt_mock_response([{"Id": 37}], 200)

        exporter = TableExporter(Role)
        exporter.run()

        # Check if the PATCH request was made correctly.
        mock_patch.assert_called_once()

        # Verify the role was updated.
        role.refresh_from_db()
        assert role.external_id == 37
        assert role.synced_at is not None
        assert role.is_synced is True

        # Check export stats.
        assert exporter.stats.to_dict() == {
            "Role": {
                "created": [],
                "updated": [
                    {"id": role.id, "diff": {"name": {"old": "Alt", "new": "Neu"}}}
                ],
                "failed_links": [],
            }
        }

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.exporter.requests.post")
    @mock.patch("froide_evidencecollection.exporter.requests.patch")
    def test_skipping_synced_records(self, mock_patch, mock_post, fxt_mock_response):
        """
        Test that records already marked as synced are not exported.
        """
        role = Role(
            name="Mitglied",
            external_id=37,
            sync_uuid=uuid.uuid4(),
        )
        role.save(sync=True)
        synced_at = role.synced_at

        exporter = TableExporter(Role)
        exporter.run()

        # Verify no requests were made.
        mock_post.assert_not_called()
        mock_patch.assert_not_called()

        # Check that role remains unchanged.
        assert role.synced_at == synced_at

        # Check export stats - should be empty as nothing changed.
        assert exporter.stats.to_dict() == {}

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.exporter.requests.post")
    @mock.patch("froide_evidencecollection.exporter.requests.patch")
    def test_handles_both_create_and_update(
        self, mock_patch, mock_post, fxt_mock_response
    ):
        """
        Test handling multiple records where some need to be created and others updated.
        """
        # Create a role that needs to be created in NocoDB.
        new_role = Role.objects.create(name="Neu", sync_uuid=uuid.uuid4())

        # Create a role that needs to be updated in NocoDB.
        update_role = Role(name="Alt", external_id=38, sync_uuid=uuid.uuid4())
        # Role has been synced before.
        update_role.save(sync=True)

        # Update the instance.
        update_role.name = "Update"
        update_role.save()

        # Create a role that's already synced.
        synced_role = Role(
            name="Synced",
            external_id=39,
            sync_uuid=uuid.uuid4(),
        )
        synced_role.save(sync=True)
        synced_at = synced_role.synced_at

        mock_post.return_value = fxt_mock_response([{"Id": 42}], 200)
        mock_patch.return_value = fxt_mock_response([{"Id": 38}], 200)

        exporter = TableExporter(Role)
        exporter.run()

        # Verify both requests were made.
        mock_post.assert_called_once()
        mock_patch.assert_called_once()

        # Check that roles were updated correctly.
        new_role.refresh_from_db()
        update_role.refresh_from_db()

        assert new_role.external_id == 42
        assert new_role.is_synced is True
        assert update_role.is_synced is True
        assert synced_role.synced_at == synced_at

        # Check export stats.
        assert exporter.stats.to_dict() == {
            "Role": {
                "created": [
                    {
                        "fields": {
                            "external_id": 42,
                            "name": "Neu",
                            "sync_uuid": str(new_role.sync_uuid),
                        },
                        "id": new_role.id,
                    },
                ],
                "updated": [
                    {
                        "id": update_role.id,
                        "diff": {"name": {"old": "Alt", "new": "Update"}},
                    }
                ],
                "failed_links": [],
            }
        }

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.exporter.requests.post")
    def test_create_records_with_length_mismatch(self, mock_post, fxt_mock_response):
        """
        Test that a ValueError is raised when the API returns a different number of records
        than were submitted for creation.
        """
        role1 = RoleFactory()
        role2 = RoleFactory()

        # Mock API response with only one record ID instead of two.
        mock_post.return_value = fxt_mock_response([{"Id": 42}])

        exporter = TableExporter(Role)
        msg = "Mismatch: send 2 instance\\(s\\), retrieved 1 record ID\\(s\\)"
        with pytest.raises(
            ValueError,
            match=msg,
        ):
            exporter.run()

        # Verify the roles weren't updated.
        role1.refresh_from_db()
        role2.refresh_from_db()

        assert role1.external_id is None
        assert role2.external_id is None
        assert role1.is_synced is False
        assert role2.is_synced is False

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.exporter.requests.post")
    def test_creation_with_links(self, mock_post, fxt_mock_response):
        """
        Test exporting an Affiliation that needs to be created in NocoDB along with its links.
        """
        person = PersonFactory(external_id=101)
        organization = OrganizationFactory(external_id=202)
        role = RoleFactory(external_id=303)

        affiliation = AffiliationFactory(
            person=person,
            organization=organization,
            role=role,
        )

        assert affiliation.external_id is None
        assert affiliation.is_synced is False

        api_response = fxt_mock_response([{"Id": 42}])
        link_response = fxt_mock_response("true")
        mock_post.side_effect = [api_response] + [link_response] * 3

        exporter = TableExporter(Affiliation)
        exporter.run()

        assert mock_post.call_count == 4  # 1 for creation + 3 for links

        affiliation.refresh_from_db()
        assert affiliation.external_id == 42
        assert affiliation.is_synced is True

        stats = exporter.stats.to_dict()
        assert "Affiliation" in stats
        assert stats["Affiliation"] == {
            "created": [
                {
                    "fields": {
                        "person": person.id,
                        "organization": organization.id,
                        "role": role.id,
                        "start_date_string": affiliation.start_date_string,
                        "start_date": None,
                        "end_date_string": affiliation.end_date_string,
                        "end_date": None,
                        "reference_url": affiliation.reference_url,
                        "comment": affiliation.comment,
                        "aw_id": affiliation.aw_id,
                        "sync_uuid": str(affiliation.sync_uuid),
                        "external_id": 42,
                    },
                    "id": affiliation.id,
                }
            ],
            "updated": [],
            "failed_links": [],
        }

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.exporter.requests.post")
    def test_creation_with_links_failure(self, mock_post, fxt_mock_response):
        """
        Test exporting an Affiliation that needs to be created in NocoDB along with its links,
        where one of the link updates fails.
        """
        person = PersonFactory(external_id=101)
        organization = OrganizationFactory(external_id=202)
        role = RoleFactory(external_id=303)

        affiliation = AffiliationFactory(
            person=person,
            organization=organization,
            role=role,
        )

        assert affiliation.external_id is None
        assert affiliation.is_synced is False

        # Successful creation and first link update.
        api_response = fxt_mock_response([{"Id": 42}])
        link_response = fxt_mock_response("true")

        # Second link update fails.
        link_response_error = mock.Mock()
        link_response_error.status_code = 400
        link_response_error.raise_for_status = mock.Mock(
            side_effect=Exception("Link update failed")
        )

        mock_post.side_effect = [api_response, link_response, link_response_error]

        exporter = TableExporter(Affiliation)
        exporter.run()

        assert mock_post.call_count == 3  # 1 for creation + 2 for links

        affiliation.refresh_from_db()
        assert affiliation.external_id == 42
        assert affiliation.is_synced is False

        stats = exporter.stats.to_dict()
        assert "Affiliation" in stats
        assert stats["Affiliation"] == {
            "created": [
                {
                    "fields": {
                        "person": person.id,
                        "organization": organization.id,
                        "role": role.id,
                        "start_date_string": affiliation.start_date_string,
                        "start_date": None,
                        "end_date_string": affiliation.end_date_string,
                        "end_date": None,
                        "reference_url": affiliation.reference_url,
                        "comment": affiliation.comment,
                        "aw_id": affiliation.aw_id,
                        "sync_uuid": str(affiliation.sync_uuid),
                        "external_id": 42,
                    },
                    "id": affiliation.id,
                }
            ],
            "updated": [],
            "failed_links": [
                {
                    "field_id": "456",
                    "record_id": affiliation.external_id,
                    "rel_instance_id": organization.external_id,
                }
            ],
        }

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.exporter.requests.post")
    def test_creation_of_multiple_affiliations(self, mock_post, fxt_mock_response):
        """
        Test exporting multiple Affiliation records that need to be created in NocoDB along with their links.
        """
        person = PersonFactory(external_id=101)
        organization = OrganizationFactory(external_id=202)
        role = RoleFactory(external_id=303)

        aff1 = AffiliationFactory(
            person=person,
            organization=organization,
            role=role,
        )
        aff2 = AffiliationFactory(
            person=person,
            organization=organization,
            role=role,
        )

        assert aff1.external_id is None
        assert aff1.is_synced is False
        assert aff2.external_id is None
        assert aff2.is_synced is False

        api_response = fxt_mock_response([{"Id": 42}, {"Id": 43}])
        link_response = fxt_mock_response("true")
        mock_post.side_effect = [api_response] + [link_response] * 6

        exporter = TableExporter(Affiliation)
        exporter.run()

        assert mock_post.call_count == 7  # 1 for creation + 6 for links

        aff1.refresh_from_db()
        aff2.refresh_from_db()
        assert aff1.external_id == 42
        assert aff2.external_id == 43
        assert aff1.is_synced is True
        assert aff2.is_synced is True

        stats = exporter.stats.to_dict()
        assert "Affiliation" in stats
        assert len(stats["Affiliation"]["created"]) == 2
        assert len(stats["Affiliation"]["updated"]) == 0
        assert len(stats["Affiliation"]["failed_links"]) == 0

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.exporter.requests.post")
    def test_creation_of_multiple_affiliations_with_link_failure(
        self, mock_post, fxt_mock_response
    ):
        """
        Test exporting multiple Affiliation records that need to be created in NocoDB along with their links,
        where one of the link updates fails.
        """
        person = PersonFactory(external_id=101)
        organization = OrganizationFactory(external_id=202)
        role = RoleFactory(external_id=303)

        aff1 = AffiliationFactory(
            person=person,
            organization=organization,
            role=role,
        )
        aff2 = AffiliationFactory(
            person=person,
            organization=organization,
            role=role,
        )

        api_response = fxt_mock_response([{"Id": 42}, {"Id": 43}])
        link_success = fxt_mock_response("true")

        link_error = mock.Mock()
        link_error.status_code = 400
        link_error.raise_for_status = mock.Mock(
            side_effect=Exception("Link update failed")
        )

        mock_post.side_effect = [
            # API creation response
            api_response,
            # Links for aff1
            link_error,
            # Links for aff2
            link_success,
            link_success,
            link_success,
        ]

        exporter = TableExporter(Affiliation)
        exporter.run()

        assert mock_post.call_count == 5  # 1 for creation + 4 for links

        aff1.refresh_from_db()
        aff2.refresh_from_db()

        assert aff1.external_id == 42
        assert aff2.external_id == 43

        # aff1 should not be marked as synced due to link failure.
        assert aff1.is_synced is False
        # aff2 should be marked as synced.
        assert aff2.is_synced is True

        stats = exporter.stats.to_dict()
        assert "Affiliation" in stats
        assert len(stats["Affiliation"]["created"]) == 2
        assert len(stats["Affiliation"]["updated"]) == 0
        assert stats["Affiliation"]["failed_links"] == [
            {
                "field_id": "123",
                "record_id": aff1.external_id,
                "rel_instance_id": person.external_id,
            }
        ]

    @pytest.mark.django_db
    @pytest.mark.parametrize("update_links", [False, True])
    @mock.patch("froide_evidencecollection.exporter.requests.post")
    @mock.patch("froide_evidencecollection.exporter.requests.patch")
    def test_update_with_links(
        self, mock_patch, mock_post, update_links, fxt_mock_response
    ):
        """
        Test updating an Affiliation that already exists in NocoDB, with optional link updates.
        """
        person = PersonFactory(external_id=101)
        organization = OrganizationFactory(external_id=202)
        role = RoleFactory(external_id=303)

        affiliation = AffiliationFactory(
            person=person,
            organization=organization,
            role=role,
            external_id=42,
            comment="Old comment",
        )
        affiliation.save(sync=True)

        # Update to make it need syncing.
        affiliation.comment = "New comment"
        if update_links:
            # Change related object to force link update.
            affiliation.role = RoleFactory(external_id=404)
        affiliation.save()

        assert affiliation.is_synced is False

        mock_patch.return_value = fxt_mock_response([{"Id": 42}])
        mock_post.return_value = fxt_mock_response("true")

        exporter = TableExporter(Affiliation)
        exporter.run()

        mock_patch.assert_called_once()
        if update_links:
            mock_post.assert_called_once()
        else:
            mock_post.assert_not_called()

        affiliation.refresh_from_db()
        assert affiliation.is_synced is True

        diff = {"comment": {"old": "Old comment", "new": "New comment"}}
        if update_links:
            diff["role"] = {
                "old": role.id,
                "new": affiliation.role.id,
            }

        stats = exporter.stats.to_dict()
        assert "Affiliation" in stats
        assert stats["Affiliation"] == {
            "created": [],
            "updated": [
                {
                    "id": affiliation.id,
                    "diff": diff,
                }
            ],
            "failed_links": [],
        }

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.exporter.requests.post")
    @mock.patch("froide_evidencecollection.exporter.requests.patch")
    def test_update_with_links_failuŕe(self, mock_patch, mock_post, fxt_mock_response):
        """
        Test updating an Affiliation that already exists in NocoDB, where link updates fail.
        """
        person = PersonFactory(external_id=101)
        organization = OrganizationFactory(external_id=202)
        role = RoleFactory(external_id=303)

        affiliation = AffiliationFactory(
            person=person,
            organization=organization,
            role=role,
            external_id=42,
            comment="Old comment",
        )
        affiliation.save(sync=True)

        # Update to make it need syncing.
        affiliation.comment = "New comment"
        # Change related object to force link update.
        affiliation.role = RoleFactory(external_id=404)
        affiliation.save()

        assert affiliation.is_synced is False

        def side_effect(instance):
            raise Exception("Link update failed")

        mock_post.side_effect = side_effect
        mock_patch.return_value = fxt_mock_response([{"Id": 42}])

        exporter = TableExporter(Affiliation)
        exporter.run()

        mock_patch.assert_called_once()
        mock_post.assert_called_once()

        affiliation.refresh_from_db()
        # Should not be marked as synced due to link update failure.
        assert affiliation.is_synced is False

        stats = exporter.stats.to_dict()
        assert "Affiliation" in stats
        assert stats["Affiliation"] == {
            "created": [],
            "updated": [
                {
                    "id": affiliation.id,
                    "diff": {
                        "comment": {"old": "Old comment", "new": "New comment"},
                        "role": {
                            "old": role.id,
                            "new": affiliation.role.id,
                        },
                    },
                }
            ],
            "failed_links": [
                {
                    "field_id": "789",
                    "record_id": affiliation.external_id,
                    "rel_instance_id": affiliation.role.external_id,
                }
            ],
        }


class TestNocoDBExporter:
    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.exporter.TableExporter.run")
    def test_exporter_runs_all_table_exporters(self, mock_run):
        """
        Test that the NocoDBExporter runs all configured TableExporters.
        """
        exporter = NocoDBExporter()
        exporter.run()

        assert mock_run.call_count == 4

    @pytest.mark.django_db
    @pytest.mark.parametrize("factory", syncable_model_factories)
    @mock.patch("froide_evidencecollection.exporter.requests.post")
    @mock.patch("froide_evidencecollection.exporter.requests.patch")
    def test_exporter_integration(
        self, mock_patch, mock_post, factory, fxt_mock_response
    ):
        """
        Test the full export process with real models but mocked HTTP requests.
        """
        # Create test data.
        new_instance = factory(sync_uuid=uuid.uuid4())
        update_instance = factory(external_id=38, sync_uuid=uuid.uuid4())

        if new_instance.__class__ == Affiliation:
            i = 1
            # Prevent export calls for foreign key instances.
            for instance in [new_instance, update_instance]:
                instance.person.external_id = i
                instance.person.save(sync=True)
                instance.organization.external_id = i
                instance.organization.save(sync=True)
                instance.role.external_id = i
                instance.role.save(sync=True)
                # For one instance, remove the role to test handling of null relations.
                if i == 2:
                    instance.role = None
                    instance.save()
                i += 1

        new_instance.save(sync=True)

        mock_post.return_value = fxt_mock_response([{"Id": 42}], 200)
        mock_patch.return_value = fxt_mock_response([{"Id": 38}], 200)

        exporter = NocoDBExporter()
        exporter.run()
        stats = exporter.log_stats()

        post_count = 1
        # Affiliations need additional POST calls for each relation field
        # to create links.
        if new_instance.__class__ == Affiliation:
            post_count = 3

        # Verify all requests were made.
        assert mock_post.call_count == post_count
        assert mock_patch.call_count == 1

        # Check that roles were updated correctly.
        new_instance.refresh_from_db()
        update_instance.refresh_from_db()

        assert new_instance.external_id == 42
        assert new_instance.is_synced is True
        assert update_instance.is_synced is True

        # Check export stats.
        model_name = new_instance.__class__.__name__
        assert model_name in stats
        assert len(stats[model_name]["created"]) == 1
        assert len(stats[model_name]["updated"]) == 1
