from froide.settings import Test as FroideTest


class Test(FroideTest):
    INSTALLED_APPS = FroideTest.INSTALLED_APPS.default + [
        "froide_evidencecollection",
    ]

    DATABASES = {
        "default": {
            "ENGINE": "django.contrib.gis.db.backends.postgis",
            "NAME": "froide_evidencecollection",
            "USER": "froide_evidencecollection",
            "PASSWORD": "froide_evidencecollection",
            "HOST": "localhost",
            "PORT": "5432",
        }
    }

    FROIDE_EVIDENCECOLLECTION_NOCODB_IMPORT_CONFIG = {
        "api_url": "FAKE_API_URL",
        "api_token": "FAKE_API_TOKEN",
        "tables": {
            "AbstractActor": "FAKE_TABLE_ACTOR",
            "Affiliation": "FAKE_TABLE_AFFILIATION",
            "Evidence": "FAKE_TABLE_EVIDENCE",
            "Role": "FAKE_TABLE_ROLE",
        },
        "views": {
            "AbstractActor_Person": "FAKE_VIEW_ACTOR_PERSON",
            "AbstractActor_Organization": "FAKE_VIEW_ACTOR_ORGANIZATION",
        },
        "field_map": {
            "Person": {
                "external_id": "Id",
                "sync_uuid": "Sync-UUID",
                "wikidata_id": "Wikidata-ID",
                "aw_id": "abgeordnetenwatch.de Politiker-ID",
                "first_name": "Vorname(n)",
                "last_name": "Nachname",
                "title": "Titel",
                "also_known_as": "Spitzname",
                "status": "Status (Person)",
            },
            "Organization": {
                "external_id": "Id",
                "sync_uuid": "Sync-UUID",
                "wikidata_id": "Wikidata-ID",
                "organization_name": "Organisationsname",
                "institutional_level": "Institutionsebene",
                "regions": "Region(en)",
                "also_known_as": "Abkürzung",
                "status": "Status (Organisation)",
            },
            "Role": {
                "external_id": "Id",
                "sync_uuid": "Sync-UUID",
                "name": "Bezeichnung",
            },
            "Affiliation": {
                "external_id": "Id",
                "sync_uuid": "Sync-UUID",
                "aw_id": "abgeordnetenwatch.de-ID",
                "person": "Personen und Organisationen_id",
                "organization": "Personen und Organisationen_id1",
                "role": "Funktion",
                "start_date_string": "Begonnen am",
                "end_date_string": "Ausgeübt bis",
                "reference_url": "Referenz-URL",
                "comment": "Kommentar/Notiz",
            },
            "Evidence": {
                "external_id": "Id",
                "citation": "Zitat/Beschreibung",
                "description": "Zusammenfassung",
                "evidence_type": "Art des Belegs",
                "collections": "Sammlung(en)",
                "originators": "_nc_m2m_Quellen und Bel_Personen und Ors",
                "related_actors": "_nc_m2m_Quellen und Bel_Personen und Or1s",
                "event_date": "Datum der Originaläußerung",
                "publishing_date": "Datum der Veröffentlichung",
                "documentation_date": "Datum der Dokumentation",
                "reference_url": "Fundstelle (URL)",
                "reference_info": "Fundstelle (zusätzliche Informationen)",
                "primary_source_url": "Primärquelle (URL)",
                "primary_source_info": "Primärquelle (zusätzliche Informationen)",
                "attribution_justification": "Zurechnungs - Begründung",
                "attribution_evidence": "_nc_m2m_Quellen und Bel_Quellen und Bels",
                "attribution_problems": "Zurechnungsprobleme",
                "comment": "Kommentar/Notiz",
                "legal_assessment": "Juristische Bewertung",
            },
        },
        "relations": {
            "Person": {
                "status": {
                    "type": "fk",
                    "model": "froide_evidencecollection.PersonStatus",
                    "lookup_field": "name",
                    "create_if_missing": True,
                },
            },
            "Organization": {
                "institutional_level": {
                    "type": "fk",
                    "model": "froide_evidencecollection.InstitutionalLevel",
                    "lookup_field": "name",
                    "create_if_missing": True,
                },
                "regions": {
                    "type": "m2m",
                    "model": "georegion.GeoRegion",
                    "lookup_field": "id",
                },
                "status": {
                    "type": "fk",
                    "model": "froide_evidencecollection.OrganizationStatus",
                    "lookup_field": "name",
                    "create_if_missing": True,
                },
            },
            "Role": {},
            "Affiliation": {
                "person": {
                    "type": "fk",
                    "model": "froide_evidencecollection.Person",
                    "lookup_field": "external_id",
                    "field_id": "123",
                },
                "organization": {
                    "type": "fk",
                    "model": "froide_evidencecollection.Organization",
                    "lookup_field": "external_id",
                    "field_id": "456",
                },
                "role": {
                    "type": "fk",
                    "model": "froide_evidencecollection.Role",
                    "lookup_field": "external_id",
                    "field_id": "789",
                },
            },
            "Evidence": {
                "evidence_type": {
                    "type": "fk",
                    "model": "froide_evidencecollection.EvidenceType",
                    "lookup_field": "name",
                    "create_if_missing": True,
                },
                "collections": {
                    "type": "m2m",
                    "model": "froide_evidencecollection.Collection",
                    "lookup_field": "name",
                    "create_if_missing": True,
                },
                "originators": {
                    "type": "m2m",
                    "model": "froide_evidencecollection.Actor",
                    "lookup_field": "external_id",
                },
                "related_actors": {
                    "type": "m2m",
                    "model": "froide_evidencecollection.Actor",
                    "lookup_field": "external_id",
                },
                "attribution_evidence": {
                    "type": "m2m",
                    "model": "froide_evidencecollection.Evidence",
                    "lookup_field": "external_id",
                },
                "attribution_problems": {
                    "type": "m2m",
                    "model": "froide_evidencecollection.AttributionProblem",
                    "lookup_field": "name",
                    "create_if_missing": True,
                },
            },
        },
        # Do not set selectable regions for tests.
        # "selectable_regions": {
        #    "ids": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17],
        # },
        "special_regions": ["Ausland"],
        "null_label": "Keine Angabe",
    }
