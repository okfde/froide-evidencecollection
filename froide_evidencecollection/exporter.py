import logging
import uuid

from django.conf import settings
from django.utils import timezone

import requests

from froide_evidencecollection.models import (
    Affiliation,
    ImportableModel,
    Organization,
    Person,
    Role,
    SyncableModel,
)
from froide_evidencecollection.utils import (
    ExportStatsCollection,
    get_base_class_name,
)

logger = logging.getLogger(__name__)

CONFIG = settings.FROIDE_EVIDENCECOLLECTION_NOCODB_IMPORT_CONFIG
API_URL = CONFIG["api_url"]
API_TOKEN = CONFIG["api_token"]


class TableExporter:
    def __init__(self, model):
        self.debug = settings.DEBUG
        self.model = model
        self.model_name = model.__name__
        self.base_model_name = get_base_class_name(
            model, exclude=[ImportableModel, SyncableModel]
        )
        self.field_map = CONFIG["field_map"][self.model_name]
        self.relation_config = CONFIG["relations"][self.model_name]
        self.table_name = CONFIG["tables"][self.base_model_name]
        self.id_field = "external_id"
        self.stats = ExportStatsCollection()
        self.base_url = f"{API_URL}/tables/{self.table_name}"
        self.headers = {"xc-token": API_TOKEN}

    def run(self):
        to_create = self.model.objects.filter(external_id__isnull=True)
        created_ids = list(to_create.values_list("id", flat=True))

        if to_create.exists():
            self.create_records(to_create)

        to_update = self.model.objects.filter(is_synced=False).exclude(
            pk__in=created_ids
        )
        if to_update.exists():
            self.update_records(to_update)

        self.stats.log_summary(self.model)

    def create_records(self, instances):
        url = f"{self.base_url}/records"

        payload = [self.instance_to_payload(instance) for instance in instances]
        logger.info(
            f"Creating {instances.count()} record(s) in NocoDB for {self.model_name}"
        )
        logger.debug(f"Payload: {payload}")

        response = requests.post(url, json=payload, headers=self.headers)
        response.raise_for_status()

        data = response.json()

        if len(data) != len(instances):
            raise ValueError(
                f"Mismatch: send {len(instances)} instance(s), retrieved {len(data)} record ID(s)"
            )

        for obj, resp in zip(instances, data, strict=False):
            obj.external_id = resp["Id"]
            obj.save(update_fields=["external_id"])
            self.stats.track_created(self.model, obj)

            try:
                self.update_links(obj)
            except Exception:
                continue
            else:
                obj.save(sync=True)

    def update_records(self, instances):
        url = f"{self.base_url}/records"

        payload = [
            self.instance_to_payload(instance, include_id=True)
            for instance in instances
        ]
        logger.info(
            f"Updating {instances.count()} record(s) in NocoDB for {self.model_name}"
        )
        logger.debug(f"Payload: {payload}")

        response = requests.patch(url, json=payload, headers=self.headers)
        response.raise_for_status()

        now = timezone.now()
        for instance in instances:
            self.stats.track_updated(self.model, instance)
            try:
                self.update_links(instance)
            except Exception:
                continue
            else:
                instance.mark_synced(now)

    def update_links(self, instance):
        """
        Set links to related records in other tables.

        We assume that we only have foreign key relations that have to be handled this way.
        For many-to-many relations there should be an additional step of first deleting
        existing relations before creating new ones when updating a record with links.

        We also assume that no related instances that already have been connected will be
        set to None. This would also require deleting the existing relation.
        """
        base_url = f"{self.base_url}/links"

        relations = self.get_changed_relations(instance)

        for field_id, rel_instance_id in relations.items():
            record_id = getattr(instance, self.id_field)
            url = f"{base_url}/{field_id}/records/{record_id}"

            payload = {"Id": rel_instance_id}

            logger.debug(
                f"Setting link for field {field_id} for {self.model_name} with ID {record_id} to {rel_instance_id}"
            )

            try:
                response = requests.post(url, json=payload, headers=self.headers)
                response.raise_for_status()
            except Exception as e:
                msg = f"Error setting link for field {field_id} for {self.model_name} with ID {record_id} to {rel_instance_id}: {e}"
                logger.error(msg)
                self.stats.track_failed_link(
                    self.model, field_id, record_id, rel_instance_id
                )
                raise

    def instance_to_payload(self, instance, include_id=False):
        payload = {}

        for model_field, source_field in self.field_map.items():
            if model_field == self.id_field and not include_id:
                continue

            value = getattr(instance, model_field)

            if value is None:
                payload[source_field] = value
                continue

            if isinstance(value, uuid.UUID):
                payload[source_field] = str(value)
            elif isinstance(value, list):
                payload[source_field] = ",".join(value)
            elif model_field in self.relation_config:
                rel_data = self.relation_config[model_field]
                if rel_data["lookup_field"] == "name" and rel_data["type"] == "fk":
                    rel_value = value.name
                    payload[source_field] = rel_value
            else:
                payload[source_field] = value

            # Make sure empty values are None.
            if source_field in payload and not payload[source_field]:
                payload[source_field] = None

        # Add additional info based on the model.
        payload.update(instance.get_additional_payload_data(self.field_map))

        return payload

    def get_changed_relations(self, instance):
        relations = {}

        for model_field in self.relation_config:
            relation_data = self.relation_config[model_field]
            field_id = relation_data.get("field_id")
            if field_id:
                rel_instance = getattr(instance, model_field)
                if rel_instance is None:
                    continue

                # Ignore unchanged relations.
                last_rel_id = instance.last_synced_state.get(model_field)
                if last_rel_id and last_rel_id == rel_instance.pk:
                    continue

                external_id = getattr(rel_instance, self.id_field)
                if external_id is None:
                    msg = f"Related {model_field} of {self.model_name} with ID {instance.pk} has no external ID"
                    raise ValueError(msg)
                relations[field_id] = external_id

        return relations


class NocoDBExporter:
    def __init__(self):
        self.stats = ExportStatsCollection()
        self.table_exporters = [
            TableExporter(Person),
            TableExporter(Organization),
            TableExporter(Role),
            TableExporter(Affiliation),
        ]

    def run(self):
        for exporter in self.table_exporters:
            exporter.run()
            self.stats.merge(exporter.stats)

    def log_stats(self):
        return self.stats.to_dict()
