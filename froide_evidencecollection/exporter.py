import logging
import uuid

from django.conf import settings
from django.db import transaction
from django.utils import timezone

import requests

from froide_evidencecollection.models import (
    ImportableModel,
    Role,
    SyncableModel,
)
from froide_evidencecollection.utils import (
    ExportStatsCollection,
    get_base_class_name,
    is_serializable,
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

    def run(self):
        to_create = self.model.objects.filter(external_id__isnull=True)
        if to_create.exists():
            self.create_records(to_create)

        to_update = self.model.objects.filter(is_synced=False)
        if to_update.exists():
            self.update_records(to_update)

        self.stats.log_summary(self.model)

    def create_records(self, instances):
        url = f"{API_URL}/tables/{self.table_name}/records"
        headers = {"xc-token": API_TOKEN}

        payload = [self.instance_to_payload(instance) for instance in instances]
        logger.info(
            f"Creating {instances.count()} record(s) in NocoDB for {self.model_name}"
        )
        logger.debug(f"Payload: {payload}")

        response = requests.post(url, json=payload, headers=headers)
        response.raise_for_status()

        data = response.json()

        if len(data) != len(instances):
            raise ValueError(
                f"Mismatch: send {len(instances)} instance(s), retrieved {len(data)} record ID(s)"
            )

        with transaction.atomic():
            for obj, resp in zip(instances, data, strict=False):
                obj.external_id = str(resp["Id"])
                obj.save(sync=True, update_fields=["external_id"])
                self.stats.track_created(self.model, obj)

    def update_records(self, instances):
        url = f"{API_URL}/tables/{self.table_name}/records"
        headers = {"xc-token": API_TOKEN}

        payload = [
            self.instance_to_payload(instance, include_id=True)
            for instance in instances
        ]
        logger.info(
            f"Updating {instances.count()} record(s) in NocoDB for {self.model_name}"
        )
        logger.debug(f"Payload: {payload}")

        response = requests.patch(url, json=payload, headers=headers)
        response.raise_for_status()

        with transaction.atomic():
            instance_ids = list(instances.values_list("pk", flat=True))
            instances.update(synced_at=timezone.now())
            self.stats.track_updated(self.model, instance_ids)

    def instance_to_payload(self, instance, include_id=False):
        payload = {}

        for model_field, source_field in self.field_map.items():
            if model_field == self.id_field and not include_id:
                continue

            if model_field in self.relation_config:
                continue

            value = getattr(instance, model_field)
            if value is None:
                continue

            if isinstance(value, uuid.UUID):
                payload[source_field] = str(value)

            field_obj = instance._meta.get_field(model_field)
            if not is_serializable(field_obj):
                continue

            payload[source_field] = value

        return payload


class NocoDBExporter:
    def __init__(self):
        self.stats = ExportStatsCollection()
        self.table_exporters = [
            # TableExporter(Person),
            TableExporter(Role),
        ]

    @transaction.atomic
    def run(self):
        for exporter in self.table_exporters:
            exporter.run()
            self.stats.merge(exporter.stats)

    def log_stats(self):
        return self.stats.to_dict()
