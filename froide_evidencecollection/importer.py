import logging
from collections import defaultdict

from django.apps import apps
from django.conf import settings
from django.core.files.base import ContentFile
from django.db import transaction

import requests

from froide_evidencecollection.models import (
    Affiliation,
    Attachment,
    Evidence,
    Organization,
    Person,
)
from froide_evidencecollection.utils import (
    ImportStats,
    equals,
    get_base_class_name,
    get_default_value,
    selectable_regions,
)

logger = logging.getLogger(__name__)

CONFIG = settings.FROIDE_EVIDENCECOLLECTION_NOCODB_IMPORT_CONFIG
API_URL = CONFIG["api_url"]
API_TOKEN = CONFIG["api_token"]


class TableDataFetcher:
    def __init__(self, table_name, view_name=None):
        self.table_name = table_name
        self.view_name = view_name

    def iter_rows(self):
        offset = 0

        while True:
            data = self.fetch_from_api(offset)
            rows = data.get("list", [])
            page_info = data.get("pageInfo", {})

            for row in rows:
                yield row

            if page_info.get("isLastPage", True):
                break

            offset += page_info["pageSize"]

    def fetch_from_api(self, offset=0):
        url = f"{API_URL}/tables/{self.table_name}/records"
        headers = {"xc-token": API_TOKEN}
        params = {
            "offset": offset,
        }
        if self.view_name:
            params["viewId"] = self.view_name

        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()

        return response.json()


class ImportError(Exception):
    def __init__(self, message):
        super().__init__(message)
        self.message = message


class TableImporter:
    def __init__(self, model):
        self.debug = settings.DEBUG
        self.model = model
        self.model_name = model.__name__
        self.base_model_name = get_base_class_name(model)
        self.field_map = CONFIG["field_map"][self.model_name]
        self.relation_config = CONFIG["relations"][self.model_name]
        self.null_label = CONFIG["null_label"]
        self.id_field = "external_id"
        self.obj_data = None
        self.relation_values = None

        table_name = CONFIG["tables"][self.base_model_name]
        view_name = CONFIG["views"].get(f"{self.base_model_name}_{self.model_name}")
        self.fetcher = TableDataFetcher(table_name, view_name)
        self.stats = ImportStats()

    def run(self):
        self.obj_data = self.get_obj_data()
        self.create_or_update_main_instances()
        self.create_or_update_related_instances()

    def get_obj_data(self):
        """Retrieve data from the API and map it to the model fields."""
        self.relation_values = defaultdict(set)
        obj_data = {}

        for row in self.fetcher.iter_rows():
            self.collect_additional_data(row)

            # Map the row data to the model fields.
            mapped_row = {}
            for model_field, source_field in self.field_map.items():
                value = row.get(source_field)
                if value is None:
                    value = get_default_value(self.model, model_field)
                if isinstance(value, str):
                    value = value.strip()
                mapped_row[model_field] = value

            mapped_row = self.prepare_row(mapped_row)

            # Collect values of related fields.
            for rel_field, cfg in self.relation_config.items():
                raw_value = mapped_row.get(rel_field)
                if not raw_value:
                    continue

                if cfg["type"] == "m2m":
                    values = raw_value
                    if isinstance(raw_value, str):
                        values = raw_value.split(",")
                        mapped_row[rel_field] = values
                    values = [v for v in values if v != self.null_label]
                    self.relation_values[rel_field].update(values)
                elif cfg["type"] == "fk":
                    # Take first element if relation is modelled as m2m in NocoDB.
                    if isinstance(raw_value, list):
                        if len(raw_value) != 1:
                            msg = (
                                f"Expected single value for {rel_field} in {self.model_name}, "
                                f"got {len(raw_value)} values: {raw_value}"
                            )
                            self.handle_error(msg)
                            continue
                        raw_value = raw_value[0]
                    if raw_value != self.null_label:
                        self.relation_values[rel_field].add(raw_value)

            obj_data[mapped_row[self.id_field]] = mapped_row

        return obj_data

    def create_or_update_main_instances(self):
        """Create or update instances of the main model based on the collected data."""
        related_cache = self.build_related_cache()
        existing_objs = self.model.objects.in_bulk(field_name=self.id_field)

        self.stats.reset()

        for ext_id, fields in self.obj_data.items():
            obj = existing_objs.get(ext_id)
            obj = self.create_or_update_instance(self.model, obj, fields, related_cache)

            if obj and not self.stats.instance_failed:
                self.process_m2m_relations(obj, fields, related_cache)

        self.delete_instances(self.model, existing_objs.keys(), self.obj_data.keys())

        self.stats.print_summary(self.model_name)

    def create_or_update_related_instances(self):
        """
        Create or update instances of related models that e.g. have a foreign key to the main model.

        This method can be overridden by subclasses to handle specific relations.
        """
        pass

    def collect_additional_data(self, row):
        """
        Collect additional data from the row if needed.

        This method can be overridden by subclasses to handle specific data collection.
        """
        pass

    def prepare_row(self, row):
        """
        Perform some adjustments to the row data before processing.

        This method can be overridden by subclasses to handle specific data preparation.
        """
        return row

    def build_related_cache(self):
        """Build FK and M2M relation caches using collected values."""
        cache = {}

        for field, cfg in self.relation_config.items():
            model = apps.get_model(cfg["model"])
            values = self.relation_values.get(field, set())
            lookup_field = cfg["lookup_field"]
            create = cfg.get("create_if_missing", False)

            cache[field] = self.get_related_instances(
                model, values, lookup_field, create
            )

        return cache

    def create_or_update_instance(self, model, obj, fields, related_cache=None):
        """Create or update an instance of the given model with the provided fields."""
        self.stats.reset_instance()
        update = False

        init_fields = {k: v for k, v in fields.items() if k not in self.relation_config}

        if obj:
            for k, v in init_fields.items():
                if not equals(getattr(obj, k), v):
                    setattr(obj, k, v)
                    update = True
            if update:
                if related_cache:
                    self.process_fk_relations(obj, fields, related_cache)
                self.save_instance(obj)
        else:
            obj = model(**init_fields)
            if related_cache:
                self.process_fk_relations(obj, fields, related_cache)
            self.save_instance(obj, is_new=True)

        return obj

    def process_fk_relations(self, obj, fields, related_cache):
        """Update FK relations if they have changed."""
        for rel_field, cfg in self.relation_config.items():
            raw = fields.get(rel_field)
            if cfg["type"] == "fk":
                # Take first element if relation is modelled as m2m in NocoDB.
                if isinstance(raw, list):
                    raw = raw[0] if raw else None
                new_obj = related_cache[rel_field].get(raw)
                new_id = new_obj.pk if new_obj else None
                if getattr(obj, f"{rel_field}_id") != new_id:
                    setattr(obj, f"{rel_field}_id", new_id)

    def process_m2m_relations(self, obj, fields, related_cache):
        """Update M2M relations if they have changed."""
        for rel_field, cfg in self.relation_config.items():
            raw = fields.get(rel_field)
            if cfg["type"] == "m2m" and raw:
                new_objs = [
                    related_cache[rel_field][v]
                    for v in raw
                    if v in related_cache[rel_field]
                ]
                current_ids = {obj.pk for obj in getattr(obj, rel_field).all()}
                new_ids = {o.pk for o in new_objs}
                if new_ids != current_ids:
                    self.set_related_objects(obj, rel_field, new_objs)

    def get_related_instances(self, model, new_values, lookup_field, create=False):
        """Get existing instances of related models and create new ones if needed."""
        existing_objs = model.objects.in_bulk(new_values, field_name=lookup_field)
        missing_values = set(new_values) - set(existing_objs.keys())

        if missing_values:
            if create:
                new_objs = [model(**{lookup_field: v}) for v in missing_values]
                model.objects.bulk_create(new_objs)
                existing_objs = model.objects.in_bulk(
                    new_values, field_name=lookup_field
                )
            else:
                msg = f"Missing values for {model.__name__}: {missing_values}"
                self.handle_error(msg)

        return existing_objs

    def delete_instances(self, model, existing_ids, new_ids):
        """Delete existing instances that are not in the new data."""
        to_delete = set(existing_ids) - set(new_ids)
        if to_delete:
            delete_qs = model.objects.filter(**{f"{self.id_field}__in": to_delete})
            delete_qs.delete()
            self.stats.track("deleted", len(to_delete))

    def save_instance(self, obj, is_new=False):
        """Save the instance, track the operation, and handle errors."""
        try:
            with transaction.atomic():
                obj.save()
                self.stats.track("created" if is_new else "updated")
        except Exception as e:
            msg = f"Error saving {obj._meta.model.__name__} instance: {e}"
            self.handle_error(msg)
            return False
        return True

    def set_related_objects(self, obj, rel_field, related_objs):
        """Set related objects for a many-to-many field, track the operation, and handle errors."""
        try:
            with transaction.atomic():
                getattr(obj, rel_field).set(related_objs)
                self.stats.track("updated")
        except Exception as e:
            model_name = obj._meta.model.__name__
            msg = f"Error setting related objects for {model_name} instance: {e}"
            self.handle_error(msg)
            return False
        return True

    def handle_error(self, msg):
        """Handle errors during import."""
        if not self.debug:
            raise ImportError(msg)

        logger.warning(msg)
        self.stats.track("skipped")


class PersonImporter(TableImporter):
    def prepare_row(self, row):
        aka_str = row.get("also_known_as")
        row["also_known_as"] = aka_str.split(",") if aka_str else []

        return row


class OrganizationImporter(TableImporter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.region_map = {obj.name: obj.pk for obj in selectable_regions()}
        self.special_regions = CONFIG.get("special_regions", [])

    def prepare_row(self, row):
        region_str = row["regions"]
        region_names = region_str.split(",") if region_str else []

        special_region_names = [
            region for region in region_names if region in self.special_regions
        ]

        try:
            with transaction.atomic():
                region_ids = [
                    self.region_map[region]
                    for region in region_names
                    if region not in self.special_regions
                ]
        except KeyError as e:
            self.handle_error(
                f'Region "{e.args[0]}" not found for "{row["organization_name"]}"'
            )
            region_ids = []

        row["regions"] = region_ids
        row["special_regions"] = special_region_names

        aka_str = row.get("also_known_as")
        row["also_known_as"] = aka_str.split(",") if aka_str else []

        return row


class AffiliationImporter(TableImporter):
    def prepare_row(self, row):
        if row["role"] is not None:
            row["role"] = row["role"]["Bezeichnung"]

        return row


class EvidenceImporter(TableImporter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.attachments = []

    def collect_additional_data(self, row):
        for attachment in row.get("Screenshot(s)") or []:
            attachment["evidence_id"] = row["Id"]
            self.attachments.append(attachment)

    def prepare_row(self, row):
        row["originators"] = [
            int(data["Personen und Organisationen_id"]) for data in row["originators"]
        ]

        row["related_actors"] = [
            int(data["Personen und Organisationen_id"])
            for data in row["related_actors"]
        ]

        row["attribution_evidence"] = [
            int(data["Quellen und Belege_id"]) for data in row["attribution_evidence"]
        ]

        return row

    def create_or_update_related_instances(self):
        self.stats.reset()

        existing_objs = Attachment.objects.in_bulk(field_name="external_id")
        evidence_objs = Evidence.objects.in_bulk(field_name="external_id")

        for data in self.attachments:
            ext_id = data.get("id")
            signed_url = data.get("signedUrl")
            evidence_id = data.get("evidence_id")

            if not ext_id or not signed_url or not evidence_id:
                self.handle_error(f"Missing data in attachment: {data}")
                continue

            evidence = evidence_objs.get(evidence_id)
            if not evidence:
                msg = f"No evidence with ID {evidence_id} found for attachment {ext_id}"
                self.handle_error(msg)
                continue

            obj = existing_objs.get(ext_id)
            created = False if obj else True

            fields = {
                "external_id": ext_id,
                "evidence": evidence,
                "title": data.get("title"),
                "mimetype": data.get("mimetype"),
                "size": data.get("size"),
                "width": data.get("width"),
                "height": data.get("height"),
            }

            obj = self.create_or_update_instance(Attachment, obj, fields)

            # Only download file if attachment did not already have one.
            if obj and not obj.file and not self.stats.instance_failed:
                logger.info(f"Downloading attachment file from {signed_url} ...")

                response = requests.get(signed_url)
                if response.status_code != 200:
                    self.handle_error(f"Download failed for {signed_url}")
                    continue

                content = ContentFile(response.content)
                filename = data["title"]

                try:
                    with transaction.atomic():
                        obj.file.save(filename, content, save=True)
                        if not created:
                            self.stats.track("updated")
                except Exception as e:
                    msg = f"Error saving file for attachment {ext_id}: {e}"
                    self.handle_error(msg)
                    continue

        new_ids = [a["id"] for a in self.attachments]
        self.delete_instances(Attachment, existing_objs.keys(), new_ids)

        self.stats.print_summary("Attachment")


class NocoDBImporter:
    def __init__(self):
        self.table_importers = [
            PersonImporter(Person),
            OrganizationImporter(Organization),
            AffiliationImporter(Affiliation),
            EvidenceImporter(Evidence),
        ]

    @transaction.atomic
    def run(self):
        for importer in self.table_importers:
            importer.run()
