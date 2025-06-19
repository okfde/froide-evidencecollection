import logging

from django.conf import settings

from froide.georegion.models import GeoRegion

logger = logging.getLogger(__name__)

CONFIG = settings.FROIDE_EVIDENCECOLLECTION_NOCODB_IMPORT_CONFIG


def get_default_value(model, field_name):
    field = model._meta.get_field(field_name)

    if not field.has_default():
        return None

    if callable(field.default):
        return field.default()

    return field.default


def selectable_regions():
    config = CONFIG.get("selectable_regions")
    queryset = GeoRegion.objects.all()

    if config and "ids" in config:
        queryset = queryset.filter(id__in=config["ids"])

    return queryset


class ImportStats:
    def __init__(self):
        self.instance_is_new = False
        self.instance_failed = False
        self.created = 0
        self.updated = 0
        self.skipped = 0
        self.deleted = 0

    def reset_instance(self):
        self.instance_is_new = False
        self.instance_failed = False

    def reset(self):
        self.reset_instance()
        self.created = 0
        self.updated = 0
        self.skipped = 0
        self.deleted = 0

    def track(self, operation, count=1):
        if hasattr(self, operation):
            if operation == "created":
                self.instance_is_new = True
            elif operation == "skipped":
                self.instance_failed = True
            elif operation == "updated" and self.instance_is_new:
                return
            setattr(self, operation, getattr(self, operation) + count)

    def print_summary(self, model):
        logger.info(
            f"Model {model} processed: {self.created} created, {self.updated} updated, "
            f"{self.deleted} deleted, {self.skipped} skipped."
        )
