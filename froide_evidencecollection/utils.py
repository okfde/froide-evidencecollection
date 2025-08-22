import datetime
import json
import logging

from django.conf import settings
from django.db import models

from froide.georegion.models import GeoRegion

logger = logging.getLogger(__name__)

CONFIG = settings.FROIDE_EVIDENCECOLLECTION_NOCODB_IMPORT_CONFIG


def get_base_class_name(model):
    """
    Returns the base class name of a Django model.
    Returns the name of the first parent class that is not `models.Model`.
    """
    for base in model.__bases__:
        if base is not models.Model:
            return base.__name__

    return model.__name__


def get_default_value(model, field_name):
    field = model._meta.get_field(field_name)

    if not field.has_default():
        return None

    if callable(field.default):
        return field.default()

    return field.default


def equals(old_value, new_value):
    if isinstance(old_value, datetime.date) and isinstance(new_value, str):
        try:
            new_value = datetime.datetime.strptime(new_value, "%Y-%m-%d").date()
        except ValueError:
            return False

    return old_value == new_value


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

    def get_summary(self):
        return (
            f"{self.created} created, {self.updated} updated, "
            f"{self.deleted} deleted, {self.skipped} skipped."
        )

    def to_json(self):
        return {
            "created": self.created,
            "updated": self.updated,
            "skipped": self.skipped,
            "deleted": self.deleted,
        }


class ImportStatsCollection:
    def __init__(self):
        self.stats = {}

    def reset_instance(self, model):
        if model in self.stats:
            self.stats[model].reset_instance()

    def reset(self):
        for stats in self.stats.values():
            stats.reset()

    def instance_failed(self, model):
        if model in self.stats:
            return self.stats[model].instance_failed

        return False

    def track(self, model, operation, count=1):
        if model not in self.stats:
            self.stats[model] = ImportStats()
        self.stats[model].track(operation, count)

    def log_summary(self, model):
        if model in self.stats:
            logger.info(
                f"Model {model.__name__} processed: {self.stats[model].get_summary()}"
            )

    def to_json(self):
        return json.dumps(
            {model.__name__: stats.to_json() for model, stats in self.stats.items()},
            indent=4,
        )
