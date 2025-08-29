import datetime
import logging
import uuid
from itertools import chain

from django.conf import settings
from django.db import models

from froide.georegion.models import GeoRegion

logger = logging.getLogger(__name__)

CONFIG = settings.FROIDE_EVIDENCECOLLECTION_NOCODB_IMPORT_CONFIG


def get_base_class_name(model, exclude=None):
    """
    Returns the base class name of a Django model.

    Returns the name of the first parent class that is not `models.Model`.

    `exclude` can be provided to exclude additional base classes.
    """
    exclude = exclude or []
    exclude.append(models.Model)

    for base in model.__bases__:
        if base not in exclude:
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

    if isinstance(old_value, uuid.UUID) and isinstance(new_value, str):
        try:
            new_value = uuid.UUID(new_value)
        except ValueError:
            return False

    return old_value == new_value


def selectable_regions():
    config = CONFIG.get("selectable_regions")
    queryset = GeoRegion.objects.all()

    if config and "ids" in config:
        queryset = queryset.filter(id__in=config["ids"])

    return queryset


def is_serializable(field):
    return not isinstance(
        field,
        (
            models.DateTimeField,
            models.DateField,
            models.FileField,
            models.GeneratedField,
        ),
    )


def to_dict(instance):
    if instance is None:
        return {}

    opts = instance._meta
    data = {}

    for f in chain(opts.concrete_fields, opts.private_fields):
        if f.name != "id" and is_serializable(f):
            value = f.value_from_object(instance)
            if isinstance(f, models.UUIDField):
                value = str(value)

            data[f.name] = value

    for f in opts.many_to_many:
        data[f.name] = [i.id for i in f.value_from_object(instance)]

    return data


def get_changes(old_data, new_data):
    changes = {}

    for field in new_data.keys():
        if old_data.get(field) != new_data.get(field):
            changes[field] = {
                "old": old_data.get(field),
                "new": new_data.get(field),
            }

    return changes


class ImportStats:
    def __init__(self):
        self.instance_is_new = False
        self.instance_failed = False
        self.created = []
        self.updated = []
        self.skipped = []
        self.deleted = []

    def reset_instance(self):
        self.instance_is_new = False
        self.instance_failed = False

    def reset(self):
        self.reset_instance()
        self.created = []
        self.updated = []
        self.skipped = []
        self.deleted = []

    def track(self, operation, data):
        if hasattr(self, operation):
            if operation == "created":
                self.instance_is_new = True
            elif operation == "skipped":
                self.instance_failed = True
            elif operation == "updated" and self.instance_is_new:
                return

            stats = getattr(self, operation)
            if isinstance(data, list):
                stats.extend(data)
            else:
                stats.append(data)
            setattr(self, operation, stats)

    def get_summary(self):
        return (
            f"{len(self.created)} created, {len(self.updated)} updated, "
            f"{len(self.deleted)} deleted, {len(self.skipped)} skipped."
        )

    def to_dict(self):
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

    def track(self, operation, model, data):
        if model not in self.stats:
            self.stats[model] = ImportStats()

        self.stats[model].track(operation, data)

    def track_created(self, model, obj):
        data = {"id": obj.id, "fields": to_dict(obj)}

        self.track("created", model, data)

    def track_updated(self, model, old_obj_data, new_obj):
        new_obj_data = to_dict(new_obj)
        diff = get_changes(old_obj_data, new_obj_data)

        if diff:
            data = {
                "id": new_obj.id,
                "diff": diff,
            }

            self.track("updated", model, data)

    def track_deleted(self, model, data):
        self.track("deleted", model, data)

    def track_skipped(self, model, msg):
        self.track("skipped", model, msg)

    def log_summary(self, model):
        if model in self.stats:
            logger.info(
                f"Model {model.__name__} processed: {self.stats[model].get_summary()}"
            )

    def to_dict(self):
        return {model.__name__: stats.to_dict() for model, stats in self.stats.items()}

    def merge(self, other: "ImportStatsCollection"):
        for model, other_stats in other.stats.items():
            if model not in self.stats:
                self.stats[model] = ImportStats()
            self.stats[model].created.extend(other_stats.created)
            self.stats[model].updated.extend(other_stats.updated)
            self.stats[model].skipped.extend(other_stats.skipped)
            self.stats[model].deleted.extend(other_stats.deleted)
