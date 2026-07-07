import base64
import csv
import datetime
import hashlib
import logging
import re
import uuid
from itertools import chain
from pathlib import Path

from django.conf import settings
from django.db import models

from froide.georegion.models import GeoRegion

logger = logging.getLogger(__name__)

CONFIG = settings.FROIDE_EVIDENCECOLLECTION_CONFIG

ORG_LABEL_REPLACEMENTS_PATH = (
    Path(__file__).resolve().parent / "data" / "org_label_replacements.csv"
)


def load_org_label_replacements(path=ORG_LABEL_REPLACEMENTS_PATH):
    """Load dump-label corrections as ``(exact, prefix)`` maps.

    ``exact`` replaces a whole label, ``prefix`` expands a leading abbreviation
    token (keeping the rest). Used to normalize the dump's shorthand both when
    aligning Organization names and when matching dump labels during import, so
    the two stay in lockstep.
    """
    exact, prefix = {}, {}
    if not Path(path).exists():
        return exact, prefix
    with open(path, encoding="utf-8") as f:
        rows = [line for line in f if not line.lstrip().startswith("#")]
    for row in csv.DictReader(rows):
        kind, wrong, correct = row.get("kind"), row.get("wrong"), row.get("correct")
        if not (wrong and correct):
            continue
        if kind == "exact":
            exact[wrong] = correct
        elif kind == "prefix":
            prefix[wrong] = correct
    return exact, prefix


def apply_org_label_replacement(label, replacements):
    """Apply ``load_org_label_replacements`` maps to a single org label."""
    exact, prefix = replacements
    if label in exact:
        return exact[label]
    head, sep, tail = label.partition(" ")
    if sep and head in prefix:
        return f"{prefix[head]} {tail}"
    return label


def compute_hash(text):
    if not text:
        return ""

    return hashlib.sha256(text.encode()).hexdigest()


# Length of the public evidence slug, in base32 characters. 10 chars = 50 bits.
# This is a frozen, partner-derivable contract that can never be re-rolled on
# collision, so the width is chosen for ample headroom: at 50 bits, birthday
# collisions stay negligible to ~200k rows (~2e-5) and ~0.04% even at 1M — far
# beyond the current low-thousands corpus.
EVIDENCE_SLUG_LENGTH = 10


def make_evidence_slug(platform: str, post_id: str) -> str:
    """Derive an evidence's public slug from its social media source.

    This is a frozen public contract: a partner derives the same value from the
    same inputs to build links into our data, so the seed format, hash, encoding
    and length must never change. Seed is ``smp:<platform>:<post_id>`` where
    ``platform`` is the canonical ``SocialMediaAccount.Platform`` value (e.g.
    ``twitter``, never ``x``). The digest is RFC 4648 base32, lowercased, with
    padding stripped, truncated to ``EVIDENCE_SLUG_LENGTH``.
    """
    seed = f"smp:{platform}:{post_id}".encode("utf-8")
    digest = hashlib.sha256(seed).digest()
    return (
        base64.b32encode(digest)
        .decode("ascii")
        .lower()
        .rstrip("=")[:EVIDENCE_SLUG_LENGTH]
    )


def normalize_name(text):
    """Normalize an actor name so the same entity matches across naming schemes.

    Lowercases, folds ``ß`` to ``ss`` (so "Moriße" matches "Morisse"), drops
    parentheticals like ``(NRW)``/``(JA)`` and the party token, and collapses
    any run of separators (spaces, hyphens, slashes) to a single space. Used
    both when aligning organization names against the dump and when resolving
    dump labels to existing actors during import, so the two stay consistent.
    """
    if not text:
        return ""

    text = text.lower()
    text = text.replace("ß", "ss")  # "Moriße" == "Morisse"
    text = re.sub(r"\(.*?\)", " ", text)  # drop "(NRW)", "(JA)", ...
    text = re.sub(r"\bafd\b", " ", text)  # drop the party token
    text = re.sub(r"[^0-9a-zäöüß]+", " ", text)  # collapse separators
    return text.strip()


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
            new_value = datetime.date.fromisoformat(new_value)
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
    # Callers (the importer's name→pk map, the admin M2M widget) read only
    # scalar fields; defer GeoRegion's large geometry columns (`geom`,
    # `geom_detail`, `gov_seat`) so they aren't fetched and GEOS-deserialized
    # per region.
    queryset = GeoRegion.objects.defer("geom", "geom_detail", "gov_seat")

    if config and "ids" in config:
        queryset = queryset.filter(id__in=config["ids"])

    return queryset


def to_dict(instance):
    if instance is None:
        return {}

    opts = instance._meta
    data = {}

    for f in chain(opts.concrete_fields, opts.private_fields):
        if f.name not in instance.exclude_from_serialization():
            value = f.value_from_object(instance)
            if (
                isinstance(
                    f,
                    (
                        models.UUIDField,
                        models.DateField,
                        models.DurationField,
                        models.FileField,
                    ),
                )
                and value is not None
            ):
                # FileField/ImageField yields a FieldFile, which is not JSON
                # serializable; store its path (name) so changes stay diffable.
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


def get_today():
    """Get today's date. Abstracted for easier mocking in testing."""
    return datetime.date.today()


def filter_future_date(date):
    """Return None if the date is in the future, otherwise return the date."""
    if isinstance(date, str):
        date = datetime.date.fromisoformat(date)

    if date and date > get_today():
        return None

    return date


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


class ExportStats:
    def __init__(self):
        self.created = []
        self.updated = []
        self.failed_links = []

    def reset(self):
        self.created = []
        self.updated = []
        self.failed_links = []

    def track(self, operation, data):
        if hasattr(self, operation):
            stats = getattr(self, operation)
            if isinstance(data, list):
                stats.extend(data)
            else:
                stats.append(data)
            setattr(self, operation, stats)

    def get_summary(self):
        return (
            f"{len(self.created)} created, {len(self.updated)} updated, "
            f"{len(self.failed_links)} failed links."
        )

    def to_dict(self):
        return {
            "created": self.created,
            "updated": self.updated,
            "failed_links": self.failed_links,
        }


class ExportStatsCollection:
    def __init__(self):
        self.stats = {}

    def reset(self):
        for stats in self.stats.values():
            stats.reset()

    def track(self, operation, model, data):
        if model not in self.stats:
            self.stats[model] = ExportStats()

        self.stats[model].track(operation, data)

    def track_created(self, model, obj):
        data = {"id": obj.id, "fields": to_dict(obj)}

        self.track("created", model, data)

    def track_updated(self, model, new_obj):
        new_obj_data = to_dict(new_obj)
        old_obj_data = new_obj.last_synced_state
        diff = get_changes(old_obj_data, new_obj_data)

        if diff:
            data = {
                "id": new_obj.id,
                "diff": diff,
            }

            self.track("updated", model, data)

    def track_failed_link(self, model, field_id, record_id, rel_instance_id):
        data = {
            "field_id": field_id,
            "record_id": record_id,
            "rel_instance_id": rel_instance_id,
        }

        self.track("failed_links", model, data)

    def log_summary(self, model):
        if model in self.stats:
            logger.info(
                f"Model {model.__name__} processed: {self.stats[model].get_summary()}"
            )

    def to_dict(self):
        return {model.__name__: stats.to_dict() for model, stats in self.stats.items()}

    def merge(self, other: "ExportStatsCollection"):
        for model, other_stats in other.stats.items():
            if model not in self.stats:
                self.stats[model] = ExportStats()
            self.stats[model].created += other_stats.created
            self.stats[model].updated += other_stats.updated
