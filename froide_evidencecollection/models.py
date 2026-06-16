import re
import textwrap
import uuid
from dataclasses import dataclass, replace
from urllib.parse import urlparse

from django.contrib.postgres.fields import ArrayField
from django.db import models
from django.urls import reverse
from django.utils import timezone
from django.utils.functional import cached_property
from django.utils.translation import gettext_lazy as _
from django.utils.translation import pgettext_lazy

from treebeard.mp_tree import MP_Node

from froide.georegion.models import GeoRegion
from froide_evidencecollection.utils import (
    EVIDENCE_SLUG_LENGTH,
    compute_hash,
    make_evidence_slug,
    to_dict,
)


class TrackableModel(models.Model):
    created_at = models.DateTimeField(auto_now_add=True, verbose_name=_("created at"))
    updated_at = models.DateTimeField(auto_now=True, verbose_name=_("updated at"))

    class Meta:
        abstract = True

    def exclude_from_serialization(self):
        return ["id", "created_at", "updated_at"]


class SyncableModel(TrackableModel):
    """
    Base class for models that are synced with an external source (NocoDB) in both
    directions.

    These models need a nullable `external_id` field to keep track of the mapping
    between the local model instance and the external source. The field is nullable
    to allow creating new instances locally that are not yet synced to the external
    source.

    In addition, a `sync_uuid` field is used to uniquely identify the instance
    across systems, even if the `external_id` is not yet set.

    The field `synced_at` is used to keep track of the last time the instance
    was synced with the external source, i.e. when local changes have been pushed to
    the external source.
    """

    external_id = models.PositiveIntegerField(
        unique=True, null=True, blank=True, verbose_name=_("external ID")
    )
    synced_at = models.DateTimeField(blank=True, null=True, verbose_name=_("synced at"))
    sync_uuid = models.UUIDField(
        unique=True, editable=False, verbose_name=_("sync UUID")
    )
    last_synced_state = models.JSONField(default=dict, editable=False)

    class Meta:
        abstract = True

    @property
    def is_synced(self):
        return self.synced_at is not None and self.synced_at >= self.updated_at

    def save(self, *args, sync=False, **kwargs):
        if not self.sync_uuid:
            self.sync_uuid = uuid.uuid4()

        super().save(*args, **kwargs)

        if sync:
            self.mark_synced(self.updated_at)

    def mark_synced(self, synced_at=None):
        self.synced_at = synced_at or timezone.now()
        self.last_synced_state = self.get_current_state()
        self.save(update_fields=["synced_at", "last_synced_state"])

    def exclude_from_serialization(self):
        return super().exclude_from_serialization() + [
            "synced_at",
            "last_synced_state",
        ]

    def get_current_state(self):
        return to_dict(self)

    def get_additional_payload_data(self, field_map):
        return {}


class AbstractActor(SyncableModel):
    """Abstract base model for `Person` and `Organization`."""

    also_known_as = ArrayField(
        models.CharField(max_length=50),
        default=list,
        blank=True,
        verbose_name=_("also known as"),
    )
    wikidata_id = models.CharField(
        max_length=20, unique=True, blank=True, null=True, verbose_name=_("Wikidata ID")
    )
    name_hash = models.CharField(
        max_length=64, blank=True, default="", verbose_name=_("name hash")
    )

    class Meta:
        abstract = True

    def save(self, *args, **kwargs):
        self.name_hash = self.get_name_hash()
        super().save(*args, **kwargs)

    def get_name_hash(self):
        raise NotImplementedError

    @cached_property
    def wikidata_url(self):
        if self.wikidata_id:
            return f"https://www.wikidata.org/wiki/{self.wikidata_id}"
        return None


class Person(AbstractActor):
    first_name = models.CharField(
        max_length=50,
        verbose_name=_("first name"),
    )
    last_name = models.CharField(
        max_length=50,
        verbose_name=_("last name"),
    )
    title = models.CharField(
        max_length=20,
        blank=True,
        default="",
        verbose_name=_("title"),
    )
    aw_id = models.PositiveIntegerField(
        unique=True, blank=True, null=True, verbose_name=_("abgeordnetenwatch.de ID")
    )
    status = models.ForeignKey(
        "PersonStatus", blank=True, null=True, on_delete=models.SET_NULL
    )

    class Meta:
        verbose_name = _("person")
        verbose_name_plural = _("persons")

    def __str__(self):
        return f"{self.title or ''} {self.first_name} {self.last_name}".strip()

    def get_name_hash(self):
        return compute_hash(f"{self.first_name} {self.last_name}")

    @cached_property
    def aw_url(self):
        if self.aw_id:
            return f"https://www.abgeordnetenwatch.de/politician/{self.aw_id}"
        return None

    def get_additional_payload_data(self, field_map):
        return {"Typ": "Person"}


class PersonStatus(models.Model):
    name = models.CharField(unique=True, max_length=50, verbose_name=_("name"))

    class Meta:
        verbose_name = _("person status")
        verbose_name_plural = _("person statuses")

    def __str__(self):
        return self.name


class Organization(AbstractActor):
    organization_name = models.CharField(
        max_length=255,
        verbose_name=_("organization name"),
    )
    institutional_level = models.ForeignKey(
        "InstitutionalLevel",
        on_delete=models.PROTECT,
        verbose_name=_("institutional level"),
    )
    regions = models.ManyToManyField(GeoRegion, blank=True, verbose_name=_("regions"))
    special_regions = ArrayField(
        models.CharField(max_length=50),
        default=list,
        blank=True,
        verbose_name=_("special regions"),
    )
    status = models.ForeignKey(
        "OrganizationStatus", blank=True, null=True, on_delete=models.SET_NULL
    )

    def __str__(self):
        return self.organization_name.strip()

    def get_name_hash(self):
        return compute_hash(self.organization_name)

    class Meta:
        verbose_name = _("organization")
        verbose_name_plural = _("organizations")

    def get_additional_payload_data(self, field_map):
        regions = (
            list(self.regions.values_list("name", flat=True)) + self.special_regions
        )
        region_col_name = field_map.get("regions")

        return {
            "Typ": "Organisation",
            region_col_name: ",".join(regions) or None,
        }


class OrganizationStatus(models.Model):
    name = models.CharField(unique=True, max_length=50, verbose_name=_("name"))

    class Meta:
        verbose_name = _("organization status")
        verbose_name_plural = _("organization statuses")

    def __str__(self):
        return self.name


class ActorQuerySet(models.QuerySet):
    def with_account_stats(self):
        return self.annotate(
            account_count=models.Count("social_media_accounts", distinct=True),
            post_count=models.Count("social_media_accounts__posts", distinct=True),
            redistributed_count=models.Count(
                "social_media_accounts__posts__redistributed_by",
                filter=~models.Q(
                    social_media_accounts__posts__redistributed_by__account__actor=models.F(
                        "pk"
                    )
                ),
                distinct=True,
            ),
        )


class Actor(TrackableModel):
    """
    Intermediate model that can be used as a foreign key in places where either
    a `Person` or `Organization` is needed.

    Organizing it this way instead of using multi-table inheritance has the advantage
    that we don't need to access the `Actor` table each time we want to access a
    `Person` or `Organization`.

    `external_id` and `name` are denormalized copies of the target's values, kept
    in sync by `save()`. The target (Person/Organization) is the source of truth;
    the copies exist so admin lookups and queries by external ID don't need to
    join through the target. `external_id` is nullable because the target may
    itself lack one (Persons/Organizations not imported from NocoDB).

    See also this blog post for a comparison of different approaches for ForeignKeys
    to multiple models:
    https://lukeplant.me.uk/blog/posts/avoid-django-genericforeignkey/#alternatives
    """

    external_id = models.PositiveIntegerField(
        unique=True, null=True, blank=True, verbose_name=_("external ID")
    )
    name = models.CharField(
        max_length=255,
        blank=True,
        verbose_name=_("name"),
    )
    person = models.OneToOneField(
        Person,
        null=True,
        blank=True,
        on_delete=models.CASCADE,
        related_name="actor",
        verbose_name=_("person"),
    )
    organization = models.OneToOneField(
        Organization,
        null=True,
        blank=True,
        on_delete=models.CASCADE,
        related_name="actor",
        verbose_name=_("organization"),
    )

    objects = ActorQuerySet.as_manager()

    class Meta:
        verbose_name = _("actor")
        verbose_name_plural = _("actors")
        constraints = [
            models.CheckConstraint(
                name="actor_person_or_organization_required",
                condition=models.Q(person__isnull=False)
                | models.Q(organization__isnull=False),
            ),
            models.UniqueConstraint(
                fields=["person"],
                name="unique_actor_person",
                condition=models.Q(person__isnull=False),
            ),
            models.UniqueConstraint(
                fields=["organization"],
                name="unique_actor_organization",
                condition=models.Q(organization__isnull=False),
            ),
        ]

    def __str__(self):
        return self.name

    def save(self, *args, **kwargs):
        if [self.person, self.organization].count(None) != 1:
            raise ValueError("Exactly one of 'person' or 'organization' must be set.")

        self.external_id = self.target.external_id
        self.name = str(self.target)

        return super(Actor, self).save(*args, **kwargs)

    @cached_property
    def target(self):
        if self.person_id is not None:
            return self.person
        if self.organization_id is not None:
            return self.organization
        raise AssertionError("Neither 'person' nor 'organization' is set.")


class InstitutionalLevel(models.Model):
    name = models.CharField(unique=True, max_length=255, verbose_name=_("name"))

    class Meta:
        verbose_name = _("institutional level")
        verbose_name_plural = _("institutional levels")

    def __str__(self):
        return self.name


class Role(SyncableModel):
    name = models.CharField(unique=True, max_length=255, verbose_name=_("name"))

    class Meta:
        verbose_name = _("role")
        verbose_name_plural = _("roles")

    def __str__(self):
        return self.name


class Affiliation(SyncableModel):
    aw_id = models.PositiveIntegerField(
        unique=True, blank=True, null=True, verbose_name=_("abgeordnetenwatch.de ID")
    )
    person = models.ForeignKey(
        Person,
        on_delete=models.CASCADE,
        related_name="affiliations",
        verbose_name=_("person"),
    )
    organization = models.ForeignKey(
        Organization,
        on_delete=models.CASCADE,
        related_name="affiliations",
        verbose_name=_("organization"),
    )
    role = models.ForeignKey(
        Role,
        on_delete=models.CASCADE,
        verbose_name=_("role"),
        blank=True,
        null=True,
    )
    start_date = models.DateField(
        blank=True,
        null=True,
        verbose_name=_("start date"),
    )
    start_date_string = models.CharField(
        max_length=10,
        blank=True,
        default="",
        verbose_name=_("start date (string)"),
    )
    end_date = models.DateField(
        blank=True,
        null=True,
        verbose_name=_("end date"),
    )
    end_date_string = models.CharField(
        max_length=10,
        blank=True,
        default="",
        verbose_name=_("end date (string)"),
    )
    reference_url = models.URLField(
        max_length=500,
        blank=True,
        default="",
        verbose_name=_("reference URL"),
    )
    comment = models.TextField(
        blank=True,
        default="",
        verbose_name=_("comment"),
    )

    class Meta:
        verbose_name = _("affiliation")
        verbose_name_plural = _("affiliations")

    def __str__(self):
        return f"{self.person} - {self.organization} ({self.role})"

    def save(self, *args, **kwargs):
        # Update string date fields if corresponding date fields are set.
        if self.start_date:
            self.start_date_string = str(self.start_date)
        if self.end_date:
            self.end_date_string = str(self.end_date)

        return super().save(*args, **kwargs)

    @cached_property
    def aw_url(self):
        if self.aw_id:
            return f"https://www.abgeordnetenwatch.de/api/v2/candidacies-mandates/{self.aw_id}"
        return None


class PoliticalPosition(TrackableModel):
    """A political mandate, parliamentary role, or party office held by a person.

    Imported from the partner JSON dump's per-person ``functions`` list. Unlike
    `Affiliation` (person↔organization, synced with NocoDB / abgeordnetenwatch),
    this is curated data with its own provenance: a separate source URL for the
    start and the end date, and the Bundesland (`region`) the position is
    anchored in. That region is meaningful even for a federal mandate — it is the
    state a Bundestag member was elected from.

    `organization` is an optional link to the party Verband behind a party
    position; it is nullable and only ever points at an Organization that already
    exists (the importer matches but never creates one, so unmatched Verbände
    stay null rather than becoming stubs).

    Source dates are month-precision ("YYYY-MM"): `start_date` is stored as the
    first day of the month and `end_date` as the last, while display is
    month-only (see `start_date_display` / `end_date_display`).
    """

    class Type(models.TextChoices):
        MANDATE = "mandate", _("Mandate")
        PARLIAMENT = "parliament", _("Parliament")
        PARTY = "party", _("Party")

    person = models.ForeignKey(
        Person,
        on_delete=models.CASCADE,
        related_name="political_positions",
        verbose_name=_("person"),
    )
    type = models.CharField(max_length=10, choices=Type.choices, verbose_name=_("type"))
    role = models.ForeignKey(
        Role,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        verbose_name=_("role"),
    )
    label = models.CharField(max_length=255, verbose_name=_("label"))
    institutional_level = models.ForeignKey(
        "InstitutionalLevel",
        on_delete=models.PROTECT,
        blank=True,
        null=True,
        verbose_name=_("institutional level"),
    )
    region = models.ForeignKey(
        GeoRegion,
        on_delete=models.SET_NULL,
        blank=True,
        null=True,
        verbose_name=_("region"),
    )
    organization = models.ForeignKey(
        Organization,
        on_delete=models.PROTECT,
        blank=True,
        null=True,
        related_name="political_positions",
        verbose_name=_("organization"),
    )
    start_date = models.DateField(blank=True, null=True, verbose_name=_("start date"))
    end_date = models.DateField(blank=True, null=True, verbose_name=_("end date"))
    start_source_url = models.URLField(
        max_length=500,
        null=True,
        blank=True,
        default="",
        verbose_name=_("start date source URL"),
    )
    end_source_url = models.URLField(
        max_length=500,
        null=True,
        blank=True,
        default="",
        verbose_name=_("end date source URL"),
    )
    comment = models.TextField(blank=True, default="", verbose_name=_("comment"))

    class Meta:
        verbose_name = _("political position")
        verbose_name_plural = _("political positions")

    def __str__(self):
        return f"{self.person} - {self.label}"

    @staticmethod
    def _format_month(value):
        # Month-precision display ("YYYY/MM"); the stored day (1st / last of
        # month) is an artifact of using a DateField and is not shown.
        return value.strftime("%Y/%m") if value else ""

    @property
    def start_date_display(self):
        return self._format_month(self.start_date)

    @property
    def end_date_display(self):
        return self._format_month(self.end_date)


class SocialMediaAccountQuerySet(models.QuerySet):
    def with_post_stats(self):
        return self.annotate(
            post_count=models.Count("posts", distinct=True),
            redistributed_count=models.Count(
                "posts__redistributed_by",
                filter=~models.Q(posts__redistributed_by__account=models.F("pk")),
                distinct=True,
            ),
        )


class SocialMediaAccount(models.Model):
    class Platform(models.TextChoices):
        FACEBOOK = "facebook", _("Facebook")
        INSTAGRAM = "instagram", _("Instagram")
        TELEGRAM = "telegram", _("Telegram")
        TIKTOK = "tiktok", _("TikTok")
        TWITTER = "twitter", _("Twitter")
        YOUTUBE = "youtube", _("YouTube")

    actor = models.ForeignKey(
        Actor,
        null=True,
        blank=True,
        on_delete=models.CASCADE,
        related_name="social_media_accounts",
        verbose_name=_("actor"),
    )
    platform = models.CharField(
        max_length=20, choices=Platform.choices, verbose_name=_("platform")
    )
    username = models.CharField(max_length=255, verbose_name=_("username"))
    platform_user_id = models.CharField(
        max_length=255,
        blank=True,
        default="",
        verbose_name=_("platform user ID"),
    )
    display_name = models.CharField(
        max_length=255,
        blank=True,
        default="",
        verbose_name=_("display name"),
    )
    description = models.TextField(
        blank=True, default="", verbose_name=_("description")
    )
    url = models.URLField(
        max_length=500,
        blank=True,
        default="",
        verbose_name=_("URL"),
    )
    is_verified = models.BooleanField(
        null=True, blank=True, verbose_name=_("is verified")
    )
    follower_count = models.PositiveBigIntegerField(
        null=True, blank=True, verbose_name=_("follower count")
    )
    collected_at = models.DateTimeField(
        null=True, blank=True, verbose_name=_("collected_at")
    )

    objects = SocialMediaAccountQuerySet.as_manager()

    class Meta:
        verbose_name = _("social media account")
        verbose_name_plural = _("social media accounts")
        constraints = [
            models.UniqueConstraint(
                fields=["platform", "platform_user_id"],
                name="unique_social_media_account",
            ),
        ]
        ordering = ("platform", "username")

    def __str__(self):
        actor = self.actor or _("(unknown)")
        return f"{actor} - {self.get_platform_display()}: {self.username}"

    def exclude_from_serialization(self):
        return ["id"]


# Cap how deep `text_segments()` follows a chain of redistributed posts
# (repost-of-a-repost). The cycle guard alone would terminate, but real chains
# are 1–2 hops; this bounds work and query depth for pathological data.
MAX_REDISTRIBUTION_DEPTH = 3


# Topic-modelling assembly order. The embedding model truncates to a fixed
# token window, so whatever leads the text dominates the topic signal — lead
# with the highest-signal fields (body / document text, then transcription)
# and let lower-signal ones (title, caption) trail where they get truncated
# away first. Redistributed segments always trail a piece's own content.
_TOPIC_KIND_PRIORITY = {
    "citation": 0,
    "body": 0,
    "extracted_text": 0,
    "transcription": 1,
    "description": 2,
    "title": 2,
    "caption": 3,
}


def _topic_sort_key(seg: "TextSegment") -> tuple[int, int]:
    redistributed = seg.is_redistributed
    base = seg.kind.split(":", 1)[1] if redistributed else seg.kind
    return (1 if redistributed else 0, _TOPIC_KIND_PRIORITY.get(base, 4))


# URLs are noise to the embedding model and waste the (truncated) token budget
# on ~10-20 subword tokens of gibberish each, so they're dropped from the topic
# input only (search/display keep them). Matches http(s):// and bare www. URLs;
# leaves domain-like words without a scheme alone to avoid false positives.
_TOPIC_URL_RE = re.compile(r"(?:https?://|www\.)\S+", re.IGNORECASE)

# @mentions and #hashtags are normalised, not removed: in this actor-tracking
# domain the handle/tag is usually topical (`@john_doe`, `#fun`).
# We strip just the leading marker and split underscores, so `@john_doe`
# → `john doe` and `#fun` → `fun` — the plain-word form the embedding model
# saw in pretraining and a clean c-TF-IDF keyword. Dropping the `#` also unifies
# `#fun` with plain "fun" in body text.
_TOPIC_TAG_RE = re.compile(r"(?<!\w)[@#](\w+)")

# Residual web/social artifacts that survive URL stripping and tag
# normalisation: the `&amp;` entity leak and retweet/cross-post markers. Matched
# as standalone tokens only (`\b…\b`), so words containing them — "amplitude",
# "wert" — are untouched. Numbers are intentionally NOT removed here: they carry
# semantic signal for the document embedding (e.g. "50 Prozent", "Artikel 3").
# They're kept out of the keyword vocabulary downstream in `fit_keywords`
# instead, so the embedding sees them but they never become a facet.
_TOPIC_ARTIFACT_RE = re.compile(r"\b(?:amp|rt|via)\b", re.IGNORECASE)


def _strip_tag_marker(match: "re.Match") -> str:
    return match.group(1).replace("_", " ")


def _clean_topic_text(text: str) -> str:
    # URLs first, so a handle embedded in a URL path is removed with the URL
    # rather than being half-normalised by the tag pass.
    text = _TOPIC_URL_RE.sub(" ", text)
    text = _TOPIC_TAG_RE.sub(_strip_tag_marker, text)
    # Then drop the content-free web/social artifact tokens.
    text = _TOPIC_ARTIFACT_RE.sub(" ", text)
    # Collapse only the horizontal whitespace the substitutions leave behind;
    # the `\n\n` separators between text segments are load-bearing, so newlines
    # are preserved.
    return re.sub(r"[^\S\n]+", " ", text).strip()


@dataclass(frozen=True)
class TextSegment:
    """One labelled piece of textual content belonging to an evidence source.

    Sources expose their text as a list of these rather than a flat string, so
    the same definition drives the detail view (each segment rendered as a
    distinct, individually formatted block), full-text search (`for_search`)
    and topic modelling (`for_topics`). `attribution` is set on segments lifted
    from a redistributed post so display can show provenance.
    """

    kind: str
    label: str
    text: str
    fmt: str = "plain"
    for_search: bool = True
    for_topics: bool = True
    attribution: str = ""

    @property
    def is_redistributed(self) -> bool:
        return self.kind.startswith("redistributed:")


class EvidenceSource:
    """
    Uniform accessor surface for models attachable to an Evidence as a source.

    Subclasses expose `url` (model field) and implement `display_text`,
    `publication_date` and `text_segments` so callers don't branch on source
    type.
    """

    url: str

    @property
    def display_text(self) -> str:
        raise NotImplementedError

    @property
    def publication_date(self):
        raise NotImplementedError

    def text_segments(self) -> list[TextSegment]:
        raise NotImplementedError

    def compute_slug(self) -> str:
        """Derive the stable public slug for an Evidence backed by this source.

        Each source type owns its slug derivation because the seed is a frozen
        public contract (see `make_evidence_slug`); the value is computed once on
        the Evidence and never changes afterwards.
        """
        raise NotImplementedError


class Evidence(TrackableModel):
    external_id = models.PositiveIntegerField(
        unique=True, null=True, blank=True, verbose_name=_("external ID")
    )
    # Stable public identifier used in the evidence URL (see `make_evidence_slug`).
    # Derived from the source and set once in `save()`; never changed afterwards,
    # because partners derive the same value to link into our data.
    slug = models.SlugField(
        max_length=EVIDENCE_SLUG_LENGTH, unique=True, verbose_name=_("slug")
    )
    citation = models.TextField(blank=True, default="", verbose_name=_("citation"))
    description = models.TextField(
        blank=True, default="", verbose_name=_("description")
    )
    evidence_type = models.ForeignKey(
        "EvidenceType",
        null=True,
        blank=True,
        on_delete=models.PROTECT,
        verbose_name=_("evidence type"),
    )
    collections = models.ManyToManyField(
        "Collection",
        blank=True,
        verbose_name=_("collections"),
    )
    social_media_post = models.OneToOneField(
        "SocialMediaPost",
        on_delete=models.PROTECT,
        related_name="evidence",
        verbose_name=_("social media post"),
    )
    originators = models.ManyToManyField(
        Actor,
        related_name="originated_evidence",
        verbose_name=_("originators"),
    )
    related_actors = models.ManyToManyField(
        Actor,
        related_name="related_evidence",
        verbose_name=_("related actors"),
    )
    documentation_date = models.DateField(
        null=True, blank=True, verbose_name=_("documentation date")
    )

    # Populated by the `fit_post_topics` management command, which fits
    # BERTopic over `search_text` (the assembled source text). `topic` points
    # to the leaf Topic; null means this evidence hasn't been fitted yet (or
    # carries no usable text). topic_x/topic_y are the per-evidence 2D UMAP
    # coordinates used by the cloud view.
    topic = models.ForeignKey(
        "Topic",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="evidences",
        verbose_name=_("topic"),
    )
    topic_x = models.FloatField(null=True, blank=True, verbose_name=_("topic x"))
    topic_y = models.FloatField(null=True, blank=True, verbose_name=_("topic y"))
    topic_fit_at = models.DateTimeField(
        null=True, blank=True, verbose_name=_("topic fitted at")
    )
    topic_reassigned = models.BooleanField(
        default=False,
        verbose_name=_("topic reassigned from outlier"),
        help_text=_(
            "True if this evidence was an HDBSCAN outlier in the most recent "
            "fit and was moved into its topic by outlier reduction, rather "
            "than being a core cluster member."
        ),
    )
    # Populated by `fit_post_topics` alongside the topic fit: the content
    # keywords whose lemma actually occurs in this evidence's text. Drives the
    # keyword-facet browse surface of the topic cloud. Unlike `topic`, this is
    # an evidence-level signal (the word is really in the text), not a
    # cluster-level one.
    keywords = models.ManyToManyField(
        "Keyword",
        blank=True,
        related_name="evidences",
        verbose_name=_("keywords"),
    )

    class Meta:
        verbose_name = _("piece of evidence")
        verbose_name_plural = _("pieces of evidence")

    def compute_slug(self) -> str:
        return self.source.compute_slug()

    def save(self, *args, **kwargs):
        # Derive the public slug once, on first save. Never recompute it: the
        # value is a frozen contract partners derive to link into our data.
        if not self.slug and self.source is not None:
            self.slug = self.compute_slug()
        super().save(*args, **kwargs)

    def __str__(self):
        return f"{self.external_id} - {self.title}"

    @property
    def source(self) -> "EvidenceSource | None":
        # Returns None when no source is attached yet (e.g. a fresh, unsaved
        # instance), so callers can branch on `source is not None` without
        # tripping RelatedObjectDoesNotExist on the required FK.
        if self.social_media_post_id is None:
            return None
        return self.social_media_post

    @property
    def url(self) -> str:
        source = self.source
        return source.url if source is not None else ""

    @property
    def url_hash(self) -> str:
        url = self.url
        return compute_hash(url) if url else ""

    @cached_property
    def title(self) -> str:
        source = self.source
        return source.display_text if source is not None else ""

    def _mention_text_segments(self) -> list["TextSegment"]:
        # The per-mention citation quotes (`EvidenceMention.citation`): the
        # curated, category-relevant text the evidence was filed under. These
        # are the searched / topic-modelled text (`for_search`/`for_topics`),
        # standing in for the raw video-excerpt transcripts (which are now
        # display-only). Each is labelled with its category and footnote for the
        # detail view. Reads prefetched `mentions` (ordered by footnote),
        # skipping mentions that carry no citation.
        segments = []
        for mention in self.mentions.all():
            text = (mention.citation or "").strip()
            if not text:
                continue
            attribution = str(mention.category)
            if mention.footnote:
                attribution = f"{attribution} · {mention.footnote}"
            segments.append(
                TextSegment("citation", _("Citation"), text, attribution=attribution)
            )
        return segments

    @property
    def text_segments(self) -> list["TextSegment"]:
        # The source's labelled text followed by the evidence's own per-mention
        # citations. Single definition behind the detail view, search index and
        # topic modelling.
        source = self.source
        segments = list(source.text_segments()) if source is not None else []
        segments.extend(self._mention_text_segments())
        return segments

    @property
    def search_text(self) -> str:
        # Concatenation of the searchable segments, in source order; fed to
        # Elasticsearch. Order is irrelevant to ES (it tokenises everything).
        return "\n\n".join(s.text for s in self.text_segments if s.for_search)

    @property
    def topic_text(self) -> str:
        # Input to BERTopic. Distinct from `search_text`: only `for_topics`
        # segments, reordered (`_topic_sort_key`) so the highest-signal fields
        # lead — because the embedding model truncates to a fixed token window,
        # so trailing text is dropped before it influences the topic. Stable
        # sort keeps each source's internal order within a priority tier.
        # `_clean_topic_text` strips URLs and normalises @mentions / #hashtags
        # to plain words (noise + wasted token budget).
        segments = sorted(
            (s for s in self.text_segments if s.for_topics), key=_topic_sort_key
        )
        text = "\n\n".join(s.text for s in segments)
        return _clean_topic_text(text)

    @cached_property
    def domain(self) -> str:
        return urlparse(self.url).netloc

    @cached_property
    def categories(self):
        return Category.objects.filter(mentions__evidence=self).distinct()

    @cached_property
    def originator_actors(self):
        # Reads from the `originators` prefetch (one query for the whole page)
        # rather than firing a fresh SELECT per card. Falls back to a query if
        # the caller did not prefetch.
        return list(self.originators.all())

    @cached_property
    def categories_distinct(self):
        # Same intent as `categories` but reads from prefetched `mentions`
        # rather than issuing a fresh query, so a page of N evidence cards
        # costs one prefetch instead of N SELECTs.
        seen = {}
        for mention in self.mentions.all():
            if mention.category_id not in seen:
                seen[mention.category_id] = mention.category
        return list(seen.values())

    ATTACHMENT_KIND_ORDER = ("image", "video", "audio", "pdf", "other")

    @cached_property
    def attachments_by_kind(self):
        # Group attachments by media kind for the card chip row. Reads from
        # the prefetched manager so a result page does not fan out to N
        # extra queries.
        counts = {}
        for att in self.attachments.all():
            counts[att.kind] = counts.get(att.kind, 0) + 1
        return [(k, counts[k]) for k in self.ATTACHMENT_KIND_ORDER if k in counts]

    @cached_property
    def categories_with_footnotes(self):
        # Like `categories_distinct` but pairs each category with the footnote
        # references it appears under (deduped, sorted). Reads from prefetched
        # `mentions`, so it costs nothing per row beyond the prefetch.
        by_pk = {}
        for mention in self.mentions.all():
            entry = by_pk.setdefault(mention.category_id, (mention.category, []))
            if mention.footnote:
                entry[1].append(mention.footnote)
        return [
            (cat, sorted(set(footnotes)))
            for cat, footnotes in sorted(
                by_pk.values(), key=lambda e: e[0].name.lower()
            )
        ]

    def get_absolute_url(self):
        return reverse("evidencecollection:evidence-detail", kwargs={"slug": self.slug})


class Topic(models.Model):
    """A flat cluster from the most recent BERTopic fit.

    `fit_post_topics` populates one row per BERTopic cluster (including the
    outlier cluster with `bertopic_id = -1`). Topics position and colour the
    scatter cloud; keyword browsing is handled separately by the `Keyword`
    facets, so topics carry no hierarchy.

    `label` and `description` are editable; the fit command auto-generates a
    label from `keywords` when creating a row, and preserves edits across
    refits when invoked with `--keep-labels`.
    """

    bertopic_id = models.SmallIntegerField(
        null=True,
        blank=True,
        verbose_name=_("BERTopic ID"),
        help_text=_(
            "Cluster id assigned by BERTopic in the most recent fit. "
            "-1 marks the outlier cluster. Not stable across refits."
        ),
    )
    label = models.CharField(
        max_length=255, blank=True, default="", verbose_name=_("label")
    )
    description = models.TextField(
        blank=True, default="", verbose_name=_("description")
    )
    keywords = models.JSONField(default=list, verbose_name=_("keywords"))
    size = models.PositiveIntegerField(default=0, verbose_name=_("evidence count"))
    fit_at = models.DateTimeField(verbose_name=_("fitted at"))

    class Meta:
        verbose_name = _("topic")
        verbose_name_plural = _("topics")
        ordering = ["-size", "bertopic_id"]

    def __str__(self):
        return self.label or f"Topic {self.bertopic_id}"

    @property
    def is_outlier(self):
        return self.bertopic_id == -1


class KeywordGroup(models.Model):
    """A curated grouping of related keywords (e.g. a concept and its synonyms).

    Drives the keyword-group bar on the topic cloud: the empty-state entry
    points. Selecting a group filters to evidence containing *any* of its
    enabled member keywords (an OR over synonyms), then the keyword facets
    narrow it further. Membership and labels are hand-curated in the admin and
    are NOT touched by `fit_post_topics` — only the keywords' derived fields are
    refit, so curation survives refits (see `Keyword.group`).
    """

    label = models.CharField(max_length=100, verbose_name=_("label"))
    description = models.TextField(
        blank=True, default="", verbose_name=_("description")
    )

    class Meta:
        verbose_name = _("keyword group")
        verbose_name_plural = _("keyword groups")
        ordering = ["label"]

    def __str__(self):
        return self.label


class Keyword(models.Model):
    """A content keyword used as a faceted browse term over Evidence.

    Populated by `fit_keywords` (Design B): KeyBERT extracts the most salient
    keyphrases per piece of evidence, each phrase is lemmatised, and the lemma
    becomes the keyword. `lemma` is the normalised match key (German lemma,
    lower-cased) and the value carried in the `keyword` query param. Every
    surface form the lemma appeared in is tallied across documents in
    `surface_forms`; `label` is the most common of those — the human-readable
    text shown on the chip. Each Evidence is linked (via `Evidence.keywords`) to
    the keywords KeyBERT picked for it, so a facet selection narrows to the
    evidence KeyBERT associated with the concept (which may include evidence
    that implies it without the literal word).
    """

    lemma = models.CharField(max_length=100, unique=True, verbose_name=_("lemma"))
    # Curator-assigned group (concept + synonyms). Null = ungrouped. SET_NULL so
    # deleting a group just un-groups its keywords. Like custom_label/enabled,
    # this is preserved across refits.
    group = models.ForeignKey(
        "KeywordGroup",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="keywords",
        verbose_name=_("group"),
    )
    # Auto-derived surface form, (re)set on every fit — do not hand-edit; use
    # `custom_label` to override the display text. Set to the most common
    # surface form across documents (see `surface_forms`).
    label = models.CharField(max_length=100, verbose_name=_("label"))
    # Every surface form this keyword appeared in across the corpus, mapped to
    # how many documents used it, e.g. {"soziale medien": 12, "sozialen
    # medien": 3}. All forms collapse onto the single `lemma` for matching; this
    # records the raw variants behind that lemma. (Re)set on every fit; `label`
    # is derived as the most-used key here. Sorted most-frequent-first.
    surface_forms = models.JSONField(
        default=dict, blank=True, verbose_name=_("surface forms")
    )
    # Curator-editable display override. Blank = fall back to `label`. Unlike
    # `label`, this is preserved across refits (see `fit_post_topics`), so manual
    # naming sticks.
    custom_label = models.CharField(
        max_length=100, blank=True, default="", verbose_name=_("custom label")
    )
    # Curator switch: when False the keyword is hidden from the facet cloud and
    # not offered as a filter. Lets a curator suppress noise/uninteresting terms
    # without losing the row. Preserved across refits.
    enabled = models.BooleanField(default=True, verbose_name=_("enabled"))
    # Corpus document frequency: how many fitted pieces of evidence contain this
    # keyword's lemma, over the whole corpus. Cached here at fit time so the
    # facet view can rank keywords by keyness (over-representation in the
    # filtered slice vs. this corpus baseline) instead of raw frequency.
    df = models.PositiveIntegerField(default=0, verbose_name=_("document frequency"))
    # Salience = KeyBERT's cosine similarity between a picked keyphrase and the
    # document it was picked for: how *representative* the phrase is of that
    # document (not how relevant it is globally). Aggregated over every pick of
    # this keyword across the corpus and cached here for inspection — `max` is
    # its strongest single appearance, `mean` its average. Instrumentation for
    # deciding a salience-based keep/rescue rule; not yet used for filtering.
    # (Re)set on every fit; zeroed for keywords that fall out of a fit.
    salience_max = models.FloatField(default=0.0, verbose_name=_("max salience"))
    salience_mean = models.FloatField(default=0.0, verbose_name=_("mean salience"))
    fit_at = models.DateTimeField(verbose_name=_("fitted at"))

    class Meta:
        verbose_name = _("keyword")
        verbose_name_plural = _("keywords")
        ordering = ["label"]

    def __str__(self):
        return self.display_label

    @property
    def display_label(self) -> str:
        """Text shown on the facet chip: the curator override if set, else the
        auto-derived surface form."""
        return self.custom_label or self.label


class SocialMediaPost(EvidenceSource, models.Model):
    account = models.ForeignKey(
        SocialMediaAccount,
        on_delete=models.PROTECT,
        related_name="posts",
        verbose_name=_("account"),
    )
    platform_post_id = models.CharField(
        max_length=255, verbose_name=_("platform post ID")
    )
    url = models.URLField(max_length=500, verbose_name=_("URL"))
    posted_at = models.DateTimeField(null=True, blank=True, verbose_name=_("posted at"))
    edited_at = models.DateTimeField(null=True, blank=True, verbose_name=_("edited at"))
    text = models.TextField(blank=True, default="", verbose_name=_("text"))
    title = models.TextField(blank=True, default="", verbose_name=_("title"))
    description = models.TextField(
        blank=True, default="", verbose_name=_("description")
    )
    caption = models.TextField(blank=True, default="", verbose_name=_("caption"))
    view_count = models.PositiveBigIntegerField(
        null=True, blank=True, verbose_name=_("view count")
    )
    like_count = models.PositiveBigIntegerField(
        null=True, blank=True, verbose_name=_("like count")
    )
    comment_count = models.PositiveBigIntegerField(
        null=True, blank=True, verbose_name=_("comment count")
    )
    is_comment_disabled = models.BooleanField(
        null=True, blank=True, verbose_name=_("comments disabled")
    )
    share_count = models.PositiveBigIntegerField(
        null=True, blank=True, verbose_name=_("share count")
    )
    reactions = models.JSONField(null=True, blank=True, verbose_name=_("reactions"))
    reply_to = models.ForeignKey(
        "self",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="replies",
        verbose_name=_("reply to"),
    )
    redistributes = models.ForeignKey(
        "self",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="redistributed_by",
        verbose_name=_("redistributed post"),
        help_text=_(
            "Post whose content this post redistributes " "(repost, quote, forward, …)."
        ),
    )
    unresolved_redistribution = models.JSONField(
        null=True,
        blank=True,
        verbose_name=_("unresolved redistribution"),
        help_text=_(
            "Reference to a redistributed post that lacks a stable platform "
            "post ID and so could not be linked via `redistributes` (e.g. a "
            "Telegram hidden forward). Stored verbatim from the source."
        ),
    )
    user_snapshot = models.JSONField(
        null=True, blank=True, verbose_name=_("user snapshot")
    )
    raw = models.JSONField(verbose_name=_("raw payload"))

    class Meta:
        verbose_name = _("social media post")
        verbose_name_plural = _("social media posts")
        constraints = [
            models.UniqueConstraint(
                fields=["account", "platform_post_id"],
                name="unique_post_per_account",
            ),
        ]

    def __str__(self):
        return f"{self.account} #{self.platform_post_id}"

    def get_admin_url(self):
        if self.pk is None:
            return None
        return reverse(
            "admin:froide_evidencecollection_socialmediapost_change",
            args=[self.pk],
        )

    def _own_text_segments(self) -> list[TextSegment]:
        # This post's own text, excluding anything redistributed. Transcription
        # is not here: it lives per-media (in a video's excerpts) and is
        # emitted by `_media_text_segments`.
        segments = []
        for kind, label, value in (
            ("title", _("Post title"), self.title),
            ("body", _("Post text"), self.text),
            ("caption", _("Caption"), self.caption),
        ):
            if value and value.strip():
                segments.append(TextSegment(kind, label, value.strip()))
        return segments

    def _media_text_segments(self) -> list[TextSegment]:
        # Text carried by the post's attached media. Each image contributes a
        # description of what it shows plus its on-screen text (`content_text`,
        # an `extracted_text` segment); each video contributes a description
        # plus the text of each of its transcript excerpts (`transcription`
        # segments). The kinds keep the right topic priority and display label.
        # Reads the prefetched `images` / `videos` (+ `videos__excerpts`)
        # relations so a prefetch keeps it query-free; emit only non-empty
        # fields.
        #
        # Transcription segments are display-only (`for_search=False`,
        # `for_topics=False`): the searched/topic-modelled text for a video now
        # comes from the curated per-mention citations on the evidence (see
        # `Evidence._mention_text_segments`), not the raw excerpt transcripts.
        segments = []
        for image in self.images.all():
            for kind, label, value in (
                ("description", _("Media description"), image.description),
                ("extracted_text", _("Image text"), image.resolved_content_text),
            ):
                if value and value.strip():
                    segments.append(TextSegment(kind, label, value.strip()))
        for video in self.videos.all():
            if video.description and video.description.strip():
                segments.append(
                    TextSegment(
                        "description", _("Media description"), video.description.strip()
                    )
                )
            for excerpt in video.excerpts.all():
                value = excerpt.resolved_text
                if value and value.strip():
                    segments.append(
                        TextSegment(
                            "transcription",
                            _("Transcription"),
                            value.strip(),
                            for_search=False,
                            for_topics=False,
                        )
                    )
        return segments

    def text_segments(
        self, *, include_redistributed: bool = True, _depth: int = 0, _seen=None
    ) -> list[TextSegment]:
        # `_seen`/`_depth` guard against cycles and runaway chains in the
        # self-referential `redistributes` FK (untrusted, scraped data).
        # `unresolved_redistribution` carries a reference, not text, by design,
        # so it contributes no segment here.
        _seen = _seen if _seen is not None else set()
        if self.pk in _seen:
            return []
        _seen.add(self.pk)

        segments = self._own_text_segments()
        segments.extend(self._media_text_segments())
        if (
            include_redistributed
            and self.redistributes_id
            and _depth < MAX_REDISTRIBUTION_DEPTH
        ):
            inner = self.redistributes.text_segments(
                include_redistributed=True, _depth=_depth + 1, _seen=_seen
            )
            attribution = str(self.redistributes.account)
            segments.extend(
                replace(seg, kind=f"redistributed:{seg.kind}", attribution=attribution)
                for seg in inner
            )
        return segments

    @property
    def full_text(self) -> str:
        # Own searchable text only; used for the short `display_text` summary,
        # so it stays cheap (no redistribution recursion / extra queries).
        return "\n\n".join(
            s.text
            for s in self.text_segments(include_redistributed=False)
            if s.for_search
        )

    @property
    def display_text(self) -> str:
        return textwrap.shorten(self.full_text, width=50, placeholder="...")

    @property
    def publication_date(self):
        return self.posted_at.date() if self.posted_at else None

    def compute_slug(self) -> str:
        return make_evidence_slug(self.account.platform, self.platform_post_id)

    def exclude_from_serialization(self):
        # Large JSON payloads are persisted but excluded from diffs so
        # ImportExportRun.changes stays readable.
        return ["id", "raw", "user_snapshot"]


class BasePostMedia(TrackableModel):
    """Shared fields/behaviour for a media file belonging to a SocialMediaPost.

    Abstract: the concrete `PostImage` and `PostVideo` each get their own table
    so a row carries only the fields valid for its type (the previous single
    `PostMedia` model mixed image-only and video-only fields). Media hangs off
    the post rather than the Evidence because the post is the `EvidenceSource`:
    the per-media text is surfaced through `SocialMediaPost.text_segments()` and
    so flows into `Evidence.text_segments` like any other source text. The files
    themselves are admin-only and not rendered in the public views.

    `source_path` is the relative path from the import payload (e.g.
    "./video/foo.mp4"), used as the natural key for idempotent re-import
    (mirrors Attachment.external_id). The `post` FK and its `(post, source_path)`
    uniqueness live on the concrete subclasses — each needs its own
    `related_name` (`images` / `videos`).
    """

    file = models.FileField(
        upload_to="post_media", max_length=255, blank=True, verbose_name=_("file")
    )
    source_path = models.CharField(
        max_length=512, blank=True, default="", verbose_name=_("source path")
    )
    description = models.TextField(
        blank=True,
        default="",
        verbose_name=_("description"),
        help_text=_("Textual description of what the image/video shows."),
    )

    class Meta:
        abstract = True

    def __str__(self):
        return f"{self.post} - {self.file.name or self.source_path}"

    def exclude_from_serialization(self):
        return super().exclude_from_serialization() + ["file"]


class PostImage(BasePostMedia):
    """An image attached to a SocialMediaPost.

    `content_text` is the image's on-screen text (the searched text). It is
    import-filled and may be wrong; `content_text_override` holds a curator's
    correction and is preserved across re-imports. Read via
    `resolved_content_text` (`content_text_override or content_text`) — the same
    auto/override idiom as `Keyword.label`/`custom_label`. The importer keeps
    overwriting `content_text` freely; it never touches the override.
    """

    post = models.ForeignKey(
        SocialMediaPost,
        on_delete=models.CASCADE,
        related_name="images",
        verbose_name=_("post"),
    )
    content_text = models.TextField(
        blank=True,
        default="",
        verbose_name=_("content text"),
        help_text=_(
            "On-screen text of the image (searched). Import-filled; correct it "
            "via the override field."
        ),
    )
    content_text_override = models.TextField(
        blank=True,
        default="",
        verbose_name=_("content text (corrected)"),
        help_text=_(
            "Curator correction of the content text. Blank falls back to the "
            "imported value. Preserved across re-imports."
        ),
    )
    is_related_to_text = models.BooleanField(
        null=True,
        blank=True,
        verbose_name=_("related to post text"),
        help_text=_("Whether the image relates to the post's text. Import-filled."),
    )

    class Meta:
        verbose_name = _("post image")
        verbose_name_plural = _("post images")
        constraints = [
            models.UniqueConstraint(
                fields=["post", "source_path"],
                condition=~models.Q(source_path=""),
                name="unique_image_per_post_source",
            ),
        ]

    @property
    def resolved_content_text(self) -> str:
        # Curator correction wins over the imported text; blank override falls
        # back. Single source of truth for "the searched text of this image".
        return self.content_text_override or self.content_text


class PostVideo(BasePostMedia):
    """A video attached to a SocialMediaPost.

    The searched text of a video is not its whole transcript but the relevant
    *excerpt(s)*, held as `VideoExcerpt` rows (one video can have several). The
    full transcript is kept verbatim as a backup in `transcript_file` (e.g. an
    SRT sidecar) and is never searched.
    """

    post = models.ForeignKey(
        SocialMediaPost,
        on_delete=models.CASCADE,
        related_name="videos",
        verbose_name=_("post"),
    )
    # Full transcript kept verbatim as a backup (e.g. an SRT sidecar of the
    # video). Distinct from `file`, which is the media itself. Never parsed and
    # never searched — the excerpts carry the searched text.
    transcript_file = models.FileField(
        upload_to="post_media",
        max_length=255,
        blank=True,
        verbose_name=_("transcript file"),
        help_text=_("Full transcript (e.g. SRT), kept as backup; not searched."),
    )

    class Meta:
        verbose_name = _("post video")
        verbose_name_plural = _("post videos")
        constraints = [
            models.UniqueConstraint(
                fields=["post", "source_path"],
                condition=~models.Q(source_path=""),
                name="unique_video_per_post_source",
            ),
        ]

    def exclude_from_serialization(self):
        return super().exclude_from_serialization() + ["transcript_file"]


class VideoExcerpt(TrackableModel):
    """A relevant, time-coded excerpt of a video's transcript.

    One `PostVideo` can have several. `text` is the searched excerpt text: it is
    import-filled and may be wrong, while `text_override` holds a curator's
    correction preserved across re-imports. Read via `resolved_text`
    (`text_override or text`) — the same auto/override idiom as
    `PostImage.content_text`. `start`/`end` locate the excerpt within the video;
    `order` keeps a stable display/import sequence (and is the natural key the
    importer matches on, so an override survives re-import).
    """

    video = models.ForeignKey(
        PostVideo,
        on_delete=models.CASCADE,
        related_name="excerpts",
        verbose_name=_("video"),
    )
    order = models.PositiveIntegerField(default=0, verbose_name=_("order"))
    start = models.DurationField(null=True, blank=True, verbose_name=_("start"))
    end = models.DurationField(null=True, blank=True, verbose_name=_("end"))
    text = models.TextField(
        blank=True,
        default="",
        verbose_name=_("text"),
        help_text=_("Transcript excerpt text (searched). Import-filled."),
    )
    text_override = models.TextField(
        blank=True,
        default="",
        verbose_name=_("text (corrected)"),
        help_text=_(
            "Curator correction of the excerpt text. Blank falls back to the "
            "imported value. Preserved across re-imports."
        ),
    )

    class Meta:
        verbose_name = _("video excerpt")
        verbose_name_plural = _("video excerpts")
        ordering = ["video", "order"]
        constraints = [
            models.UniqueConstraint(
                fields=["video", "order"],
                name="unique_excerpt_order_per_video",
            ),
        ]

    def __str__(self):
        return f"{self.video} [{self.order}]"

    @property
    def resolved_text(self) -> str:
        # Curator correction wins over the imported text; blank override falls
        # back. Single source of truth for "the searched text of this excerpt".
        return self.text_override or self.text


class PostScreenshot(BasePostMedia):
    """An archival screenshot of the post itself (provenance).

    Unlike `PostImage`/`PostVideo`, a screenshot is not media *content* carried
    by the post but a capture *of* the post — proof of what was posted and how
    it looked. The post's own text already lives in `SocialMediaPost.text`, so a
    screenshot carries no searched text and contributes nothing to
    `text_segments` or the search index; it is a pure archival file (with an
    optional `description` for admin context, inherited from `BasePostMedia`).
    """

    post = models.ForeignKey(
        SocialMediaPost,
        on_delete=models.CASCADE,
        related_name="screenshots",
        verbose_name=_("post"),
    )

    class Meta:
        verbose_name = _("post screenshot")
        verbose_name_plural = _("post screenshots")
        constraints = [
            models.UniqueConstraint(
                fields=["post", "source_path"],
                condition=~models.Q(source_path=""),
                name="unique_screenshot_per_post_source",
            ),
        ]


class Collection(models.Model):
    name = models.CharField(max_length=255, unique=True, verbose_name=_("name"))
    description = models.TextField(
        blank=True, default="", verbose_name=_("description")
    )

    class Meta:
        verbose_name = _("collection")
        verbose_name_plural = _("collections")

    def __str__(self):
        return self.name


class Attachment(TrackableModel):
    # The external ID is a string in this case.
    external_id = models.CharField(
        unique=True, max_length=20, verbose_name=_("external ID")
    )
    evidence = models.ForeignKey(
        Evidence,
        on_delete=models.CASCADE,
        verbose_name=_("evidence"),
        related_name="attachments",
    )
    title = models.CharField(max_length=255, verbose_name=_("title"))
    file = models.FileField(
        upload_to="attachments", max_length=255, verbose_name=_("file")
    )
    mimetype = models.CharField(
        max_length=100, blank=True, default="", verbose_name=_("mimetype")
    )
    size = models.PositiveIntegerField(null=True, blank=True, verbose_name=_("size"))
    width = models.PositiveIntegerField(null=True, blank=True, verbose_name=_("width"))
    height = models.PositiveIntegerField(
        null=True, blank=True, verbose_name=_("height")
    )

    class Meta:
        verbose_name = _("attachment")
        verbose_name_plural = _("attachments")

    def __str__(self):
        return f"{self.evidence} - {self.file.name}"

    @cached_property
    def kind(self):
        # Bucket the mimetype into the same media kinds the UI uses for
        # icons and gating decisions.
        mt = (self.mimetype or "").lower()
        if mt.startswith("image/"):
            return "image"
        if mt.startswith("video/"):
            return "video"
        if mt.startswith("audio/"):
            return "audio"
        if mt == "application/pdf":
            return "pdf"
        return "other"

    def exclude_from_serialization(self):
        return super().exclude_from_serialization() + ["file"]


class EvidenceType(models.Model):
    name = models.CharField(unique=True, max_length=255, verbose_name=_("name"))

    class Meta:
        verbose_name = _("evidence type")
        verbose_name_plural = _("evidence types")

    def __str__(self):
        return self.name


class Category(models.Model):
    name = models.CharField(max_length=255, unique=True, verbose_name=_("name"))

    class Meta:
        verbose_name = _("category")
        verbose_name_plural = _("categories")
        ordering = ["name"]

    def __str__(self):
        return self.name


class EvidenceMention(models.Model):
    evidence = models.ForeignKey(
        Evidence,
        on_delete=models.CASCADE,
        related_name="mentions",
        verbose_name=_("evidence"),
    )
    category = models.ForeignKey(
        "Category",
        on_delete=models.CASCADE,
        related_name="mentions",
        verbose_name=_("category"),
    )
    footnote = models.CharField(
        max_length=255, blank=True, default="", verbose_name=_("footnote")
    )
    chapter_structure = models.JSONField(
        default=list, verbose_name=_("chapter structure")
    )
    chapter = models.ForeignKey(
        "Chapter",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="mentions",
        verbose_name=_("chapter"),
    )
    citation = models.TextField(blank=True, default="", verbose_name=_("citation"))

    class Meta:
        verbose_name = _("evidence mention")
        verbose_name_plural = _("evidence mentions")
        ordering = ["footnote"]

    def __str__(self):
        return f"{self.evidence} — {self.category} ({self.footnote})"

    def exclude_from_serialization(self):
        return ["id"]


class Chapter(MP_Node):
    """A node in the chapter hierarchy of the underlying report.

    The tree is materialised during the JSON import from the
    ``chapter_sturcrue`` field of each evidence mention, where every entry is a
    root-to-leaf list of chapter labels. A node's identity is the full path of
    labels leading to it, so the same label under different parents yields
    distinct nodes.

    ``is_main_topic`` marks the node whose label appears as the mention's
    ``topic`` (i.e. the chapter that names the thematic topic an evidence is
    filed under).
    """

    custom_label = models.CharField(max_length=255, verbose_name=_("label"))
    is_main_topic = models.BooleanField(default=False, verbose_name=_("is main topic"))

    node_order_by = ["custom_label"]

    class Meta:
        verbose_name = _("chapter")
        verbose_name_plural = _("chapters")

    def __str__(self):
        return self.custom_label

    @classmethod
    def get_or_create_from_path(cls, labels):
        """Return the leaf node for ``labels``, creating missing nodes.

        ``labels`` is an ordered list of chapter labels from root to leaf.
        Returns ``None`` when no non-empty label is given.
        """
        node = None
        for label in labels:
            label = (label or "").strip()
            if not label:
                continue
            if node is None:
                child = cls.objects.filter(depth=1, custom_label=label).first()
                if child is None:
                    child = cls.add_root(custom_label=label)
            else:
                child = node.get_children().filter(custom_label=label).first()
                if child is None:
                    # Reload to keep treebeard's child counters in sync before
                    # appending a new child on a possibly stale instance.
                    node.refresh_from_db()
                    child = node.add_child(custom_label=label)
            node = child
        return node

    def subsumed_evidences(self):
        """Evidences filed under this chapter or any of its descendants."""
        subtree = Chapter.get_tree(self)
        return Evidence.objects.filter(mentions__chapter__in=subtree).distinct()


class ImportExportRun(models.Model):
    IMPORT = "I"
    EXPORT = "E"
    OPERATIONS = {
        IMPORT: pgettext_lazy("froide-evidencecollection", "Import"),
        EXPORT: pgettext_lazy("froide-evidencecollection", "Export"),
    }

    FROIDE_EVIDENCECOLLECTION = "FE"
    NOCODB = "NC"
    ABGEORDNETENWATCH = "AW"
    WIKIDATA = "WD"
    JSON = "JS"
    DATA_ENDPOINTS = {
        FROIDE_EVIDENCECOLLECTION: _("Froide EvidenceCollection"),
        NOCODB: _("NocoDB"),
        ABGEORDNETENWATCH: _("abgeordnetenwatch.de"),
        WIKIDATA: _("Wikidata"),
        JSON: _("JSON dump"),
    }

    operation = models.CharField(
        max_length=1, choices=OPERATIONS, verbose_name=_("operation")
    )
    source = models.CharField(
        max_length=2, choices=DATA_ENDPOINTS, verbose_name=_("source")
    )
    target = models.CharField(
        max_length=2, choices=DATA_ENDPOINTS, verbose_name=_("target")
    )
    started_at = models.DateTimeField(auto_now_add=True, verbose_name=_("started at"))
    finished_at = models.DateTimeField(
        null=True, blank=True, verbose_name=_("finished at")
    )
    success = models.BooleanField(default=False, verbose_name=_("success"))
    changes = models.JSONField(default=dict, blank=True, verbose_name=_("changes"))
    notes = models.TextField(blank=True, default="", verbose_name=_("notes"))

    class Meta:
        verbose_name = _("🔧 Import/export run")
        verbose_name_plural = _("🔧 Import/export runs")

    def __str__(self):
        source_display = self.get_source_display()
        target_display = self.get_target_display()
        started_at = timezone.localtime(self.started_at).strftime("%d.%m.%Y, %H:%M")
        return f"{source_display} -> {target_display} | {started_at}"

    def complete(self, success: bool, changes: dict = None, notes: str = ""):
        self.success = success
        if changes is not None:
            self.changes = changes
        self.notes = notes
        self.finished_at = timezone.now()
        self.save()


class Parliament(models.Model):
    aw_id = models.PositiveIntegerField(
        unique=True, verbose_name=_("abgeordnetenwatch.de ID")
    )
    name = models.CharField(unique=True, max_length=50, verbose_name=_("name"))
    fraction = models.OneToOneField(
        Organization,
        on_delete=models.PROTECT,
        verbose_name=_("fraction"),
    )

    class Meta:
        verbose_name = _("🏛️ Parliament")
        verbose_name_plural = _("🏛️ Parliaments")

    def __str__(self):
        return self.name

    def find_matching_fraction(self):
        # Match the parliament name as a whole word between word boundaries
        # in the organization name. Hyphen - is explicitely allowed as part of
        # the word to avoid "Sachsen" matching in "Sachsen-Anhalt".
        #
        # `also_known_as` is searched too: when an org has been renamed to the
        # dump's scheme (e.g. "AfD-Fraktion im Bundestag" -> "Bundestagsfraktion")
        # the parliament wording survives only in the retained alias, so matching
        # aliases keeps this resolving exactly as it did before the rename.
        regex = f"([^\\w-]|^){self.name}([^\\w-]|$)"
        pattern = re.compile(regex)

        candidates = {
            org.pk: org
            for org in Organization.objects.filter(organization_name__regex=regex)
        }
        for org in Organization.objects.exclude(also_known_as=[]):
            if org.pk not in candidates and any(
                pattern.search(alias) for alias in org.also_known_as
            ):
                candidates[org.pk] = org
        candidates = list(candidates.values())

        if len(candidates) == 1:
            return candidates[0]
        elif len(candidates) > 1:
            cand_str = ", ".join(str(c) for c in candidates)
            msg = f"Multiple matching fractions found for parliament {self.name}: {cand_str}"
            raise ValueError(msg)
        else:
            raise ValueError(f"No matching fraction found for parliament {self.name}")


class ParliamentPeriod(models.Model):
    aw_id = models.PositiveIntegerField(
        unique=True, verbose_name=_("abgeordnetenwatch.de ID")
    )
    name = models.CharField(unique=True, max_length=255, verbose_name=_("name"))
    start_date = models.DateField(verbose_name=_("start date"))
    end_date = models.DateField(null=True, blank=True, verbose_name=_("end date"))

    class Meta:
        abstract = True
        unique_together = ("parliament", "start_date", "end_date")

    def __str__(self):
        return self.name


class Election(ParliamentPeriod):
    parliament = models.ForeignKey(
        Parliament,
        on_delete=models.CASCADE,
        related_name="elections",
        verbose_name=_("parliament"),
    )

    class Meta(ParliamentPeriod.Meta):
        verbose_name = _("🏛️ Election")
        verbose_name_plural = _("🏛️ Elections")


class LegislativePeriod(ParliamentPeriod):
    parliament = models.ForeignKey(
        Parliament,
        on_delete=models.CASCADE,
        related_name="legislative_periods",
        verbose_name=_("parliament"),
    )
    election = models.OneToOneField(
        Election,
        blank=True,
        null=True,
        on_delete=models.PROTECT,
        verbose_name=_("election"),
        related_name="legislative_period",
    )

    class Meta(ParliamentPeriod.Meta):
        verbose_name = _("🏛️ Legislative period")
        verbose_name_plural = _("🏛️ Legislative periods")
