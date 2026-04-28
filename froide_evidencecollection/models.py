import textwrap
import uuid
from urllib.parse import urlparse

from django.contrib.postgres.fields import ArrayField
from django.db import models
from django.urls import reverse
from django.utils import timezone
from django.utils.functional import cached_property
from django.utils.translation import gettext_lazy as _
from django.utils.translation import pgettext_lazy

from froide.georegion.models import GeoRegion
from froide_evidencecollection.utils import compute_hash, to_dict


class TrackableModel(models.Model):
    created_at = models.DateTimeField(auto_now_add=True, verbose_name=_("created at"))
    updated_at = models.DateTimeField(auto_now=True, verbose_name=_("updated at"))

    class Meta:
        abstract = True

    def exclude_from_serialization(self):
        return ["id", "created_at", "updated_at"]


class ImportableModel(TrackableModel):
    """
    Base class for models that are imported from an external source (NocoDB) but should
    not be synced back to it.

    These models need an unnullable `external_id` field to keep track of the mapping
    between the local model instance and the external source.
    """

    external_id = models.PositiveIntegerField(
        unique=True, verbose_name=_("external ID")
    )

    class Meta:
        abstract = True


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
    is_synced = models.GeneratedField(
        expression=models.Case(
            models.When(synced_at__isnull=True, then=models.Value(False)),
            models.When(synced_at__gte=models.F("updated_at"), then=models.Value(True)),
            default=models.Value(False),
        ),
        output_field=models.BooleanField(),
        db_persist=True,
        verbose_name=_("is synced"),
    )
    last_synced_state = models.JSONField(default=dict, editable=False)

    class Meta:
        abstract = True

    def save(self, *args, sync=False, **kwargs):
        if not self.sync_uuid:
            self.sync_uuid = uuid.uuid4()

        super().save(*args, **kwargs)

        if sync:
            self.mark_synced(self.updated_at)

        # Refresh for correct value of `is_synced` field.
        self.refresh_from_db()

    def mark_synced(self, synced_at=None):
        self.synced_at = synced_at or timezone.now()
        self.last_synced_state = self.get_current_state()
        self.save(update_fields=["synced_at", "last_synced_state"])

    def exclude_from_serialization(self):
        return super().exclude_from_serialization() + [
            "synced_at",
            "is_synced",
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


class Actor(ImportableModel):
    """
    Intermediate model that can be used as a foreign key in places where either
    a `Person` or `Organization` is needed.

    Organizing it this way instead of using multi-table inheritance has the advantage
    that we don't need to access the `Actor` table each time we want to access a
    `Person` or `Organization`.

    In addition, we can copy some fields from the target model to this model
    (like `external_id` and `name`) to make lookups and display easier.

    See also this blog post for a comparison of different approaches for ForeignKeys
    to multiple models:
    https://lukeplant.me.uk/blog/posts/avoid-django-genericforeignkey/#alternatives
    """

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


class SocialMediaAccount(models.Model):
    class Platform(models.TextChoices):
        FACEBOOK = "facebook", _("Facebook")
        INSTAGRAM = "instagram", _("Instagram")
        TELEGRAM = "telegram", _("Telegram")
        TIKTOK = "tiktok", _("TikTok")
        X = "x", _("X")
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
    bio = models.TextField(blank=True, default="", verbose_name=_("bio"))
    profile_url = models.URLField(
        max_length=500,
        blank=True,
        default="",
        verbose_name=_("profile URL"),
    )
    is_verified = models.BooleanField(
        null=True, blank=True, verbose_name=_("is verified")
    )
    follower_count = models.PositiveBigIntegerField(
        null=True, blank=True, verbose_name=_("follower count")
    )
    profile_retrieved_at = models.DateTimeField(
        null=True, blank=True, verbose_name=_("profile retrieved at")
    )

    class Meta:
        verbose_name = _("social media account")
        verbose_name_plural = _("social media accounts")
        constraints = [
            models.UniqueConstraint(
                fields=["platform", "username"],
                name="unique_social_media_account",
            ),
        ]
        ordering = ("platform", "username")

    def __str__(self):
        actor = self.actor or _("(unknown)")
        return f"{actor} - {self.get_platform_display()}: {self.username}"


class Evidence(ImportableModel):
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
    originators = models.ManyToManyField(
        Actor, verbose_name=_("originators"), related_name="originated_evidence"
    )
    related_actors = models.ManyToManyField(
        Actor, verbose_name=_("related actors"), related_name="related_evidence"
    )
    event_date = models.DateField(null=True, blank=True, verbose_name=_("event date"))
    publishing_date = models.DateField(
        null=True, blank=True, verbose_name=_("publishing date")
    )
    documentation_date = models.DateField(
        null=True, blank=True, verbose_name=_("documentation date")
    )
    reference_url = models.URLField(
        max_length=500, blank=True, default="", verbose_name=_("reference (URL)")
    )
    reference_info = models.TextField(
        blank=True, default="", verbose_name=_("reference (additional information)")
    )
    primary_source_url = models.URLField(
        max_length=500, blank=True, default="", verbose_name=_("primary source URL")
    )
    primary_source_info = models.TextField(
        blank=True,
        default="",
        verbose_name=_("primary source (additional information)"),
    )
    attribution_justification = models.TextField(
        blank=True, default="", verbose_name=_("attribution justification")
    )
    attribution_evidence = models.ManyToManyField(
        "Evidence", blank=True, verbose_name=_("attribution evidence")
    )
    attribution_problems = models.ManyToManyField(
        "AttributionProblem", blank=True, verbose_name=_("attribution problems")
    )
    posted_by = models.ForeignKey(
        SocialMediaAccount,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="posted_evidence",
        verbose_name=_("posted by"),
    )
    source = models.ForeignKey(
        "EvidenceSource",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="evidence_set",
        verbose_name=_("source"),
    )
    comment = models.TextField(blank=True, default="", verbose_name=_("comment"))
    legal_assessment = models.PositiveIntegerField(
        choices=[
            (1, "⭐"),
            (2, "⭐⭐"),
            (3, "⭐⭐⭐"),
            (4, "⭐⭐⭐⭐"),
            (5, "⭐⭐⭐⭐⭐"),
        ],
        null=True,
        blank=True,
        verbose_name=_("legal assessment"),
    )
    url_hash = models.CharField(
        max_length=64, blank=True, default="", verbose_name=_("URL hash")
    )

    class Meta:
        verbose_name = _("piece of evidence")
        verbose_name_plural = _("pieces of evidence")

    def __str__(self):
        return f"{self.external_id} - {self.title}"

    @cached_property
    def title(self):
        return textwrap.shorten(
            self.citation or self.description, width=50, placeholder="..."
        )

    @cached_property
    def domain(self) -> str:
        return urlparse(self.reference_url).netloc

    @cached_property
    def categories(self):
        return Category.objects.filter(mentions__evidence=self).distinct()

    def get_absolute_url(self):
        return reverse("evidencecollection:evidence-detail", kwargs={"pk": self.pk})

    def save(self, *args, **kwargs):
        self.url_hash = compute_hash(self.reference_url)
        super().save(*args, **kwargs)


class SocialMediaPost(models.Model):
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
    transcription = models.TextField(
        blank=True, default="", verbose_name=_("transcription")
    )
    view_count = models.PositiveBigIntegerField(
        null=True, blank=True, verbose_name=_("view count")
    )
    like_count = models.PositiveBigIntegerField(
        null=True, blank=True, verbose_name=_("like count")
    )
    comment_count = models.PositiveBigIntegerField(
        null=True, blank=True, verbose_name=_("comment count")
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
    quoted = models.ForeignKey(
        "self",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="quoted_by",
        verbose_name=_("quoted post"),
    )
    repost_of = models.ForeignKey(
        "self",
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        related_name="reposts",
        verbose_name=_("repost of"),
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


class EvidenceSource(models.Model):
    """
    Intermediate model that can be used as a foreign key on `Evidence` to point
    at exactly one concrete source record (currently only `SocialMediaPost`,
    later potentially news articles, court documents, etc.).

    Mirrors the `Actor` pattern: instead of putting a nullable FK per source
    type on `Evidence`, we route through this table so `Evidence` only knows
    about a single `source` field.
    """

    social_media_post = models.OneToOneField(
        SocialMediaPost,
        null=True,
        blank=True,
        on_delete=models.CASCADE,
        related_name="source",
        verbose_name=_("social media post"),
    )

    class Meta:
        verbose_name = _("evidence source")
        verbose_name_plural = _("evidence sources")
        constraints = [
            models.CheckConstraint(
                name="evidence_source_target_required",
                condition=models.Q(social_media_post__isnull=False),
            ),
        ]

    def __str__(self):
        return (
            str(self.target)
            if self.target is not None
            else f"EvidenceSource #{self.pk}"
        )

    @cached_property
    def target(self):
        if self.social_media_post_id is not None:
            return self.social_media_post
        return None

    def save(self, *args, **kwargs):
        if self.social_media_post_id is None:
            raise ValueError("EvidenceSource requires a concrete source target.")
        return super().save(*args, **kwargs)


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


class Attachment(ImportableModel):
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

    def exclude_from_serialization(self):
        return super().exclude_from_serialization() + ["file"]


class AttributionProblem(models.Model):
    name = models.CharField(max_length=255, unique=True, verbose_name=_("name"))

    class Meta:
        verbose_name = _("attribution problem")
        verbose_name_plural = _("attribution problems")

    def __str__(self):
        return self.name


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
    page = models.PositiveIntegerField(verbose_name=_("page"))

    class Meta:
        verbose_name = _("evidence mention")
        verbose_name_plural = _("evidence mentions")
        ordering = ["page"]

    def __str__(self):
        return f"{self.evidence} — {self.category} (p. {self.page})"


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
    DATA_ENDPOINTS = {
        FROIDE_EVIDENCECOLLECTION: _("Froide EvidenceCollection"),
        NOCODB: _("NocoDB"),
        ABGEORDNETENWATCH: _("abgeordnetenwatch.de"),
        WIKIDATA: _("Wikidata"),
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
        regex = f"([^\\w-]|^){self.name}([^\\w-]|$)"

        candidates = Organization.objects.filter(organization_name__regex=regex)

        if candidates.count() == 1:
            return candidates.first()
        elif candidates.count() > 1:
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
