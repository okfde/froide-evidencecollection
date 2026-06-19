import logging
import re
import textwrap
import uuid
from dataclasses import dataclass, replace
from urllib.parse import urlparse

from django.contrib.postgres.fields import ArrayField
from django.db import models
from django.db.models.signals import m2m_changed, post_delete, post_save
from django.dispatch import receiver
from django.urls import reverse
from django.utils import timezone
from django.utils.functional import cached_property
from django.utils.translation import gettext_lazy as _
from django.utils.translation import pgettext_lazy

from froide.georegion.models import GeoRegion
from froide_evidencecollection.storage import OverwriteStorage, post_screenshot_path
from froide_evidencecollection.utils import make_evidence_slug, to_dict

logger = logging.getLogger(__name__)


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

    class Meta:
        abstract = True

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


class Actor(TrackableModel):
    """
    Intermediate model that can be used as a foreign key in places where either
    a `Person` or `Organization` is needed.

    Organizing it this way instead of using multi-table inheritance has the advantage
    that we don't need to access the `Actor` table each time we want to access a
    `Person` or `Organization`.

    See also this blog post for a comparison of different approaches for ForeignKeys
    to multiple models:
    https://lukeplant.me.uk/blog/posts/avoid-django-genericforeignkey/#alternatives
    """

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

        return super(Actor, self).save(*args, **kwargs)

    @property
    def name(self):
        return str(self.target)

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

    @property
    def base_kind(self) -> str:
        # The semantic kind without the `redistributed:` prefix, so the detail
        # view can pick a per-kind style (quote / post / caption / …) without
        # branching on provenance. Mirrors `_topic_sort_key`'s split.
        return self.kind.split(":", 1)[1] if self.is_redistributed else self.kind


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


class PostMediaMixin(models.Model):
    """Media tracking for a `SocialMediaPost`.

    A post has at most one image and at most one video, so media no longer needs
    its own tables: the screenshot is stored as a file (the only file-backed
    post media — an archival capture of the post for provenance), while the
    content image and video are merely *tracked* by their import source path
    (the binaries are not stored, matching that they were never rendered
    publicly). `image_description` is the image's alt text; the video's full
    `transcription` is kept verbatim as a display/backup copy and is searched /
    topic-modelled only as the fallback when no `EvidenceMention.raw_transcript`
    excerpt exists (see `Evidence._video_transcript_segments`).
    """

    screenshot = models.ImageField(
        null=True,
        blank=True,
        max_length=255,
        upload_to=post_screenshot_path,
        storage=OverwriteStorage(),
        verbose_name=_("screenshot"),
    )
    screenshot_source_path = models.CharField(
        max_length=512, blank=True, default="", verbose_name=_("screenshot source path")
    )
    image_source_path = models.CharField(
        max_length=512, blank=True, default="", verbose_name=_("image source path")
    )
    image_description = models.TextField(
        blank=True,
        default="",
        verbose_name=_("image description"),
        help_text=_("Textual description (alt text) of what the image shows."),
    )
    video_source_path = models.CharField(
        max_length=512, blank=True, default="", verbose_name=_("video source path")
    )
    transcription = models.TextField(
        blank=True,
        default="",
        verbose_name=_("transcription"),
        help_text=_("Full verbatim video transcript; display/backup."),
    )

    class Meta:
        abstract = True


class SocialMediaPost(EvidenceSource, PostMediaMixin, models.Model):
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

    @property
    def is_video(self) -> bool:
        # A post counts as a video post if the import tracked a video file for
        # it. Drives the transcript-vs-own-text branch in
        # `Evidence._video_transcript_segments`.
        return bool(self.video_source_path)

    def _own_text_segments(self) -> list[TextSegment]:
        # This post's own authored text, excluding anything redistributed. For a
        # video post the (often promotional) `description` is dropped — the
        # transcript carries the content — but the title and body still ride
        # along; non-video posts keep their whole text. The image's alt-text
        # `image_description` is included where present. The video transcript is
        # not here: it is emitted at the Evidence level
        # (`Evidence._video_transcript_segments`).
        fields = [
            ("title", _("Post title"), self.title),
            ("body", _("Post text"), self.text),
        ]
        if not self.is_video:
            fields.append(("description", _("Description"), self.description))
        fields.append(("description", _("Image description"), self.image_description))
        segments = []
        for kind, label, value in fields:
            if value and value.strip():
                segments.append(TextSegment(kind, label, value.strip()))
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
        # Redacted (global + this post's scoped rules) so the summary never
        # leaks a masked term.
        text = "\n\n".join(
            s.text
            for s in self.text_segments(include_redistributed=False)
            if s.for_search
        )
        return apply_redactions(text, post=self)

    @property
    def display_text(self) -> str:
        return textwrap.shorten(self.full_text, width=50, placeholder="...")

    @property
    def publication_date(self):
        return self.posted_at.date() if self.posted_at else None

    @cached_property
    def media_descriptions(self):
        # (kind, text) shown in the detail view's Visual material section. The
        # media files themselves are admin-only / not stored, only what they
        # depict: the image's alt-text description and (for a video) that a
        # transcript exists.
        out = []
        if self.image_description and self.image_description.strip():
            out.append(("image", self.image_description.strip()))
        return out

    def compute_slug(self) -> str:
        return make_evidence_slug(self.account.platform, self.platform_post_id)

    def exclude_from_serialization(self):
        # The user snapshot is a large JSON blob persisted but excluded from
        # diffs so ImportExportRun.changes stays readable.
        return ["id", "user_snapshot"]


@receiver(post_delete, sender=SocialMediaPost)
def _delete_post_screenshot_file(sender, instance, **kwargs):
    # The overwrite storage doesn't deduplicate (unlike HashedFilenameStorage),
    # so every post owns its screenshot file outright — delete it from storage
    # when the post goes, including cascades from a deleted account. `save=False`
    # because the row is already gone.
    if instance.screenshot:
        instance.screenshot.delete(save=False)


# Redaction: read-time term→placeholder substitution over assembled text. The
# raw imported text is never mutated, so changing a rule only requires
# re-deriving downstream artifacts (search index, topic fit), never a data
# migration. Global rules (no `posts`) apply to every post; scoped rules apply
# only to the posts they list. The enabled global rules compile to a single
# callable cached at module level and invalidated by signals on `RedactionRule`
# (see below); per-post scoped rules are few and applied directly.
_GLOBAL_REDACTOR = None  # compiled callable for enabled global rules; None = stale


def _compile_redaction_rules(rules) -> "callable":
    """Compile RedactionRules into one callable applying each in turn.

    Literal patterns match whole-word and case-insensitively; regex patterns are
    used verbatim. An invalid regex is skipped (logged) rather than breaking the
    whole pass. Rules are applied sequentially — correct even when a regex rule
    carries its own capture groups, and cheap at this corpus size.
    """
    compiled = []
    for rule in rules:
        rx = rule.compiled_pattern()
        if rx is not None:
            compiled.append((rx, rule.placeholder))

    def apply(text: str) -> str:
        if not text:
            return text
        for rx, placeholder in compiled:
            text = rx.sub(placeholder, text)
        return text

    return apply


def _get_global_redactor() -> "callable":
    global _GLOBAL_REDACTOR
    if _GLOBAL_REDACTOR is None:
        rules = RedactionRule.objects.filter(enabled=True, posts__isnull=True)
        _GLOBAL_REDACTOR = _compile_redaction_rules(rules)
    return _GLOBAL_REDACTOR


def invalidate_global_redactor(*args, **kwargs):
    global _GLOBAL_REDACTOR
    _GLOBAL_REDACTOR = None


def apply_redactions(text: str, post=None) -> str:
    """Mask redacted terms in `text`: global rules, then `post`'s scoped rules.

    Applied at display time and when assembling `search_text` / `topic_text`, so
    the masked form is what reaches the page, the search index and the topic
    fit. Changing rules requires a re-index / topic re-fit to take effect on
    already-derived artifacts.
    """
    if not text:
        return text
    text = _get_global_redactor()(text)
    if post is not None:
        for rule in post.redaction_rules.all():
            if rule.enabled:
                text = rule.apply(text)
    return text


class RedactionRule(models.Model):
    """A term→placeholder substitution masking sensitive text (slurs, names).

    Applied read-time over assembled text — display, `search_text` and
    `topic_text` — by `apply_redactions`, so the raw imported text is never
    mutated. A rule is *global* when it lists no `posts` (applied to every post,
    for terms that are always sensitive) or *scoped* to the `posts` it lists
    (the context-dependent cases, shareable across several posts). The masked
    form is frozen into the search index / topic fit when those are derived, so
    editing a rule requires a re-index / topic re-fit to take effect there.
    """

    pattern = models.CharField(
        max_length=255,
        verbose_name=_("pattern"),
        help_text=_("Literal term (matched whole-word) or, if marked, a regex."),
    )
    is_regex = models.BooleanField(default=False, verbose_name=_("is regex"))
    placeholder = models.CharField(
        max_length=100,
        verbose_name=_("placeholder"),
        help_text=_("Replacement shown in place of the term, e.g. “[N-Wort]”."),
    )
    enabled = models.BooleanField(default=True, verbose_name=_("enabled"))
    posts = models.ManyToManyField(
        SocialMediaPost,
        blank=True,
        related_name="redaction_rules",
        verbose_name=_("posts"),
        help_text=_("Leave empty for a global rule; otherwise scope to these posts."),
    )

    class Meta:
        verbose_name = _("redaction rule")
        verbose_name_plural = _("redaction rules")
        ordering = ["pattern"]

    def __str__(self):
        return f"{self.pattern} → {self.placeholder}"

    def compiled_pattern(self):
        # Compiled regex for this rule, or None when the pattern is empty or an
        # invalid regex (logged, skipped — one bad rule must not break the pass).
        if not self.pattern:
            return None
        pattern = (
            self.pattern if self.is_regex else r"\b" + re.escape(self.pattern) + r"\b"
        )
        try:
            return re.compile(pattern, re.IGNORECASE)
        except re.error:
            logger.warning("Invalid redaction regex %r; skipping.", self.pattern)
            return None

    def apply(self, text: str) -> str:
        if not text:
            return text
        rx = self.compiled_pattern()
        return rx.sub(self.placeholder, text) if rx is not None else text


@receiver(post_save, sender=RedactionRule)
@receiver(post_delete, sender=RedactionRule)
def _invalidate_redactor_on_change(sender, **kwargs):
    # Drop the cached global redactor so the next render/index recompiles. A
    # rule change only affects already-derived search/topic artifacts after a
    # re-index / topic re-fit.
    invalidate_global_redactor()


@receiver(m2m_changed, sender=RedactionRule.posts.through)
def _invalidate_redactor_on_scope_change(sender, **kwargs):
    # Adding/removing posts can flip a rule between global and scoped, so the
    # global set may have changed — invalidate the cache.
    invalidate_global_redactor()


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
    # `label`, this is preserved across refits (see `fit_keywords`), so manual
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


class Evidence(TrackableModel):
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

    class Meta:
        verbose_name = _("piece of evidence")
        verbose_name_plural = _("pieces of evidence")

    def __str__(self):
        return f"{self.pk} - {self.title}"

    @cached_property
    def title(self):
        return textwrap.shorten(
            self.citation or self.description, width=50, placeholder="..."
        )

    @cached_property
    def domain(self) -> str:
        return urlparse(self.reference_url).netloc

    def get_absolute_url(self):
        return reverse("evidencecollection:evidence-detail", kwargs={"pk": self.pk})


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
