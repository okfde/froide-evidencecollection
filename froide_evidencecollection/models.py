import logging
import re
import uuid
from dataclasses import dataclass, replace

from django.contrib.postgres.fields import ArrayField
from django.db import models
from django.db.models.signals import m2m_changed, post_delete, post_save
from django.dispatch import receiver
from django.urls import reverse
from django.utils import timezone
from django.utils.functional import cached_property
from django.utils.translation import gettext_lazy as _
from django.utils.translation import pgettext_lazy

from cms.models import CMSPlugin
from treebeard.mp_tree import MP_Node

from froide.georegion.models import GeoRegion
from froide_evidencecollection.storage import OverwriteStorage, post_screenshot_path
from froide_evidencecollection.utils import (
    EVIDENCE_SLUG_LENGTH,
    make_evidence_slug,
)

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
    Base class for models that are populated from an external source (e.g.
    abgeordnetenwatch.de or Wikidata).

    A `sync_uuid` field is used to uniquely identify the instance across systems.
    """

    sync_uuid = models.UUIDField(
        unique=True, editable=False, verbose_name=_("sync UUID")
    )

    class Meta:
        abstract = True

    def save(self, *args, **kwargs):
        if not self.sync_uuid:
            self.sync_uuid = uuid.uuid4()

        super().save(*args, **kwargs)


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
    # The regional chapter ("Verband") the actor belongs to: a Bundesland for a
    # Landesverband, or the country-level region ("Deutschland") for "Bund".
    verband = models.ForeignKey(
        GeoRegion,
        on_delete=models.PROTECT,
        blank=True,
        null=True,
        related_name="+",
        verbose_name=_("Verband"),
        limit_choices_to={"kind__in": ["state", "country"]},
    )

    class Meta:
        abstract = True

    @property
    def verband_label(self):
        """Display label for `verband`: ``Bund`` for the country-level region,
        the bare Bundesland name otherwise (instead of GeoRegion's verbose
        ``__str__``)."""
        if self.verband is None:
            return ""
        return "Bund" if self.verband.kind == "country" else self.verband.name

    @cached_property
    def wikidata_url(self):
        if self.wikidata_id:
            return f"https://www.wikidata.org/wiki/{self.wikidata_id}"
        return None

    @cached_property
    def wikipedia_redirect_url(self):
        if self.wikidata_id:
            return f"https://www.wikidata.org/wiki/Special:GoToLinkedPage/de/{self.wikidata_id}"
        return ""


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

    class Meta:
        verbose_name = _("person")
        verbose_name_plural = _("persons")

    def __str__(self):
        return f"{self.first_name} {self.last_name}".strip()

    @cached_property
    def aw_url(self):
        if self.aw_id:
            return f"https://www.abgeordnetenwatch.de/politician/{self.aw_id}"
        return None


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

    def __str__(self):
        return self.organization_name.strip()

    class Meta:
        verbose_name = _("organization")
        verbose_name_plural = _("organizations")


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

    def get_absolute_url(self):
        return reverse("evidencecollection:actor-detail", kwargs={"pk": self.pk})

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

    @property
    def political_position_label(self):
        if self.person_id is None:
            return ""
        position = self.person.political_positions.first()
        return f"{position.label} (Stand 24. Juni 2026)" if position else None


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
    `Affiliation` (person↔organization, synced with abgeordnetenwatch),
    this is curated data: the dump's free-text label, classified into a
    canonical `role` and an `institutional_level`.
    """

    person = models.ForeignKey(
        Person,
        on_delete=models.CASCADE,
        related_name="political_positions",
        verbose_name=_("person"),
    )
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
    comment = models.TextField(blank=True, default="", verbose_name=_("comment"))

    class Meta:
        verbose_name = _("political position")
        verbose_name_plural = _("political positions")

    def __str__(self):
        return f"{self.person} - {self.label}"


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


@dataclass(frozen=True)
class TextSegment:
    """One piece of textual content belonging to an evidence source.

    `kind` is the semantic role (title / body / description / …), which the
    detail view uses to pick a per-segment style. `attribution` names the
    account a segment was lifted from, and is set only on a repost.
    """

    kind: str
    text: str
    attribution: str = ""


@dataclass
class TextSegmentGroup:
    """The structured text of an evidence source: the canonical shape sources
    expose, and what the detail view renders directly.

    `segments` holds the source's own authored components (title, body and
    description), shown merged into a single "Post text" / "Video description"
    block. `repost` carries the reposted source's body segment, rendered
    indented inside the block and labelled with its ``attribution``. Nesting
    encodes the provenance, so no segment needs to be marked as redistributed.
    """

    heading: str
    segments: list[TextSegment]
    repost: "TextSegment | None" = None

    def flat_segments(self) -> list[TextSegment]:
        """The group's segments in display order, own text first, repost last.

        For consumers that only tokenise the text (search, topics) and so have
        no use for the nesting.
        """
        return [*self.segments, *([self.repost] if self.repost else [])]


class EvidenceSource:
    """
    Uniform accessor surface for models attachable to an Evidence as a source.

    Subclasses expose `url` (model field) and implement `publication_date` and
    `text_block` so callers don't branch on source type.
    """

    url: str

    @property
    def publication_date(self):
        raise NotImplementedError

    def text_block(self) -> "TextSegmentGroup | None":
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
    `transcription` is kept verbatim as a backup copy but is deliberately not
    surfaced in display, search or topic modelling.
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
        help_text=_("Full verbatim video transcript; kept as backup only."),
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
            "Post whose content this post redistributes (repost, quote, forward, …)."
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
        # it. Drives the "Video description" vs "Post text" heading.
        return bool(self.video_source_path)

    def _own_text_segments(self) -> list[TextSegment]:
        # This post's own authored text, excluding anything redistributed. Title,
        # body and description all ride along, searched and topic-modelled. The
        # order below is the only place segment order is declared; display,
        # search and topics all take it from here.
        segments = []
        for kind, value in (
            ("title", self.title),
            ("body", self.text),
            ("description", self.description),
        ):
            if value and value.strip():
                segments.append(TextSegment(kind, value.strip()))
        return segments

    def _repost_segment(self) -> TextSegment | None:
        # A redistributed post carries only its body text (stubs store nothing
        # else), so lift that single segment rather than looping its components.
        if not self.redistributes_id or not self.redistributes.text.strip():
            return None
        return TextSegment(
            "body",
            self.redistributes.text.strip(),
            attribution=str(self.redistributes.account),
        )

    def text_block(self) -> TextSegmentGroup | None:
        segments = self._own_text_segments()
        repost = self._repost_segment()
        if not segments and repost is None:
            return None
        heading = _("Video description") if self.is_video else _("Post text")
        return TextSegmentGroup(heading, segments, repost)

    @property
    def publication_date(self):
        return self.posted_at.date() if self.posted_at else None

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


# compiled callable for enabled global rules; None = stale
_GLOBAL_REDACTOR = None


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


def scoped_redaction_rules(post) -> list["RedactionRule"]:
    """The enabled rules scoped to `post`.

    Resolved once per post and passed into `apply_redactions` for each of its
    segments, so a post's rules cost one query rather than one per segment.
    """
    return [rule for rule in post.redaction_rules.all() if rule.enabled]


def apply_redactions(text: str, scoped_rules=()) -> str:
    """Mask redacted terms in `text`: global rules, then the given scoped rules.

    Applied where text leaves the system — the detail page, the export and
    `search_text` — never to `topic_text`, whose fit emits only coordinates.
    Display and export redact read-time, so a rule takes effect there at once;
    the search index freezes the masked form when it is written, so it takes a
    re-index to catch up.
    """
    if not text:
        return text
    text = _get_global_redactor()(text)
    for rule in scoped_rules:
        text = rule.apply(text)
    return text


class RedactionRule(models.Model):
    """A term→placeholder substitution masking sensitive text (slurs, deadnames).

    Applied read-time by `apply_redactions` over assembled text, so the raw
    imported text is never mutated. A rule is *global* when it lists no `posts`
    (applied to every post, for terms that are always sensitive) or *scoped* to
    the `posts` it lists (the context-dependent cases, shareable across several
    posts). The search index freezes the masked form when it is written, so
    editing a rule takes a re-index to take effect there.
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
    # rule change only reaches the already-written search index after a re-index.
    invalidate_global_redactor()


@receiver(m2m_changed, sender=RedactionRule.posts.through)
def _invalidate_redactor_on_scope_change(sender, **kwargs):
    # Adding/removing posts can flip a rule between global and scoped, so the
    # global set may have changed — invalidate the cache.
    invalidate_global_redactor()


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


def _strip_tag_marker(match: "re.Match") -> str:
    return match.group(1).replace("_", " ")


def _clean_topic_text(text: str) -> str:
    # URLs first, so a handle embedded in a URL path is removed with the URL
    # rather than being half-normalised by the tag pass.
    text = _TOPIC_URL_RE.sub(" ", text)
    text = _TOPIC_TAG_RE.sub(_strip_tag_marker, text)
    # Collapse only the horizontal whitespace the substitutions leave behind;
    # the `\n\n` separators between text segments are load-bearing, so newlines
    # are preserved. Strip per line, or a substitution at the start or end of a
    # segment leaves its space stranded against a separator.
    text = re.sub(r"[^\S\n]+", " ", text)
    text = "\n".join(line.strip() for line in text.split("\n"))
    # A segment cleaned away in full — a body that was nothing but a link —
    # leaves a run of blank lines where it stood.
    return re.sub(r"\n{3,}", "\n\n", text).strip()


class Evidence(TrackableModel):
    # Stable public identifier used in the evidence URL (see `make_evidence_slug`).
    # Derived from the source and set once in `save()`; never changed afterwards,
    # because partners derive the same value to link into our data.
    slug = models.SlugField(
        max_length=EVIDENCE_SLUG_LENGTH, unique=True, verbose_name=_("slug")
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
    documentation_date = models.DateField(
        null=True, blank=True, verbose_name=_("documentation date")
    )

    # Populated by the `fit_topic_coords` management command, which embeds
    # `topic_text` (the assembled source text). topic_x/topic_y are the
    # per-evidence 2D UMAP coordinates used by the cloud view; topic_fit_at is
    # the "is fitted" gate (null = not yet fitted, or no usable text).
    topic_x = models.FloatField(null=True, blank=True, verbose_name=_("topic x"))
    topic_y = models.FloatField(null=True, blank=True, verbose_name=_("topic y"))
    topic_fit_at = models.DateTimeField(
        null=True, blank=True, verbose_name=_("topic fitted at")
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
        return self.title

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
    def title(self) -> str:
        """
        Return a human-readable label built from the public slug plus the originators
        and publication date. Empty parts are dropped so it stays clean.
        """
        parts = [f"{_('Evidence')} {self.slug}"]

        # `originator_actors` hits the M2M table, which requires a saved pk.
        if self.pk:
            originators = ", ".join(actor.name for actor in self.originator_actors)
            if originators:
                parts.append(originators)

        date = self.source.publication_date if self.source is not None else None
        if date:
            parts.append(str(date))

        return " · ".join(part for part in parts if part)

    @property
    def redacted_text_block(self) -> "TextSegmentGroup | None":
        """The source's structured text with masked terms replaced, for display
        and export.

        Prefetch `social_media_post__redaction_rules`, or the scoped rules cost a
        query per evidence.
        """
        source = self.source
        block = source.text_block() if source is not None else None
        if block is None:
            return None

        # Resolved once for the whole block, not per segment.
        scoped_rules = scoped_redaction_rules(source)

        def redact(seg: TextSegment) -> TextSegment:
            return replace(seg, text=apply_redactions(seg.text, scoped_rules))

        return TextSegmentGroup(
            block.heading,
            [redact(seg) for seg in block.segments],
            redact(block.repost) if block.repost else None,
        )

    @property
    def text_segments(self) -> list["TextSegment"]:
        # Flattened view of the source's text, for consumers that don't care
        # about the nesting.
        source = self.source
        block = source.text_block() if source is not None else None
        return block.flat_segments() if block is not None else []

    @cached_property
    def citation_segments(self) -> list["TextSegment"]:
        """The report's prose about this evidence, one segment per mention.

        A quote filed under several footnotes appears once. Prefetch `mentions`,
        or each evidence costs a query.
        """
        segments, seen = [], set()
        for mention in self.mentions.all():
            text = mention.citation.strip()
            if not text or text in seen:
                continue
            seen.add(text)
            segments.append(TextSegment("citation", text))
        return segments

    @property
    def all_segments(self) -> list["TextSegment"]:
        """Every text the evidence carries: the citations, then the post's own.

        Citations lead because they hold the essence of the evidence, and the
        embedding model truncates to a fixed token window, so whatever trails is
        cut away first. They may restate the post; for videos they usually come
        from the transcript, which the post itself does not carry.
        """
        return [*self.citation_segments, *self.text_segments]

    @property
    def search_text(self) -> str:
        """Fed to Elasticsearch, and redacted.

        Elasticsearch tokenises everything, so only the content matters here, not
        the order.
        """
        scoped_rules = scoped_redaction_rules(self.source) if self.source else ()
        return "\n\n".join(
            apply_redactions(s.text, scoped_rules) for s in self.all_segments
        )

    @property
    def topic_text(self) -> str:
        """Input to the embedding model, and *not* redacted.

        `_clean_topic_text` drops what only wastes the token window: URLs, and
        the @ / # markers on mentions and hashtags.
        """
        return _clean_topic_text("\n\n".join(s.text for s in self.all_segments))

    @cached_property
    def originator_actors(self):
        # Reads from the `originators` prefetch (one query for the whole page)
        # rather than firing a fresh SELECT per card. Falls back to a query if
        # the caller did not prefetch.
        return list(self.originators.all())

    def get_absolute_url(self):
        return reverse("evidencecollection:evidence-detail", kwargs={"slug": self.slug})


class EvidenceMention(models.Model):
    evidence = models.ForeignKey(
        Evidence,
        on_delete=models.CASCADE,
        related_name="mentions",
        verbose_name=_("evidence"),
    )
    # Which originator this specific mention is attributed to. An evidence can
    # have several originators (e.g. two speakers in one video); each is grouped
    # under its own footnotes/quotes in the source, so the mention records who
    # said what. Always set: a mention is only created from a post grouped under
    # a resolved actor. PROTECT so deleting an actor can't silently destroy the
    # footnote/quote data attributed to them.
    originator = models.ForeignKey(
        Actor,
        on_delete=models.PROTECT,
        related_name="originated_mentions",
        verbose_name=_("originator"),
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
    # The curator's relevant quote for this footnote (= source `fliesstext`).
    citation = models.TextField(blank=True, default="", verbose_name=_("citation"))
    # Public URL of the chapter this mention is filed under in the online report
    # (= source `report_urls`, row-parallel to the mention rows). Links a mention
    # back to its page on the published report; blank when the dump carried none.
    report_url = models.URLField(
        max_length=500, blank=True, default="", verbose_name=_("report URL")
    )
    # Video-excerpt fields (= source `video_timestamp`), set only for mentions of
    # a video post. `start`/`end` locate the excerpt in the video.
    start = models.DurationField(null=True, blank=True, verbose_name=_("start"))
    end = models.DurationField(null=True, blank=True, verbose_name=_("end"))

    class Meta:
        verbose_name = _("evidence mention")
        verbose_name_plural = _("evidence mentions")
        ordering = ["footnote"]
        constraints = [
            models.UniqueConstraint(
                fields=["evidence", "footnote"],
                name="unique_footnote_per_evidence",
            ),
        ]

    def __str__(self):
        return f"{self.evidence} ({self.footnote})"

    @cached_property
    def redacted_citation(self) -> str:
        """The curator's quote with masked terms removed.

        The rules are scoped to the post the quote was taken from, so they are
        reached through the evidence. Prefetch `social_media_post__redaction_rules`
        when rendering several mentions, or each one costs a rules query.
        """
        source = self.evidence.source
        if source is None:
            return self.citation
        return apply_redactions(self.citation, scoped_redaction_rules(source))

    def exclude_from_serialization(self):
        return ["id"]


class Chapter(MP_Node):
    """A node in the chapter hierarchy of the underlying report.

    The tree is materialised during the JSON import from each evidence mention's
    ``topic`` path, a root-to-leaf list of theme labels. A node's identity is the
    full path of labels leading to it, so the same label under different parents
    yields distinct nodes.

    ``is_main_topic`` marks the leaf of each imported path (i.e. the chapter that
    names the specific thematic topic an evidence is filed under); a node can be
    both a main topic for one evidence and an intermediate node for another.
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
    # NocoDB import/export was removed, but the choice is kept so historical
    # ImportExportRun records that reference it still display correctly.
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


class TopicCloudCMSPlugin(CMSPlugin):
    class Meta:
        verbose_name = _("topic cloud")
