import json
import re

from django import forms
from django.conf import settings
from django.contrib import admin
from django.db.models import F, Q
from django.urls import reverse
from django.utils.html import format_html, format_html_join
from django.utils.safestring import mark_safe
from django.utils.translation import gettext_lazy as _

from .models import (
    Actor,
    Affiliation,
    Attachment,
    Category,
    Chapter,
    Election,
    Evidence,
    EvidenceMention,
    ImportExportRun,
    Keyword,
    KeywordGroup,
    LegislativePeriod,
    Organization,
    Parliament,
    Person,
    PoliticalPosition,
    PostImage,
    PostScreenshot,
    PostVideo,
    Role,
    SocialMediaAccount,
    SocialMediaPost,
    Topic,
    VideoExcerpt,
)
from .utils import selectable_regions


class SyncedListFilter(admin.SimpleListFilter):
    title = _("is synced")
    parameter_name = "is_synced"

    def lookups(self, request, model_admin):
        return [("1", _("Yes")), ("0", _("No"))]

    def queryset(self, request, queryset):
        synced = Q(synced_at__isnull=False) & Q(synced_at__gte=F("updated_at"))
        if self.value() == "1":
            return queryset.filter(synced)
        if self.value() == "0":
            return queryset.exclude(synced)
        return queryset


class SyncableMixin:
    def is_synced_display(self, obj):
        return obj.is_synced

    is_synced_display.boolean = True
    is_synced_display.short_description = _("is synced")


class ReadOnlyAdmin(admin.ModelAdmin):
    def get_readonly_fields(self, request, obj=None):
        if obj is None or settings.DEBUG:
            return self.readonly_fields
        else:
            return tuple(
                [field.name for field in obj._meta.fields]
                + [field.name for field in obj._meta.many_to_many]
                + list(self.readonly_fields)
            )

    def has_add_permission(self, request):
        return settings.DEBUG

    def has_delete_permission(self, request, obj=None):
        return settings.DEBUG


class SocialMediaAccountInline(admin.TabularInline):
    model = SocialMediaAccount
    extra = 0
    fields = [
        "platform",
        "platform_user_id",
        "username",
        "display_name",
        "is_verified",
        "follower_count",
    ]
    readonly_fields = fields


class SocialMediaPostInline(admin.TabularInline):
    model = SocialMediaPost
    extra = 0
    fields = ["post_link", "url", "posted_at", "redistributes"]
    readonly_fields = fields

    def post_link(self, obj):
        url = obj.get_admin_url()
        if url is None:
            return obj.platform_post_id
        return format_html('<a href="{}">{}</a>', url, obj.platform_post_id)

    post_link.short_description = _("platform post ID")


@admin.register(SocialMediaAccount)
class SocialMediaAccountAdmin(ReadOnlyAdmin):
    inlines = [SocialMediaPostInline]
    list_display = [
        "actor",
        "platform",
        "platform_user_id",
        "username",
        "display_name",
        "is_verified",
        "follower_count",
        "post_count",
        "redistributed_count",
    ]
    list_filter = ["platform", "is_verified"]
    search_fields = ["username", "display_name", "platform_user_id"]
    readonly_fields = [
        "actor",
        "platform",
        "username",
        "platform_user_id",
        "display_name",
        "description",
        "url",
        "is_verified",
        "follower_count",
        "collected_at",
    ]

    def get_queryset(self, request):
        return super().get_queryset(request).with_post_stats()

    @admin.display(description=_("number of posts"), ordering="post_count")
    def post_count(self, obj):
        return obj.post_count

    @admin.display(
        description=_("redistributed by other accounts"),
        ordering="redistributed_count",
    )
    def redistributed_count(self, obj):
        return obj.redistributed_count


class PostImageInline(admin.TabularInline):
    model = PostImage
    extra = 0
    fields = [
        "preview",
        "image",
        "source_path",
        "description",
        "is_related_to_text",
        "content_text",
        "content_text_override",
    ]
    # Everything is import-owned and read-only except `content_text_override`,
    # the curator's correction of the imported `content_text` (preserved across
    # re-imports). This is the one editable field on the otherwise read-only
    # post admin.
    readonly_fields = [f for f in fields if f != "content_text_override"]

    @admin.display(description=_("preview"))
    def preview(self, obj):
        # Render the image inline so editors can view it on the post page.
        if not obj.image:
            return _("(no file)")
        style = "max-height: 240px; max-width: 320px;"
        return format_html('<img src="{}" style="{}" />', obj.image.url, style)


class PostVideoInline(admin.TabularInline):
    model = PostVideo
    extra = 0
    fields = [
        "preview",
        "transcript_file",
        "source_path",
        "description",
        "excerpts_summary",
    ]
    # Fully import-owned. Excerpt text (and its curator override) lives on the
    # related VideoExcerpt rows — edited via their own admin, since Django can't
    # nest an inline within an inline; here they're shown read-only for context.
    readonly_fields = fields

    @admin.display(description=_("preview"))
    def preview(self, obj):
        # Render the video inline so editors can view it on the post page.
        # A transcript-only video carries no file.
        if not obj.file:
            return _("(no file)")
        style = "max-height: 240px; max-width: 320px;"
        return format_html(
            '<video src="{}" controls preload="metadata" style="{}"></video>',
            obj.file.url,
            style,
        )

    @admin.display(description=_("excerpts"))
    def excerpts_summary(self, obj):
        if obj.pk is None:
            return ""
        excerpts = obj.excerpts.all()
        if not excerpts:
            return _("(no excerpts)")
        return format_html_join(
            mark_safe("<br>"),
            "{}. {}",
            ((e.order, e.resolved_text) for e in excerpts),
        )


@admin.register(VideoExcerpt)
class VideoExcerptAdmin(ReadOnlyAdmin):
    list_display = ["video", "order", "resolved_text"]
    fields = ["video", "order", "start", "end", "text", "text_override"]
    # `text` is import-owned; `text_override` is the curator's correction
    # (preserved across re-imports), the one editable field here.
    readonly_fields = [f for f in fields if f != "text_override"]


class PostScreenshotInline(admin.TabularInline):
    model = PostScreenshot
    extra = 0
    fields = ["preview", "image", "source_path", "description"]
    # A screenshot is a fully import-owned archival file (provenance); nothing
    # here is curator-editable.
    readonly_fields = fields

    @admin.display(description=_("preview"))
    def preview(self, obj):
        # Render the screenshot inline so editors can view it on the post page.
        if not obj.image:
            return _("(no file)")
        style = "max-height: 240px; max-width: 320px;"
        return format_html('<img src="{}" style="{}" />', obj.image.url, style)


@admin.register(SocialMediaPost)
class SocialMediaPostAdmin(ReadOnlyAdmin):
    inlines = [PostImageInline, PostVideoInline, PostScreenshotInline]
    list_display = [
        "platform_post_id",
        "account",
        "posted_at",
        "view_count",
        "like_count",
        "comment_count",
        "reply_to",
        "redistributes",
    ]
    list_filter = ["account__platform"]
    search_fields = ["platform_post_id", "url", "text", "title", "caption"]
    readonly_fields = [
        "evidence",
        "account",
        "platform_post_id",
        "url",
        "posted_at",
        "edited_at",
        "text",
        "title",
        "description",
        "caption",
        "view_count",
        "like_count",
        "comment_count",
        "share_count",
        "reactions",
        "reply_to",
        "redistributes",
        "user_snapshot",
        "raw",
    ]


class AffiliationInline(admin.TabularInline):
    model = Affiliation
    extra = 0
    fields = [
        "organization",
        "role",
        "start_date_string",
        "end_date_string",
        "aw_link",
        "reference_url",
        "comment",
    ]
    readonly_fields = fields
    ordering = ("start_date_string",)

    def aw_link(self, obj):
        if obj.aw_url:
            return mark_safe(f'<a href="{obj.aw_url}" target="_blank">{obj.aw_id}</a>')
        return ""


class PoliticalPositionInline(admin.TabularInline):
    model = PoliticalPosition
    extra = 0
    fields = [
        "type",
        "label",
        "role",
        "institutional_level",
        "region",
        "organization",
        "start_date_display",
        "end_date_display",
    ]
    readonly_fields = fields
    ordering = ("start_date",)


@admin.register(Actor)
class ActorAdmin(ReadOnlyAdmin):
    inlines = [SocialMediaAccountInline]
    list_display = [
        "name",
        "external_id",
        "account_count",
        "post_count",
        "redistributed_count",
    ]
    search_fields = ["name"]

    def get_queryset(self, request):
        return super().get_queryset(request).with_account_stats()

    @admin.display(description=_("number of accounts"), ordering="account_count")
    def account_count(self, obj):
        return obj.account_count

    @admin.display(description=_("number of posts"), ordering="post_count")
    def post_count(self, obj):
        return obj.post_count

    @admin.display(
        description=_("redistributed by other actors"),
        ordering="redistributed_count",
    )
    def redistributed_count(self, obj):
        return obj.redistributed_count


@admin.register(Person)
class PersonAdmin(SyncableMixin, ReadOnlyAdmin):
    inlines = [AffiliationInline, PoliticalPositionInline]
    list_display = [
        "last_name",
        "first_name",
        "also_known_as",
        "wikidata_link",
        "aw_link",
        "synced_at",
        "is_synced_display",
    ]
    fields = [
        "external_id",
        "sync_uuid",
        "name_hash",
        "title",
        "first_name",
        "last_name",
        "also_known_as",
        "wikidata_link",
        "aw_link",
        "status",
        "created_at",
        "updated_at",
        "synced_at",
        "is_synced_display",
    ]
    readonly_fields = [
        "sync_uuid",
        "name_hash",
        "wikidata_link",
        "aw_link",
        "created_at",
        "updated_at",
        "synced_at",
        "is_synced_display",
    ]
    list_filter = [
        SyncedListFilter,
        "synced_at",
        "affiliations__organization__institutional_level",
        "affiliations__role",
        "affiliations__organization",
    ]
    search_fields = ["first_name", "last_name", "also_known_as"]

    def wikidata_link(self, obj):
        if obj.wikidata_url:
            return mark_safe(
                f'<a href="{obj.wikidata_url}" target="_blank">{obj.wikidata_id}</a>'
            )
        return ""

    def aw_link(self, obj):
        if obj.aw_url:
            return mark_safe(f'<a href="{obj.aw_url}" target="_blank">{obj.aw_id}</a>')
        return ""


class OrganizationAdminForm(forms.ModelForm):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if "regions" in self.fields:
            self.fields["regions"].queryset = selectable_regions()


@admin.register(Organization)
class OrganizationAdmin(SyncableMixin, ReadOnlyAdmin):
    form = OrganizationAdminForm
    list_display = [
        "organization_name",
        "also_known_as",
        "wikidata_link",
        "institutional_level",
        "region_list",
        "synced_at",
        "is_synced_display",
    ]
    fields = [
        "external_id",
        "sync_uuid",
        "organization_name",
        "also_known_as",
        "wikidata_link",
        "institutional_level",
        "regions",
        "special_regions",
        "status",
        "created_at",
        "updated_at",
        "synced_at",
        "is_synced_display",
    ]
    readonly_fields = [
        "sync_uuid",
        "wikidata_link",
        "created_at",
        "updated_at",
        "synced_at",
        "is_synced_display",
    ]
    filter_horizontal = ("regions",)
    list_filter = [
        SyncedListFilter,
        "synced_at",
        "institutional_level",
        "affiliations__person",
    ]
    search_fields = ["organization_name", "also_known_as"]

    def wikidata_link(self, obj):
        if obj.wikidata_url:
            return mark_safe(
                f'<a href="{obj.wikidata_url}" target="_blank">{obj.wikidata_id}</a>'
            )
        return ""

    def region_list(self, obj):
        return ", ".join([region.name for region in obj.regions.all()])


@admin.register(Role)
class RoleAdmin(SyncableMixin, ReadOnlyAdmin):
    list_display = [
        "name",
        "external_id",
        "sync_uuid",
        "synced_at",
        "is_synced_display",
    ]
    fields = [
        "external_id",
        "name",
        "sync_uuid",
        "created_at",
        "updated_at",
        "synced_at",
        "is_synced_display",
    ]
    readonly_fields = [
        "sync_uuid",
        "created_at",
        "updated_at",
        "synced_at",
        "is_synced_display",
    ]
    list_filter = [SyncedListFilter, "synced_at"]
    search_fields = ["name"]


@admin.register(Affiliation)
class AffiliationAdmin(SyncableMixin, ReadOnlyAdmin):
    list_display = [
        "person",
        "organization",
        "role",
        "start_date_string",
        "end_date_string",
        "synced_at",
        "is_synced_display",
    ]
    fields = [
        "external_id",
        "sync_uuid",
        "person",
        "organization",
        "role",
        "start_date_string",
        "end_date_string",
        "aw_link",
        "reference_url",
        "comment",
        "created_at",
        "updated_at",
        "synced_at",
        "is_synced_display",
    ]
    readonly_fields = [
        "sync_uuid",
        "aw_link",
        "created_at",
        "updated_at",
        "synced_at",
        "is_synced_display",
    ]
    list_filter = [
        SyncedListFilter,
        "synced_at",
        "organization__institutional_level",
        "role",
        "person",
        "organization",
    ]
    search_fields = [
        "person__first_name",
        "person__last_name",
        "organization__organization_name",
        "role__name",
    ]

    def aw_link(self, obj):
        if obj.aw_url:
            return mark_safe(f'<a href="{obj.aw_url}" target="_blank">{obj.aw_id}</a>')
        return ""


@admin.register(PoliticalPosition)
class PoliticalPositionAdmin(admin.ModelAdmin):
    list_display = [
        "person",
        "type",
        "label",
        "role",
        "institutional_level",
        "region",
        "start_date_display",
        "end_date_display",
    ]
    list_filter = ["type", "institutional_level", "region", "role"]
    search_fields = [
        "person__first_name",
        "person__last_name",
        "label",
        "role__name",
    ]
    raw_id_fields = ["person", "role", "organization", "region"]
    readonly_fields = [
        "created_at",
        "updated_at",
        "start_date_display",
        "end_date_display",
    ]

    @admin.display(description=_("start date"), ordering="start_date")
    def start_date_display(self, obj):
        return obj.start_date_display

    @admin.display(description=_("end date"), ordering="end_date")
    def end_date_display(self, obj):
        return obj.end_date_display


class AttachmentInline(admin.TabularInline):
    model = Attachment
    extra = 0


class EvidenceMentionInline(admin.TabularInline):
    model = EvidenceMention
    extra = 0
    fields = ["category", "footnote", "chapter", "chapter_structure", "citation"]
    readonly_fields = fields


class CategoryMentionInline(admin.TabularInline):
    model = EvidenceMention
    fk_name = "category"
    extra = 0
    fields = ["evidence", "footnote", "chapter", "chapter_structure", "citation"]
    readonly_fields = fields


@admin.register(Category)
class CategoryAdmin(ReadOnlyAdmin):
    inlines = [CategoryMentionInline]
    list_display = ["name"]
    search_fields = ["name"]


@admin.register(Chapter)
class ChapterAdmin(ReadOnlyAdmin):
    list_display = ["indented_label", "is_main_topic", "evidence_count"]
    list_filter = ["is_main_topic", "depth"]
    search_fields = ["custom_label"]
    readonly_fields = ["subsumed_evidences"]

    def get_queryset(self, request):
        # Order by materialised path so the tree reads top-down in the list.
        return super().get_queryset(request).order_by("path")

    def indented_label(self, obj):
        indent = (obj.depth - 1) * 2
        return format_html(
            '<span style="padding-left:{}em">{}</span>',
            indent,
            obj.custom_label,
        )

    indented_label.short_description = _("label")

    def evidence_count(self, obj):
        return obj.subsumed_evidences().count()

    evidence_count.short_description = _("subsumed evidences")

    def subsumed_evidences(self, obj):
        if obj is None or obj.pk is None:
            return _("None")
        evidences = obj.subsumed_evidences().order_by("external_id")
        if not evidences:
            return _("None")
        return format_html_join(
            mark_safe("<br>"),
            '<a href="{}">{}</a>',
            (
                (
                    reverse(
                        "admin:froide_evidencecollection_evidence_change",
                        args=[evidence.pk],
                    ),
                    str(evidence),
                )
                for evidence in evidences
            ),
        )

    subsumed_evidences.short_description = _("subsumed evidences")


@admin.register(Evidence)
class EvidenceAdmin(ReadOnlyAdmin):
    inlines = [
        AttachmentInline,
        EvidenceMentionInline,
    ]
    list_display = [
        "external_id",
        "title",
        "evidence_type",
        "originator_list",
        "topic",
        "topic_reassigned",
    ]
    filter_horizontal = ["collections"]
    list_filter = ["collections", "evidence_type", "topic_reassigned"]
    search_fields = ["citation", "description"]

    def originator_list(self, obj):
        return ", ".join([originator.name for originator in obj.originators.all()])

    originator_list.short_description = _("originators")


@admin.register(Topic)
class TopicAdmin(admin.ModelAdmin):
    list_display = ["label", "size", "keywords"]
    # search_fields = ["name"]


@admin.register(KeywordGroup)
class KeywordGroupAdmin(admin.ModelAdmin):
    list_display = ["label", "keyword_count", "description"]
    search_fields = ["label"]

    @admin.display(description=_("keywords"))
    def keyword_count(self, obj):
        return obj.keywords.count()


_WHITESPACE_RE = re.compile(r"\s+")


def _surface_form_pattern(forms):
    """Compile a case-insensitive, whole-word pattern over `forms` (a keyword's
    surface variants), longest first so a multi-word form wins over its parts.
    Returns None when there's nothing to match."""
    forms = sorted({f.strip() for f in forms if f and f.strip()}, key=len, reverse=True)
    if not forms:
        return None
    # (?<!\w)…(?!\w) is a Unicode-aware word boundary (German umlauts/ß count as
    # word chars), so a form isn't highlighted inside a longer word.
    return re.compile(
        r"(?<!\w)(?:" + "|".join(re.escape(f) for f in forms) + r")(?!\w)",
        re.IGNORECASE,
    )


def _matching_snippets(text, pattern, context=60, limit=5):
    """Up to `limit` safe-HTML snippets of `text` around each match of
    `pattern`, the matched span wrapped in <mark>. Surrounding whitespace is
    collapsed so multi-line source text reads as a single line."""
    snippets = []
    for match in pattern.finditer(text):
        start = max(0, match.start() - context)
        end = min(len(text), match.end() + context)
        snippets.append(
            format_html(
                "{}{}<mark>{}</mark>{}{}",
                "…" if start > 0 else "",
                _WHITESPACE_RE.sub(" ", text[start : match.start()]),
                _WHITESPACE_RE.sub(" ", match.group(0)),
                _WHITESPACE_RE.sub(" ", text[match.end() : end]),
                "…" if end < len(text) else "",
            )
        )
        if len(snippets) >= limit:
            break
    return snippets


@admin.register(Keyword)
class KeywordAdmin(admin.ModelAdmin):
    # custom_label / enabled / group are curator-editable straight from the
    # changelist for fast bulk curation; the derived fields are read-only since
    # the fit overwrites them. `group` uses autocomplete so assignment scales.
    list_display = [
        "display_label",
        "lemma",
        "custom_label",
        "group",
        "enabled",
        "df",
        "fit_at",
    ]
    list_editable = ["custom_label", "group", "enabled"]
    list_filter = ["enabled", "group"]
    search_fields = ["label", "custom_label", "lemma"]
    autocomplete_fields = ["group"]
    readonly_fields = [
        "lemma",
        "label",
        "surface_forms",
        "df",
        "fit_at",
        "related_evidence",
    ]
    ordering = ["-df", "label"]

    # Cap the related-evidence listing on the change form: the matching-snippet
    # extraction reads each evidence's assembled text (a few queries per row),
    # so an unbounded list would be slow for a high-df keyword.
    RELATED_EVIDENCE_LIMIT = 50
    # Snippets shown per evidence, across all its labelled text chunks.
    SNIPPETS_PER_EVIDENCE = 5

    @staticmethod
    def _labelled_chunks(ev):
        """Yield ``(label, text)`` for each named piece of an evidence's text,
        so a snippet can name where it came from. Citation/description are the
        evidence's own fields; the rest are the source's labelled segments
        (title, post text, transcript, …), tagged with the redistribution
        attribution when present."""
        if ev.citation:
            yield _("Citation"), ev.citation
        if ev.description:
            yield _("Description"), ev.description
        for seg in ev.text_segments:
            label = seg.label
            if getattr(seg, "attribution", ""):
                label = format_html("{} · {}", seg.label, seg.attribution)
            yield label, seg.text

    @admin.display(description=_("related evidence (matches in text)"))
    def related_evidence(self, obj):
        if obj is None or obj.pk is None:
            return "—"

        # Match the keyword's recorded surface forms; fall back to the lemma if
        # a row somehow has none. These are the variants that actually occurred
        # in the corpus, extracted from each evidence's `topic_text`.
        pattern = _surface_form_pattern(
            obj.surface_forms or {}
        ) or _surface_form_pattern([obj.lemma])

        total = obj.evidences.count()
        evidences = obj.evidences.select_related(
            "social_media_post__account",
        ).prefetch_related(
            "social_media_post__images",
            "social_media_post__videos__excerpts",
            "mentions__category",
        )[: self.RELATED_EVIDENCE_LIMIT]

        items = []
        for ev in evidences:
            # Match within each labelled chunk separately, so every snippet can
            # be prefixed with the name of the text it came from.
            snippet_items = []
            for label, chunk_text in self._labelled_chunks(ev):
                if not pattern or len(snippet_items) >= self.SNIPPETS_PER_EVIDENCE:
                    break
                remaining = self.SNIPPETS_PER_EVIDENCE - len(snippet_items)
                for snip in _matching_snippets(chunk_text, pattern, limit=remaining):
                    snippet_items.append(
                        format_html(
                            '<li><span style="color:#555;font-weight:600;">'
                            "{}:</span> {}</li>",
                            label,
                            snip,
                        )
                    )
            url = reverse(
                "admin:froide_evidencecollection_evidence_change", args=[ev.pk]
            )
            if snippet_items:
                body = format_html(
                    '<ul style="margin:.25rem 0 .5rem 1rem;">{}</ul>',
                    format_html_join("", "{}", ((s,) for s in snippet_items)),
                )
            else:
                body = format_html(
                    '<p style="margin:.25rem 0 .5rem 1rem;color:#777;">{}</p>',
                    _("Linked, but no literal surface-form match in the text."),
                )
            items.append(
                format_html(
                    '<li style="margin-bottom:.75rem;"><a href="{}">{}</a>{}</li>',
                    url,
                    str(ev),
                    body,
                )
            )

        if not items:
            return _("No related evidence.")

        header = (
            format_html(
                "<p>{}</p>",
                _("Showing %(shown)d of %(total)d (matches highlighted).")
                % {"shown": len(items), "total": total},
            )
            if total > len(items)
            else format_html(
                "<p>{}</p>",
                _("%(total)d related (matches highlighted).") % {"total": total},
            )
        )
        return format_html(
            '{}<ul style="list-style:none;padding-left:0;">{}</ul>',
            header,
            format_html_join("", "{}", ((i,) for i in items)),
        )


@admin.register(ImportExportRun)
class ImportExportRunAdmin(ReadOnlyAdmin):
    list_display = [
        "id",
        "source",
        "target",
        "operation",
        "started_at",
        "finished_at",
        "success",
    ]
    fields = [
        "source",
        "target",
        "operation",
        "started_at",
        "finished_at",
        "success",
        "pretty_changes",
        "notes",
    ]
    readonly_fields = fields
    list_filter = ["operation", "source", "target", "success"]
    date_hierarchy = "started_at"

    def pretty_changes(self, obj):
        if not obj.changes:
            return "-"
        pretty = json.dumps(obj.changes, indent=4, ensure_ascii=False)
        return mark_safe(f"<pre>{pretty}</pre>")

    pretty_changes.short_description = _("changes")


@admin.register(Parliament)
class ParliamentAdmin(ReadOnlyAdmin):
    list_display = ["name", "aw_id", "fraction"]


@admin.register(Election)
class ElectionAdmin(ReadOnlyAdmin):
    list_display = ["name", "aw_id", "start_date", "end_date"]
    search_fields = ["name"]


@admin.register(LegislativePeriod)
class LegislativePeriodAdmin(admin.ModelAdmin):
    list_display = ["name", "aw_id", "start_date", "end_date"]
    search_fields = ["name"]
