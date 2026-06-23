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
    LegislativePeriod,
    Organization,
    Parliament,
    Person,
    PoliticalPosition,
    RedactionRule,
    Role,
    SocialMediaAccount,
    SocialMediaPost,
    Theme,
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


@admin.register(SocialMediaPost)
class SocialMediaPostAdmin(ReadOnlyAdmin):
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
    search_fields = ["platform_post_id", "url", "text", "title", "transcription"]
    readonly_fields = [
        "account",
        "platform_post_id",
        "url",
        "posted_at",
        "edited_at",
        "text",
        "title",
        "description",
        "transcription",
        "screenshot_preview",
        "screenshot_source_path",
        "image_source_path",
        "image_description",
        "video_source_path",
        "view_count",
        "like_count",
        "comment_count",
        "share_count",
        "reactions",
        "reply_to",
        "redistributes",
        "user_snapshot",
    ]

    @admin.display(description=_("screenshot"))
    def screenshot_preview(self, obj):
        # Render the archival screenshot inline so editors can view it on the
        # post page. The only file-backed post media.
        if not obj.screenshot:
            return _("(no file)")
        style = "max-height: 240px; max-width: 320px;"
        return format_html('<img src="{}" style="{}" />', obj.screenshot.url, style)


@admin.register(RedactionRule)
class RedactionRuleAdmin(admin.ModelAdmin):
    list_display = ["pattern", "placeholder", "is_regex", "enabled", "scope"]
    list_filter = ["enabled", "is_regex"]
    search_fields = ["pattern", "placeholder"]
    autocomplete_fields = ["posts"]

    @admin.display(description=_("scope"))
    def scope(self, obj):
        # A rule with no posts is global; otherwise it is scoped to a count.
        count = obj.posts.count()
        return _("global") if count == 0 else _("%(n)d post(s)") % {"n": count}


@admin.register(Actor)
class ActorAdmin(ReadOnlyAdmin):
    inlines = [SocialMediaAccountInline]
    list_display = ["name"]


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
    fields = [
        "category",
        "footnote",
        "chapter",
        "chapter_structure",
        "citation",
        "start",
        "end",
        "raw_transcript",
    ]
    readonly_fields = fields


class CategoryMentionInline(admin.TabularInline):
    model = EvidenceMention
    fk_name = "category"
    extra = 0
    fields = [
        "evidence",
        "footnote",
        "chapter",
        "chapter_structure",
        "citation",
        "start",
        "end",
        "raw_transcript",
    ]
    readonly_fields = fields


@admin.register(Category)
class CategoryAdmin(ReadOnlyAdmin):
    inlines = [CategoryMentionInline]
    list_display = ["name"]
    search_fields = ["name"]


@admin.register(Chapter)
class ChapterAdmin(ReadOnlyAdmin):
    # Chapters are seeded by the JSON import, but curators maintain them by hand
    # on prod: the label, the main-topic flag, and the bulk `theme` assignment
    # ("everything in chapter X belongs to theme Y", also editable straight from
    # the changelist via list_editable). Only treebeard's structural fields stay
    # locked so the materialised tree can't be corrupted from the form.
    list_display = ["indented_label", "is_main_topic", "theme", "evidence_count"]
    list_editable = ["is_main_topic", "theme"]
    list_filter = ["is_main_topic", "depth", "theme"]
    search_fields = ["custom_label"]
    autocomplete_fields = ["theme"]
    readonly_fields = ["subsumed_evidences"]

    # treebeard internals: never hand-editable, regardless of DEBUG.
    structural_fields = ("path", "depth", "numchild")

    def get_readonly_fields(self, request, obj=None):
        # Override ReadOnlyAdmin's blanket prod lock: keep only treebeard's
        # structural fields (plus the computed `subsumed_evidences`) read-only,
        # so `custom_label`, `is_main_topic` and `theme` stay editable on prod.
        return tuple(self.structural_fields) + tuple(self.readonly_fields)

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
        evidences = obj.subsumed_evidences().order_by("pk")
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
        "slug",
        "title",
        "evidence_type",
        "originator_list",
    ]
    filter_horizontal = ["collections"]
    list_filter = ["collections", "evidence_type"]
    search_fields = ["citation", "description"]

    def originator_list(self, obj):
        return ", ".join([originator.name for originator in obj.originators.all()])

    originator_list.short_description = _("originators")


@admin.register(Theme)
class ThemeAdmin(admin.ModelAdmin):
    # The curator surface for the topic cloud's single browse bar. `evidences`
    # is the direct, chapter-free assignment; chapter-mapped evidence is added
    # in ChapterAdmin. `order` drives the chip sort, the palette and the dot's
    # dominant-theme tie-break, so it's editable inline.
    list_display = ["label", "order", "evidence_count"]
    list_editable = ["order"]
    search_fields = ["label"]
    filter_horizontal = ["evidences"]

    @admin.display(description=_("evidences"))
    def evidence_count(self, obj):
        return obj.evidence_queryset().count()


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
    # custom_label / enabled are curator-editable straight from the changelist
    # for fast bulk curation; the derived fields are read-only since the fit
    # overwrites them.
    list_display = [
        "display_label",
        "lemma",
        "custom_label",
        "enabled",
        "df",
        "fit_at",
    ]
    list_editable = ["custom_label", "enabled"]
    list_filter = ["enabled"]
    search_fields = ["label", "custom_label", "lemma"]
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
