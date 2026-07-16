import json

from django.conf import settings
from django.contrib import admin
from django.urls import reverse
from django.utils.html import format_html, format_html_join
from django.utils.safestring import mark_safe
from django.utils.translation import gettext_lazy as _

from .models import (
    Actor,
    Affiliation,
    Chapter,
    Election,
    Evidence,
    EvidenceMention,
    ImportExportRun,
    LegislativePeriod,
    Organization,
    Parliament,
    Person,
    PoliticalPosition,
    RedactionRule,
    Role,
    SocialMediaAccount,
    SocialMediaPost,
)


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
        "role",
        "institutional_level",
    ]
    readonly_fields = fields


@admin.register(Person)
class PersonAdmin(ReadOnlyAdmin):
    inlines = [AffiliationInline, PoliticalPositionInline]
    list_display = [
        "last_name",
        "first_name",
        "also_known_as",
        "wikidata_link",
        "aw_link",
    ]
    fields = [
        "sync_uuid",
        "title",
        "first_name",
        "last_name",
        "also_known_as",
        "wikidata_link",
        "aw_link",
        "verband_display",
        "political_position_label",
        "political_position_checked_at",
        "created_at",
        "updated_at",
    ]
    readonly_fields = [
        "sync_uuid",
        "wikidata_link",
        "aw_link",
        "verband_display",
        "created_at",
        "updated_at",
    ]
    list_filter = [
        "political_positions__institutional_level",
        "political_positions__role",
    ]
    search_fields = ["first_name", "last_name", "also_known_as"]

    @admin.display(description=_("Verband"))
    def verband_display(self, obj):
        return obj.verband_label

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


@admin.register(Organization)
class OrganizationAdmin(ReadOnlyAdmin):
    list_display = [
        "organization_name",
        "also_known_as",
        "wikidata_link",
        "institutional_level",
    ]
    fields = [
        "sync_uuid",
        "organization_name",
        "also_known_as",
        "wikidata_link",
        "institutional_level",
        "verband_display",
        "created_at",
        "updated_at",
    ]
    readonly_fields = [
        "sync_uuid",
        "wikidata_link",
        "verband_display",
        "created_at",
        "updated_at",
    ]
    list_filter = [
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

    def get_queryset(self, request):
        # `institutional_level` shows in the changelist; select it to avoid an N+1.
        return super().get_queryset(request).select_related("institutional_level")

    @admin.display(description=_("Verband"))
    def verband_display(self, obj):
        return obj.verband_label


@admin.register(Role)
class RoleAdmin(ReadOnlyAdmin):
    list_display = [
        "name",
        "sync_uuid",
    ]
    fields = [
        "name",
        "sync_uuid",
        "created_at",
        "updated_at",
    ]
    readonly_fields = [
        "sync_uuid",
        "created_at",
        "updated_at",
    ]
    search_fields = ["name"]


@admin.register(Affiliation)
class AffiliationAdmin(ReadOnlyAdmin):
    list_display = [
        "person",
        "organization",
        "role",
        "start_date_string",
        "end_date_string",
    ]
    fields = [
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
    ]
    readonly_fields = [
        "sync_uuid",
        "aw_link",
        "created_at",
        "updated_at",
    ]
    list_filter = [
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
        "role",
        "institutional_level",
    ]
    list_filter = ["institutional_level", "role"]
    search_fields = [
        "person__first_name",
        "person__last_name",
        "role__name",
    ]
    raw_id_fields = ["person", "role"]
    readonly_fields = [
        "created_at",
        "updated_at",
    ]


class EvidenceMentionInline(admin.TabularInline):
    model = EvidenceMention
    extra = 0
    fields = [
        "footnote",
        "chapter",
        "chapter_structure",
        "citation",
        "start",
        "end",
        "report_url",
    ]
    readonly_fields = fields


@admin.register(Chapter)
class ChapterAdmin(ReadOnlyAdmin):
    # Chapters are seeded by the JSON import, but curators maintain them by hand
    # on prod: the label and the main-topic flag (also editable straight from
    # the changelist via list_editable). Only treebeard's structural fields stay
    # locked so the materialised tree can't be corrupted from the form.
    list_display = ["indented_label", "is_main_topic", "evidence_count"]
    list_editable = ["is_main_topic"]
    list_filter = ["is_main_topic", "depth"]
    search_fields = ["custom_label"]
    readonly_fields = ["subsumed_evidences"]

    # treebeard internals: never hand-editable, regardless of DEBUG.
    structural_fields = ("path", "depth", "numchild")

    def get_readonly_fields(self, request, obj=None):
        # Override ReadOnlyAdmin's blanket prod lock: keep only treebeard's
        # structural fields (plus the computed `subsumed_evidences`) read-only,
        # so `custom_label` and `is_main_topic` stay editable on prod.
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
        EvidenceMentionInline,
    ]
    list_display = [
        "slug",
        "title",
        "originator_list",
    ]

    def originator_list(self, obj):
        return ", ".join([originator.name for originator in obj.originators.all()])

    originator_list.short_description = _("originators")


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
