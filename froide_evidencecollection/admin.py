import json

from django import forms
from django.conf import settings
from django.contrib import admin
from django.utils.safestring import mark_safe
from django.utils.translation import gettext_lazy as _

from .models import (
    Actor,
    Affiliation,
    Attachment,
    Category,
    Document,
    Election,
    Evidence,
    EvidenceActorRelation,
    EvidenceActorRelationRole,
    EvidenceMention,
    EvidenceRelation,
    EvidenceRelationRole,
    ImportExportRun,
    LegislativePeriod,
    Organization,
    Parliament,
    Person,
    Role,
    SocialMediaAccount,
    SocialMediaPost,
)
from .utils import selectable_regions


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
    fields = ["platform", "username", "display_name", "is_verified", "follower_count"]
    readonly_fields = fields


@admin.register(SocialMediaAccount)
class SocialMediaAccountAdmin(ReadOnlyAdmin):
    list_display = [
        "actor",
        "platform",
        "username",
        "display_name",
        "is_verified",
        "follower_count",
        "collected_at",
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
        "references",
        "reference_type",
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
        "transcription",
        "view_count",
        "like_count",
        "comment_count",
        "share_count",
        "reactions",
        "reply_to",
        "references",
        "reference_type",
        "user_snapshot",
        "raw",
    ]


@admin.register(Document)
class DocumentAdmin(ReadOnlyAdmin):
    list_display = ["id", "title", "issuer", "published_at"]
    search_fields = ["title", "url"]
    readonly_fields = [
        "evidence",
        "title",
        "file",
        "url",
        "issuer",
        "published_at",
        "text",
        "collected_at",
    ]


@admin.register(EvidenceActorRelationRole)
class EvidenceActorRelationRoleAdmin(admin.ModelAdmin):
    list_display = ["name", "description"]
    search_fields = ["name"]


@admin.register(EvidenceRelationRole)
class EvidenceRelationRoleAdmin(admin.ModelAdmin):
    list_display = ["name", "description"]
    search_fields = ["name"]


class EvidenceActorRelationInline(admin.TabularInline):
    model = EvidenceActorRelation
    extra = 0
    fields = ["actor", "role"]


class EvidenceOutgoingRelationInline(admin.TabularInline):
    model = EvidenceRelation
    fk_name = "from_evidence"
    extra = 0
    fields = ["to_evidence", "role"]
    verbose_name = _("outgoing relation")
    verbose_name_plural = _("outgoing relations")


class EvidenceIncomingRelationInline(admin.TabularInline):
    model = EvidenceRelation
    fk_name = "to_evidence"
    extra = 0
    fields = ["from_evidence", "role"]
    verbose_name = _("incoming relation")
    verbose_name_plural = _("incoming relations")


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


@admin.register(Actor)
class ActorAdmin(ReadOnlyAdmin):
    inlines = [SocialMediaAccountInline]
    list_display = ["name", "external_id"]
    search_fields = ["name"]


@admin.register(Person)
class PersonAdmin(SyncableMixin, ReadOnlyAdmin):
    inlines = [AffiliationInline]
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
        "is_synced",
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
        "is_synced",
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
    list_filter = ["is_synced", "synced_at"]
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
        "is_synced",
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


class AttachmentInline(admin.TabularInline):
    model = Attachment
    extra = 0


class EvidenceMentionInline(admin.TabularInline):
    model = EvidenceMention
    extra = 0
    fields = ["category", "page"]
    readonly_fields = fields


class CategoryMentionInline(admin.TabularInline):
    model = EvidenceMention
    fk_name = "category"
    extra = 0
    fields = ["evidence", "page"]
    readonly_fields = fields


@admin.register(Category)
class CategoryAdmin(ReadOnlyAdmin):
    inlines = [CategoryMentionInline]
    list_display = ["name"]
    search_fields = ["name"]


@admin.register(Evidence)
class EvidenceAdmin(ReadOnlyAdmin):
    inlines = [
        AttachmentInline,
        EvidenceMentionInline,
        EvidenceActorRelationInline,
        EvidenceOutgoingRelationInline,
        EvidenceIncomingRelationInline,
    ]
    list_display = [
        "external_id",
        "title",
        "evidence_type",
        "originator_list",
    ]
    filter_horizontal = ["collections"]
    list_filter = ["collections", "evidence_type"]
    search_fields = ["citation", "description"]

    def originator_list(self, obj):
        return ", ".join([originator.name for originator in obj.originators])

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
