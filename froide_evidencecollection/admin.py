import json

from django import forms
from django.conf import settings
from django.contrib import admin
from django.utils.safestring import mark_safe
from django.utils.translation import gettext_lazy as _

from .models import (
    Affiliation,
    Attachment,
    Evidence,
    ImportExportRun,
    Organization,
    Person,
    Role,
)
from .utils import selectable_regions


class ReadOnlyAdmin(admin.ModelAdmin):
    def get_readonly_fields(self, request, obj=None):
        if obj is None or settings.DEBUG:
            return self.readonly_fields
        else:
            return tuple(
                [field.name for field in obj._meta.fields]
                + [field.name for field in obj._meta.many_to_many]
            )

    def has_add_permission(self, request):
        return settings.DEBUG

    def has_delete_permission(self, request, obj=None):
        return settings.DEBUG


class AffiliationInline(admin.TabularInline):
    model = Affiliation
    extra = 0
    fields = [
        "organization",
        "role",
        "start_date_string",
        "end_date_string",
        "aw_id",
        "reference_url",
        "comment",
    ]
    readonly_fields = fields


@admin.register(Person)
class PersonAdmin(ReadOnlyAdmin):
    inlines = [AffiliationInline]
    list_display = [
        "last_name",
        "first_name",
        "also_known_as",
        "wikidata_link",
        "aw_link",
        "created_at",
        "updated_at",
        "synced_at",
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
    ]
    readonly_fields = [
        "sync_uuid",
        "wikidata_link",
        "aw_link",
        "created_at",
        "updated_at",
        "synced_at",
    ]
    list_filter = [
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
        self.fields["regions"].queryset = selectable_regions()


@admin.register(Organization)
class OrganizationAdmin(ReadOnlyAdmin):
    form = OrganizationAdminForm
    list_display = [
        "organization_name",
        "also_known_as",
        "wikidata_link",
        "institutional_level",
        "region_list",
        "created_at",
        "updated_at",
        "synced_at",
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
    ]
    readonly_fields = [
        "sync_uuid",
        "wikidata_link",
        "created_at",
        "updated_at",
        "synced_at",
    ]
    filter_horizontal = ("regions",)
    list_filter = ["institutional_level", "affiliations__person"]
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
class RoleAdmin(ReadOnlyAdmin):
    list_display = ["name", "created_at", "updated_at", "synced_at"]
    fields = ["name", "sync_uuid", "created_at", "updated_at", "synced_at"]
    readonly_fields = ["sync_uuid", "created_at", "updated_at", "synced_at"]
    search_fields = ["name"]


class AttachmentInline(admin.TabularInline):
    model = Attachment
    extra = 0


@admin.register(Evidence)
class EvidenceAdmin(ReadOnlyAdmin):
    inlines = [AttachmentInline]
    list_display = [
        "external_id",
        "title",
        "evidence_type",
        "originator_list",
        "created_at",
        "updated_at",
    ]
    filter_horizontal = [
        "collections",
        "originators",
        "related_actors",
        "attribution_evidence",
        "attribution_problems",
    ]
    list_filter = ["collections", "evidence_type", "legal_assessment", "originators"]
    search_fields = ["citation", "description"]

    def originator_list(self, obj):
        return ", ".join([originator.name for originator in obj.originators.all()])


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
