from django import forms
from django.conf import settings
from django.contrib import admin
from django.utils.safestring import mark_safe

from .models import (
    AffiliationNew,
    AttachmentNew,
    Collection,
    EvidenceNew,
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
    model = AffiliationNew
    extra = 0
    fields = ["organization", "role", "start_date_string", "end_date_string"]


@admin.register(Person)
class PersonAdmin(ReadOnlyAdmin):
    inlines = [AffiliationInline]
    list_display = [
        "last_name",
        "first_name",
        "also_known_as",
        "wikidata_link",
        "aw_link",
    ]
    fields = [
        "external_id",
        "title",
        "first_name",
        "last_name",
        "also_known_as",
        "wikidata_link",
        "aw_link",
        "status",
    ]
    readonly_fields = ["wikidata_link", "aw_link"]
    list_filter = [
        "affiliations__organization__institutional_level",
        "affiliations__role",
        "affiliations__organization",
    ]
    search_fields = ["first_name", "last_name", "also_known_as"]

    def wikidata_link(self, obj):
        if obj.wikidata_url:
            return mark_safe(
                f'<a href="{obj.wikidata_url}" target="_blank">{obj.wikidata_url}</a>'
            )
        return ""

    def aw_link(self, obj):
        if obj.aw_url:
            return mark_safe(f'<a href="{obj.aw_url}" target="_blank">{obj.aw_url}</a>')
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
    ]
    fields = [
        "external_id",
        "organization_name",
        "also_known_as",
        "wikidata_link",
        "institutional_level",
        "regions",
        "special_regions",
        "status",
    ]
    readonly_fields = ["wikidata_link"]
    filter_horizontal = ("regions",)
    list_filter = ["institutional_level", "affiliations__person"]
    search_fields = ["organization_name", "also_known_as"]

    def wikidata_link(self, obj):
        if obj.wikidata_url:
            return mark_safe(
                f'<a href="{obj.wikidata_url}" target="_blank">{obj.wikidata_url}</a>'
            )
        return ""

    def region_list(self, obj):
        return ", ".join([region.name for region in obj.regions.all()])


class AttachmentInline(admin.TabularInline):
    model = AttachmentNew
    extra = 0


@admin.register(EvidenceNew)
class EvidenceAdmin(ReadOnlyAdmin):
    inlines = [AttachmentInline]
    list_display = ["external_id", "title", "evidence_type", "originator_list"]
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


admin.site.register(Collection, ReadOnlyAdmin)
admin.site.register(Role, ReadOnlyAdmin)
