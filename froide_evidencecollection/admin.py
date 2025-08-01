from django import forms
from django.conf import settings
from django.contrib import admin

from .models import (
    Attachment,
    AttributionBasis,
    Evidence,
    EvidenceCategory,
    EvidenceType,
    Group,
    Institution,
    PersonOrOrganization,
    Role,
    Source,
    SpreadLevel,
)
from .utils import selectable_regions


class ReadOnlyAdmin(admin.ModelAdmin):
    def get_readonly_fields(self, request, obj=None):
        if obj is None or settings.DEBUG:
            return ()
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
    model = PersonOrOrganization.affiliations.through
    extra = 0
    fields = ["institution", "role"]


class PersonOrOrganizationAdminForm(forms.ModelForm):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["regions"].queryset = selectable_regions()


class PersonOrOrganizationAdmin(ReadOnlyAdmin):
    form = PersonOrOrganizationAdminForm
    inlines = [AffiliationInline]
    list_display = (
        "name",
        "is_active",
    )
    fields = (
        "external_id",
        "name",
        "regions",
        "special_regions",
        "is_active",
    )

    list_filter = ["affiliations", "is_active"]
    filter_horizontal = ("regions",)
    search_fields = ["name"]


class AttachmentInline(admin.TabularInline):
    model = Attachment
    extra = 0


class SourceAdmin(ReadOnlyAdmin):
    inlines = [AttachmentInline]
    list_display = ("reference_value", "file_reference", "document_number")
    fields = (
        "external_id",
        "reference_value",
        "persons_or_organizations",
        "url",
        "attribution_bases",
        "file_reference",
        "document_number",
        "is_on_record",
        "recorded_by",
    )
    filter_horizontal = ("persons_or_organizations", "attribution_bases")
    search_fields = ("reference_value", "file_reference", "document_number")


class EvidenceAdmin(ReadOnlyAdmin):
    list_display = ("description", "date", "type")


class GroupAdmin(ReadOnlyAdmin):
    list_display = ("name",)
    fields = ("external_id", "name", "members")
    filter_horizontal = ("members",)
    search_fields = ("name",)


admin.site.register(AttributionBasis, ReadOnlyAdmin)
admin.site.register(Evidence, EvidenceAdmin)
admin.site.register(EvidenceType, ReadOnlyAdmin)
admin.site.register(EvidenceCategory, ReadOnlyAdmin)
admin.site.register(Group, GroupAdmin)
admin.site.register(Institution, ReadOnlyAdmin)
admin.site.register(PersonOrOrganization, PersonOrOrganizationAdmin)
admin.site.register(Role, ReadOnlyAdmin)
admin.site.register(Source, SourceAdmin)
admin.site.register(SpreadLevel, ReadOnlyAdmin)
