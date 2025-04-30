from django.conf import settings
from django.contrib import admin

from .models import (
    Evidence,
    EvidenceArea,
    EvidenceType,
    Function,
    Institution,
    Person,
    PersonOrOrganization,
    Position,
    Quality,
    Source,
    Status,
)


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
    fields = ["institution", "function"]


class PersonOrOrganizationAdmin(ReadOnlyAdmin):
    inlines = [AffiliationInline]
    list_display = (
        "name",
        "is_active",
        "review_comment",
    )
    fields = (
        "name",
        "regions",
        "is_active",
        "review_comment",
    )

    list_filter = ["affiliations", "is_active"]


admin.site.register(Evidence, ReadOnlyAdmin)
admin.site.register(EvidenceArea, ReadOnlyAdmin)
admin.site.register(EvidenceType, ReadOnlyAdmin)
admin.site.register(Institution, ReadOnlyAdmin)
admin.site.register(Person, ReadOnlyAdmin)
admin.site.register(Position, ReadOnlyAdmin)
admin.site.register(Quality, ReadOnlyAdmin)
admin.site.register(Source, ReadOnlyAdmin)
admin.site.register(Status, ReadOnlyAdmin)
admin.site.register(PersonOrOrganization, PersonOrOrganizationAdmin)
admin.site.register(Function, ReadOnlyAdmin)
