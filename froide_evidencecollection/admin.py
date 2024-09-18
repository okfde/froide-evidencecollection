from django.conf import settings
from django.contrib import admin

from .models import (
    Evidence,
    EvidenceArea,
    EvidenceType,
    Institution,
    Person,
    Position,
    Quality,
    Source,
    Status,
)


class ReadOnlyAdmin(admin.ModelAdmin):
    def get_readonly_fields(self, request, obj=None):
        if obj is None:
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


admin.site.register(Evidence, ReadOnlyAdmin)
admin.site.register(EvidenceArea, ReadOnlyAdmin)
admin.site.register(EvidenceType, ReadOnlyAdmin)
admin.site.register(Institution, ReadOnlyAdmin)
admin.site.register(Person, ReadOnlyAdmin)
admin.site.register(Position, ReadOnlyAdmin)
admin.site.register(Quality, ReadOnlyAdmin)
admin.site.register(Source, ReadOnlyAdmin)
admin.site.register(Status, ReadOnlyAdmin)
