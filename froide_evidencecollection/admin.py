from django.contrib import admin
from .models import Evidence, Source, Person


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
        return False

    def has_delete_permission(self, request, obj=None):
        return False


admin.site.register(Evidence, ReadOnlyAdmin)
admin.site.register(Source, ReadOnlyAdmin)
admin.site.register(Person, ReadOnlyAdmin)
