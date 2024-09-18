from django.utils.timezone import datetime
from django.utils.translation import gettext as _
from django.views.generic import DetailView

from froide.helper.breadcrumbs import BreadcrumbView

from .models import Evidence


class EvidenceMixin(BreadcrumbView):
    def get_breadcrumbs(self, context):
        if "request" in context:
            request = context["request"]

            title = request.current_page.get_title()
            url = request.current_page.get_absolute_url()
            return [(title, url)]

        return []


class EvidenceDetailView(EvidenceMixin, DetailView):
    slug_url_kwarg = "plan"
    template_name = "froide_evidencecollection/detail.html"

    def get_queryset(self):
        return Evidence.objects.filter(published_on__lt=datetime.now())

    def get_breadcrumbs(self, context):
        obj = self.get_object()

        breadcrumbs = super().get_breadcrumbs(context)

        return breadcrumbs + [
            (_("Evidence #%s" % obj.pk), obj.get_absolute_url()),
        ]
