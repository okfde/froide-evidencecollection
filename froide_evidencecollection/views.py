from django.utils.translation import gettext as _
from django.views.generic import DetailView

from froide.helper.breadcrumbs import BreadcrumbView
from froide.helper.search.views import BaseSearchView
from froide_evidencecollection.documents import EvidenceDocument

from .filterset import EvidenceFilterSet
from .models import Evidence


class EvidenceMixin(BreadcrumbView):
    def get_breadcrumbs(self, context):
        if "request" in context:
            request = context["request"]

            title = request.current_page.get_title()
            url = request.current_page.get_absolute_url()
            return [(title, url)]

        return []

    def get_queryset(self):
        return Evidence.objects.filter(published_on__isnull=False)


class EvidenceDetailView(EvidenceMixin, DetailView):
    template_name = "froide_evidencecollection/detail.html"

    def get_breadcrumbs(self, context):
        obj = self.get_object()

        breadcrumbs = super().get_breadcrumbs(context)

        return breadcrumbs + [
            (_("Evidence #%s" % obj.pk), obj.get_absolute_url()),
        ]


class EvidenceListView(BaseSearchView):
    search_name = "evidence"
    template_name = "froide_evidencecollection/list.html"
    filterset = EvidenceFilterSet
    document = EvidenceDocument
    model = Evidence
    search_url_name = "evidencecollection:evidence-list"
