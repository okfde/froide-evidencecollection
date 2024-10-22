import csv
import io

from django.http import HttpResponse
from django.utils.translation import gettext as _
from django.views.generic import DetailView

import openpyxl

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


class EvidenceExportView(EvidenceListView):
    EXPORT_FIELDS = [
        "id",
        "date",
        "source__url",
        "source__public_body__id",
        "source__public_body__name",
        "source__document_number",
        "type__name",
        "area__name",
        "person__name",
        "quality__name",
        "description",
    ]
    FORMATS = ["csv", "xlsx"]

    def get_rows(self):
        self.object_list = self.get_queryset()
        self.object_list.update_query()
        return (
            self.object_list.to_queryset()
            .prefetch_related(*self.EXPORT_FIELDS)
            .values(*self.EXPORT_FIELDS)
        )

    def get(self, request, *args, **kwargs):
        format = request.GET.get("format", "csv")
        if format not in self.FORMATS:
            format = "csv"

        rows = self.get_rows()
        content = getattr(self, f"generate_{format}")(rows)

        response = HttpResponse(content)
        response["Content-Disposition"] = f"attachment; filename=export.{format}"
        return response

    def generate_csv(self, rows):
        f = io.StringIO()
        writer = csv.DictWriter(f, fieldnames=self.EXPORT_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

        return f.getvalue().encode()

    def generate_xlsx(self, rows):
        wb = openpyxl.Workbook()
        ws = wb.active
        if ws is None:
            ws = wb.create_sheet()
        ws.append(self.EXPORT_FIELDS)
        for row in rows:
            ws.append([row.get(key) for key in self.EXPORT_FIELDS])
        f = io.BytesIO()
        wb.save(f)
        return f.getvalue()
