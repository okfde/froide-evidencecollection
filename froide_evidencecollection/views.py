import csv
import io

from django.conf import settings
from django.core.exceptions import BadRequest
from django.db.models import QuerySet
from django.http import Http404, HttpResponse
from django.template.loader import render_to_string
from django.utils.translation import gettext as _
from django.views.generic import DetailView

import openpyxl

from froide.foirequest.pdf_generator import get_wp
from froide.helper.breadcrumbs import BreadcrumbView
from froide.helper.search.views import BaseSearchView
from froide_evidencecollection.documents import EvidenceDocument

from .filterset import EvidenceFilterSet
from .models import Evidence


class EvidenceExporter:
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
        "title",
        "description",
    ]
    FORMATS = ["csv", "xlsx", "pdf"]

    def __init__(self, format):
        if format not in self.FORMATS:
            raise ValueError(f"format {format} is not supported")
        self.format = format

    def export(self, queryset):
        rows = self.get_rows(queryset)
        return getattr(self, f"generate_{self.format}")(rows)

    def get_rows(self, queryset):
        return queryset.prefetch_related(*self.EXPORT_FIELDS).values(
            *self.EXPORT_FIELDS
        )

    def generate_csv(self, rows):
        f = io.StringIO()
        writer = csv.DictWriter(f, fieldnames=self.EXPORT_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

        return f.getvalue().encode(), "text/csv"

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
        return (
            f.getvalue(),
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )

    def generate_pdf(self, rows):
        html = render_to_string(
            "froide_evidencecollection/pdf_export.html",
            context={"rows": rows, "SITE_NAME": settings.SITE_NAME},
        )
        wp = get_wp()
        if not wp:
            raise Exception("WeasyPrint needs to be installed")
        doc = wp.HTML(string=html)
        return doc.write_pdf(), "application/pdf"


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

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context["export_formats"] = EvidenceExporter.FORMATS
        return context


class ExportMixin:
    def get_export_queryset(self) -> QuerySet:
        raise NotImplementedError()

    def get(self, request, *args, **kwargs):
        format = request.GET.get("format", "pdf")
        if format not in EvidenceExporter.FORMATS:
            raise BadRequest("Invalid format")

        exporter = EvidenceExporter(format=format)
        content, content_type = exporter.export(queryset=self.get_export_queryset())

        response = HttpResponse(content, content_type=content_type)
        response["Content-Disposition"] = f"inline; filename=export.{format}"
        return response


class EvidenceExportView(ExportMixin, EvidenceListView):
    def get_export_queryset(self):
        sqs = self.get_queryset()
        sqs.update_query()
        return sqs.to_queryset()


class EvidenceDetailExportView(ExportMixin, EvidenceMixin, DetailView):
    def get_export_queryset(self):
        queryset = self.get_queryset().filter(pk=self.kwargs["pk"])
        if not queryset.exists():
            raise Http404(
                _("No %(verbose_name)s found matching the query")
                % {"verbose_name": queryset.model._meta.verbose_name}
            )
        return queryset
