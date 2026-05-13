import csv
import io

from django.conf import settings
from django.core.exceptions import BadRequest, FieldDoesNotExist
from django.db.models import QuerySet
from django.db.models.fields.related import ManyToManyField
from django.http import Http404, HttpResponse
from django.template.loader import render_to_string
from django.utils.translation import gettext as _
from django.views.decorators.cache import never_cache
from django.views.generic import DetailView

from froide.foirequest.pdf_generator import get_wp
from froide.helper.breadcrumbs import BreadcrumbView
from froide.helper.search.views import BaseSearchView
from froide_evidencecollection.documents import EvidenceDocument

from .filterset import EvidenceFilterSet
from .models import Evidence


class EvidenceExporter:
    EXPORT_FIELDS = [
        ("id", _("Id")),
        ("citation", _("Citation")),
        ("description", _("Description")),
        ("documentation_date", _("Documentation Date")),
        ("evidence_type__name", _("Evidence Type")),
    ]
    FORMATS = ["csv", "xlsx", "pdf"]

    @property
    def export_db_fields(self):
        fields = []
        for field in self.EXPORT_FIELDS:
            if isinstance(field, tuple):
                fields.append(field[0])
            else:
                fields.append(field)
        return fields

    @property
    def export_human_fields(self):
        fields = []
        for field in self.EXPORT_FIELDS:
            if isinstance(field, tuple):
                fields.append(field[1])
            else:
                fields.append(field)
        return fields

    def __init__(self, format):
        if format not in self.FORMATS:
            raise ValueError(f"format {format} is not supported")
        self.format = format

    def export(self, queryset):
        rows = self.get_rows(queryset)
        return getattr(self, f"generate_{self.format}")(rows)

    def get_rows(self, queryset):
        """
        Builds a list of row dictionaries with resolved field values for export.

        Handles nested relations and many-to-many fields, returning each row
        as a flat dictionary where keys are field paths (as in `export_db_fields`).

        Args:
            queryset: The base queryset of model instances.

        Returns:
            A list of dictionaries, one per row to be exported.
        """
        prefetch_fields = self._collect_prefetch_fields(queryset.model)
        queryset = queryset.prefetch_related(*prefetch_fields)

        rows = []
        for obj in queryset:
            row = {}
            for field_path in self.export_db_fields:
                value = resolve_nested_value(obj, field_path.split("__"))
                row[field_path] = ", ".join(value) if isinstance(value, list) else value
            rows.append(row)

        return rows

    def _collect_prefetch_fields(self, model):
        """
        Collects all nested fields from `export_db_fields` that require prefetching,
        such as many-to-many fields and reverse relations.

        Args:
            model: The base model class.

        Returns:
            A set of field paths suitable for use with `prefetch_related()`.
        """
        prefetch_fields = set()

        for field_path in self.export_db_fields:
            parts = field_path.split("__")
            cur_model = model
            prefetch = []

            for part in parts:
                try:
                    field = cur_model._meta.get_field(part)
                except FieldDoesNotExist:
                    break

                if isinstance(field, ManyToManyField):
                    prefetch_fields.add("__".join(prefetch + [part]))
                    break
                elif field.is_relation:
                    prefetch.append(part)
                    cur_model = field.related_model
                else:
                    break
            else:
                if prefetch:
                    prefetch_fields.add("__".join(prefetch))

        return prefetch_fields

    def _generate_table(self, rows):
        table = []
        table.append(self.export_human_fields)
        for row in rows:
            table.append([row.get(key) for key in self.export_db_fields])
        return table

    def generate_csv(self, rows):
        f = io.StringIO()
        writer = csv.writer(f)
        writer.writerows(self._generate_table(rows))

        return f.getvalue().encode(), "text/csv"

    def generate_xlsx(self, rows):
        import openpyxl

        wb = openpyxl.Workbook()
        ws = wb.active
        if ws is None:
            ws = wb.create_sheet()
        for row in self._generate_table(rows):
            ws.append(row)
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


def resolve_nested_value(obj, parts):
    """
    Recursively resolves a nested field path from an object.
    Handles many-to-many relationships by collecting all values and returning a list.

    Args:
        obj: The base model instance.
        parts: A list of attribute names, representing the path (e.g., ["source", "public_body", "name"]).

    Returns:
        A string, list of strings, or empty string depending on the result.
    """
    current = obj

    for i, part in enumerate(parts):
        if current is None:
            return ""

        # Check if we are at a ManyToMany or reverse relation manager.
        if hasattr(current, "all"):
            results = []
            for item in current.all():
                val = resolve_nested_value(item, parts[i:])
                if isinstance(val, list):
                    results.extend(val)
                else:
                    results.append(val)
            return sorted(set(map(str, results)))
        else:
            current = getattr(current, part, None)

    if isinstance(current, list):
        return sorted(set(map(str, current)))

    return str(current) if current is not None else ""


class EvidenceMixin(BreadcrumbView):
    def get_breadcrumbs(self, context):
        if "request" in context:
            request = context["request"]

            title = request.current_page.get_title()
            url = request.current_page.get_absolute_url()
            return [(title, url)]

        return []

    def get_queryset(self):
        return Evidence.objects.all()


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

    # ES field name → form filter name.  Filters listed here use post_filter so
    # that each field's aggregation ignores its own selection (standard faceted
    # search behaviour: selecting category=A still shows all categories in the
    # dropdown, but narrows the options of every *other* dropdown).
    FILTER_AGGREGATIONS = {
        "categories": "category",
        "platform": "platform",
        "originator_organizations": "organization",
        "originator_roles": "role",
        "originator_institutional_levels": "institutional_level",
        "evidence_type": "evidence_type",
    }

    facet_config = {field: {} for field in FILTER_AGGREGATIONS}

    def show_facets(self):
        return True

    def paginate_queryset(self, sqs, page_size):
        result = super().paginate_queryset(sqs, page_size)
        self._restrict_form_choices(sqs)
        return result

    def _restrict_form_choices(self, sqs):
        """Limit each filter dropdown to values that actually appear in the
        current (filtered) result set, based on ES aggregation buckets."""
        agg_data = sqs.get_facet_data()
        for es_field, form_field_name in self.FILTER_AGGREGATIONS.items():
            if es_field not in agg_data:
                continue
            # Aggregation is nested: outer filter-agg → inner terms-agg.
            # ES-DSL returns AttrDict objects (attribute access, not .get()).
            inner = agg_data[es_field]
            if es_field in inner:
                inner = inner[es_field]
            buckets = inner.buckets if hasattr(inner, "buckets") else []
            bucket_keys = {b["key"] for b in buckets}

            field = self.form.fields.get(form_field_name)
            if field is None:
                continue
            if hasattr(field, "queryset"):
                field.queryset = field.queryset.filter(pk__in=bucket_keys)
            elif hasattr(field, "choices"):
                # Don't keep the empty ("", "---") entry — the ChoiceIterator
                # adds one automatically when rendering.
                field.choices = [c for c in field.choices if c[0] in bucket_keys]

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


class NeverCacheMixin:
    def dispatch(self, *args, **kwargs):
        return never_cache(super().dispatch)(*args, **kwargs)


class EvidenceExportView(NeverCacheMixin, ExportMixin, EvidenceListView):
    def get_export_queryset(self):
        sqs = self.get_queryset()
        sqs.update_query()
        return sqs.to_queryset()


class EvidenceDetailExportView(NeverCacheMixin, ExportMixin, EvidenceMixin, DetailView):
    def get_export_queryset(self):
        queryset = self.get_queryset().filter(pk=self.kwargs["pk"])
        if not queryset.exists():
            raise Http404(
                _("No %(verbose_name)s found matching the query")
                % {"verbose_name": queryset.model._meta.verbose_name}
            )
        return queryset
