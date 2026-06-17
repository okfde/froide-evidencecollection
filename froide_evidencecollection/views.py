import csv
import html
import io
import math
import sys
import time
from collections import defaultdict

from django.conf import settings
from django.core.exceptions import BadRequest, FieldDoesNotExist
from django.core.paginator import Paginator
from django.db.models import Count, Max, Min, Prefetch, Q, QuerySet, Sum
from django.db.models.fields.related import ManyToManyField
from django.db.models.functions import TruncDate
from django.http import Http404, HttpResponse
from django.template.loader import render_to_string
from django.urls import reverse
from django.utils.safestring import mark_safe
from django.utils.translation import gettext as _
from django.views.decorators.cache import never_cache
from django.views.generic import DetailView, TemplateView

from froide.foirequest.pdf_generator import get_wp
from froide.georegion.models import GeoRegion
from froide.helper.breadcrumbs import BreadcrumbView
from froide.helper.search.views import BaseSearchView
from froide_evidencecollection.documents import EvidenceDocument

from .filterset import EvidenceFilterSet
from .models import (
    Actor,
    Category,
    Chapter,
    Evidence,
    EvidenceMention,
    EvidenceType,
    InstitutionalLevel,
    Keyword,
    Organization,
    PoliticalPosition,
    Role,
    SocialMediaAccount,
    Theme,
)


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

    def get_queryset(self):
        return Evidence.objects.select_related(
            "evidence_type",
            "social_media_post__account",
        ).prefetch_related(
            "originators__person__status",
            "originators__organization__institutional_level",
            "related_actors__person__status",
            "related_actors__organization__institutional_level",
            # Only enabled keywords — a curator-disabled keyword is suppressed
            # everywhere, including this listing (mirrors the topic cloud).
            Prefetch("keywords", queryset=Keyword.objects.filter(enabled=True)),
            "mentions__category",
            "attachments",
            "social_media_post__images",
            "social_media_post__videos__excerpts",
            "social_media_post__screenshots",
        )

    def get_breadcrumbs(self, context):
        obj = self.get_object()

        breadcrumbs = super().get_breadcrumbs(context)

        return breadcrumbs + [
            (_("Evidence #%s" % obj.pk), obj.get_absolute_url()),
        ]


# Prefetches shared by every place that renders a list of evidence cards.
EVIDENCE_CARD_SELECT_RELATED = (
    "evidence_type",
    "social_media_post__account",
)
EVIDENCE_CARD_PREFETCH_RELATED = (
    "originators__person__status",
    "originators__organization__institutional_level",
    "mentions__category",
    "attachments",
    "social_media_post__images",
    "social_media_post__videos__excerpts",
)

ACTOR_PROFILE_EVIDENCE_LIMIT = 20


class ActorDetailView(DetailView):
    model = Actor
    template_name = "froide_evidencecollection/actor_detail.html"
    context_object_name = "actor"

    def get_queryset(self):
        return Actor.objects.select_related(
            "person",
            "person__status",
            "organization",
            "organization__institutional_level",
        ).prefetch_related(
            "social_media_accounts",
            "organization__regions",
        )

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        actor = self.object

        # "Originated by this actor" vs. "Related" — the two plain M2M fields
        # on Evidence (`originators` / `related_actors`). The actor profile
        # lists each piece by date / platform / theme(s) / keywords, so the
        # rows carry `themes` and `keywords` on top of the shared card prefetch.
        list_prefetch = (*EVIDENCE_CARD_PREFETCH_RELATED, "themes", "keywords")
        originated = (
            Evidence.objects.filter(originators=actor)
            .select_related(*EVIDENCE_CARD_SELECT_RELATED)
            .prefetch_related(*list_prefetch)
            .order_by("-pk")
            .distinct()
        )
        related = (
            Evidence.objects.filter(related_actors=actor)
            .select_related(*EVIDENCE_CARD_SELECT_RELATED)
            .prefetch_related(*list_prefetch)
            .order_by("-pk")
            .distinct()
        )
        context["originated_evidence"] = self._with_full_themes(
            originated[:ACTOR_PROFILE_EVIDENCE_LIMIT]
        )
        context["originated_total"] = originated.count()
        context["related_evidence"] = self._with_full_themes(
            related[:ACTOR_PROFILE_EVIDENCE_LIMIT]
        )
        context["related_total"] = related.count()
        context["evidence_limit"] = ACTOR_PROFILE_EVIDENCE_LIMIT

        return context

    @staticmethod
    def _with_full_themes(evidence_iterable):
        """Attach each evidence's *full* theme set as ``full_themes``.

        A piece belongs to a theme either directly (the ``themes`` M2M) or via a
        chapter it's filed under that maps to the theme or a descendant (resolved
        through ``Chapter.chapter_theme_map``) — the same union
        ``Theme.evidence_queryset`` builds in the other direction. Reads only the
        prefetched ``themes`` and ``mentions``, deduplicates, and orders by
        ``Theme.order`` so the chips read in the curator's order.
        """
        theme_by_chapter = Chapter.chapter_theme_map()
        themes_by_id = {t.id: t for t in Theme.objects.all()}
        evidence_list = list(evidence_iterable)
        for evidence in evidence_list:
            ids = {t.id for t in evidence.themes.all()}
            for mention in evidence.mentions.all():
                theme_id = theme_by_chapter.get(mention.chapter_id)
                if theme_id is not None:
                    ids.add(theme_id)
            evidence.full_themes = sorted(
                (themes_by_id[i] for i in ids if i in themes_by_id),
                key=lambda t: (t.order, t.id),
            )
        return evidence_list


class EvidenceListView(BaseSearchView):
    search_name = "evidence"
    template_name = "froide_evidencecollection/dashboard.html"
    filterset = EvidenceFilterSet
    document = EvidenceDocument
    model = Evidence
    search_url_name = "evidencecollection:dashboard"
    # Fields the result card touches per row. Keeps the originator block,
    # category chips, attachments badge and source line off the per-row
    # SELECT loop.
    select_related = EVIDENCE_CARD_SELECT_RELATED
    prefetch_related = EVIDENCE_CARD_PREFETCH_RELATED

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


def group_evidence_by_originator(evidences):
    """Group an iterable of Evidence by originator actor.

    An evidence with several originators appears in each of their groups;
    one with no originator goes into a single "unattributed" group keyed
    by ``actor=None``. Groups are ordered by tile count, descending.
    """
    groups: dict[int | None, dict] = {}
    for evidence in evidences:
        originators = evidence.originator_actors or [None]
        for actor in originators:
            key = actor.pk if actor is not None else None
            bucket = groups.get(key)
            if bucket is None:
                groups[key] = {"actor": actor, "evidences": [evidence]}
            else:
                bucket["evidences"].append(evidence)
    return sorted(
        ({**g, "count": len(g["evidences"])} for g in groups.values()),
        key=lambda g: (-g["count"], g["actor"].name if g["actor"] else ""),
    )


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
        queryset = self.get_queryset().filter(slug=self.kwargs["slug"])
        if not queryset.exists():
            raise Http404(
                _("No %(verbose_name)s found matching the query")
                % {"verbose_name": queryset.model._meta.verbose_name}
            )
        return queryset


# GET param names the dashboard treats as "an active filter". When any of
# them is set (non-empty) we render the results section; otherwise we show
# the empty-state discovery tiles.
DASHBOARD_FILTER_PARAMS = (
    "q",
    "originator",
    "organization",
    "role",
    "institutional_level",
    "category",
    "evidence_type",
    "platform",
    "publishing_date_after",
    "publishing_date_before",
)


class DashboardView(EvidenceListView):
    """Combined entry point: filter form + (results | discovery tiles).

    Inherits ES search + facet behaviour from EvidenceListView; adds the
    filter-form dropdown choices and the discovery-tile data.
    """

    # Pagination is by actor, not by tile. We fetch a large window of raw
    # evidence from ES (well under its 10 000-record default cap) and slice
    # the *actor groups* with a Django Paginator further down.
    paginate_by = 1000
    actors_per_page = 20

    def paginate_queryset(self, sqs, page_size):
        # The URL's `?page=` is reserved for actor pagination, so ES must
        # always operate on its first window. Swap the param out for the
        # duration of the ES paginate call only.
        original_get = self.request.GET
        self.request.GET = original_get.copy()
        self.request.GET[self.page_kwarg] = "1"
        try:
            return super().paginate_queryset(sqs, page_size)
        finally:
            self.request.GET = original_get

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)

        all_groups = group_evidence_by_originator(context["object_list"])
        paginator = Paginator(all_groups, self.actors_per_page)
        page = paginator.get_page(self.request.GET.get(self.page_kwarg))
        context["actor_groups"] = list(page.object_list)
        context["actor_total"] = paginator.count
        context["page_obj"] = page
        context["paginator"] = paginator
        context["is_paginated"] = page.has_other_pages()

        context["has_filters"] = any(
            (self.request.GET.get(p) or "").strip() for p in DASHBOARD_FILTER_PARAMS
        )
        context["dashboard_reset_url"] = reverse("evidencecollection:dashboard")

        context["organizations"] = Organization.objects.order_by("organization_name")
        context["roles"] = Role.objects.order_by("name")
        context["institutional_levels"] = InstitutionalLevel.objects.order_by("name")
        context["evidence_types"] = EvidenceType.objects.order_by("name")
        context["categories"] = Category.objects.all()
        context["platforms"] = SocialMediaAccount.Platform.choices

        # Discovery tiles only render in the empty state, but populating them
        # unconditionally costs little and keeps the template branchless.
        context["top_originators"] = (
            Actor.objects.annotate(
                evidence_count=Count("originated_evidence", distinct=True)
            )
            .filter(evidence_count__gt=0)
            .order_by("-evidence_count", "name")[:10]
        )

        return context


class EvidenceTopicCloudView(TemplateView):
    """View over BERTopic-fitted pieces of evidence, browsed by keyword.

    The primary structure is a server-rendered, screen-reader-navigable
    outline listing the matching evidence. A small SVG scatter sits on top
    as a visual aid — ``aria-hidden`` because the list below carries the
    same information in semantic form. Browsing is by ``Theme`` (the single
    chip bar) and keyword facet; the toolbar additionally filters by
    platform, date range, actor, and free-text search.

    Dot *positions* still come from the fit's 2D embedding (``topic_x`` /
    ``topic_y``); their *colour* is the evidence's dominant ``Theme`` (see
    ``_dominant_theme``), the same hue carried by that theme's chip.

    Account-derived facets (platform, username, organization, actor) are
    sourced from each evidence's social-media-post source; evidence backed
    by a document instead has no account and falls out of those filters.
    """

    template_name = "froide_evidencecollection/topic_cloud.html"

    # Hard cap on rows fetched from the DB. The cloud SVG renders one
    # circle per row; the screen-reader outline is further trimmed by
    # OUTLINE_MAX_EVIDENCE so the hidden HTML payload stays small.
    MAX_EVIDENCE = 2000

    # Evidence listed in the SR-only / mobile outline. Keeps the hidden DOM
    # bounded even when the filtered set is large; users hunting a specific
    # item can narrow via the keyword facets.
    OUTLINE_MAX_EVIDENCE = 100

    # Neutral fallback ink: drawn for dots whose dominant keyword is ungrouped,
    # and for groups past the palette's length. A warm taupe keeps the cloud
    # on-theme and holds contrast on the near-white canvas.
    DOT_COLOR = "#7a6e60"

    # Categorical palette for dominant-theme dot colouring. Mid-dark hues
    # chosen to hold contrast on the near-white (#fafafa) canvas, stay
    # distinguishable from one another, and read apart from the neutral
    # DOT_COLOR. Assigned in curator `Theme.order` (see `_assign_theme_colors`),
    # which is stable across requests so a theme keeps its colour as filters
    # change. Themes beyond this length fall back to DOT_COLOR.
    GROUP_PALETTE = (
        "#4e79a7",  # blue
        "#f28e2b",  # orange
        "#59a14f",  # green
        "#e15759",  # red
        "#b07aa1",  # purple
        "#76b7b2",  # teal
        "#9c6b30",  # brown
        "#d4a017",  # gold
    )

    # SVG viewport. The data x/y are projected into this box; the actual
    # rendered size is fluid (width:100%) so it adapts to mobile widths.
    SVG_WIDTH = 1000
    SVG_HEIGHT = 600
    SVG_PADDING = 16

    SNIPPET_CHARS = 280

    # Keyword facets are the primary "by keyword" browse surface: a facet is one
    # keyword, drawn from the precomputed Evidence↔Keyword index (built by
    # `fit_post_topics` from lemmatised text), so selecting it narrows to
    # evidence that genuinely contains the word. Several can be combined (AND).
    # The list is recomputed over the filtered set each request, so only
    # co-occurring keywords remain. MAX_FACETS caps the cloud;
    # FACET_WEIGHT_BUCKETS is the font-size scale.
    MAX_FACETS = 50
    FACET_WEIGHT_BUCKETS = 5

    # Template fragment rendered on its own when htmx asks for an in-place
    # filter refresh (detected via the HX-Request header / request.htmx) —
    # covers the count line, the cloud, the legend, and the SR-only outline.
    PARTIAL_TEMPLATE = "froide_evidencecollection/_topic_cloud_partial.html"

    def render_to_response(self, context, **response_kwargs):
        _t0 = time.perf_counter()
        # Detect htmx via the request header rather than django-htmx's
        # middleware, so the host project doesn't have to install it.
        if self.request.headers.get("HX-Request") == "true":
            resp = HttpResponse(
                render_to_string(self.PARTIAL_TEMPLATE, context, request=self.request)
            )
        else:
            resp = super().render_to_response(context, **response_kwargs)
            # Force template rendering now so the timing covers it.
            resp.render()
        print(
            f"topiccloud[render_to_response] {(time.perf_counter() - _t0) * 1000:.1f} ms",
            file=sys.stderr,
            flush=True,
        )
        return resp

    # Query params that narrow the evidence set in `_filter_qs`. If any is
    # present the facet ranking switches from frequency to keyness, since the
    # filtered set is then a real slice to contrast against the corpus.
    # `keyword` is handled separately (against the enabled selection), since a
    # disabled lemma in the URL is dropped and must not trigger keyness.
    NARROWING_PARAMS = (
        "q",
        "theme",
        "platform",
        "posted_after",
        "posted_before",
        "actor",
        "role",
        "level",
        "region",
    )

    # Relation path from an Evidence to the political positions held by the
    # posting actor's person. Only social-media-post evidence reaches it;
    # document-backed evidence has no account/actor and falls out.
    POLITICAL_POSITION_PREFIX = (
        "social_media_post__account__actor__person__political_positions"
    )

    @classmethod
    def _has_active_filter(cls, params):
        """True if any narrowing filter is set, so the filtered evidence is a
        genuine slice (keyness applies) rather than the whole corpus. Excludes
        the `keyword` param — the caller ORs in the resolved enabled selection."""
        return any((params.get(p) or "").strip() for p in cls.NARROWING_PARAMS)

    @staticmethod
    def _param_year(value):
        """Parse the leading year out of a ``YYYY-MM-DD`` date param. Returns an
        int year, or None when the param is absent or not a parseable date. The
        year slider writes Jan-1 / Dec-31 dates, so this round-trips the handle
        positions back out of `posted_after` / `posted_before`."""
        value = (value or "").strip()
        if len(value) >= 4 and value[:4].isdigit():
            return int(value[:4])
        return None

    @staticmethod
    def _selected_keywords(params):
        """Selected keyword facets = the (de-duped, order-preserving) ``keyword``
        query params, each a Keyword lemma. Supports repeated params for the
        multi-select AND drill-down. Raw — may include disabled/unknown lemmas;
        use ``_selected_enabled_keywords`` for anything that acts on them."""
        seen = []
        for raw in params.getlist("keyword"):
            kw = (raw or "").strip()
            if kw and kw not in seen:
                seen.append(kw)
        return seen

    @classmethod
    def _selected_enabled_keywords(cls, params):
        """Selected lemmas restricted to keywords that exist and are enabled,
        order preserved. Disabled (curator-suppressed) or unknown lemmas in the
        URL are silently dropped, so they never filter the set or render a chip —
        a disabled keyword is excluded everywhere, not just hidden from the
        cloud."""
        selected = cls._selected_keywords(params)
        if not selected:
            return []
        enabled = set(
            Keyword.objects.filter(lemma__in=selected, enabled=True).values_list(
                "lemma", flat=True
            )
        )
        return [kw for kw in selected if kw in enabled]

    @staticmethod
    def _selected_theme_id(params):
        """Selected theme = the first valid ``theme`` query param (a ``Theme``
        pk), or ``None`` when none is set. The theme bar is single-select:
        clicking a theme narrows the cloud to that theme's evidence (and colours
        every visible dot with the theme's hue), so two selected themes has no
        meaning. Non-numeric values are skipped; any extra ``theme`` params are
        ignored."""
        for raw in params.getlist("theme"):
            raw = (raw or "").strip()
            if raw.isdigit():
                return int(raw)
        return None

    @classmethod
    def _political_position_q(cls, params):
        """Filter on the *function the originator held when the evidence was
        posted* — the posting actor's person's political position, narrowed by
        any of three params:

        * ``role`` — the function/role of that position (a ``Role`` pk),
        * ``level`` — its institutional level (an ``InstitutionalLevel`` pk),
        * ``region`` — the region it is anchored in (a ``GeoRegion`` pk).

        All three bind to a *single* position (one join), and that position
        must have been active on the post's date: its start on or before and
        its end on or after it, with an open-ended (null) start or end counting
        as unbounded. Combining several therefore narrows to one position that
        matches all of them at post time — "the same function". A position's
        ``start_date`` / ``end_date`` are month-precision day markers, so the
        comparison is against the post's calendar date (``TruncDate``), making
        both day boundaries inclusive.

        Returns a ``Q`` to AND into the queryset, or ``None`` when none of the
        three params is set. Only social-media-post evidence can match (the path
        runs through the account's actor → person); document-backed evidence
        falls out.
        """
        pp = cls.POLITICAL_POSITION_PREFIX
        attr_conds = []
        for name, field in (
            ("role", "role_id"),
            ("level", "institutional_level_id"),
            ("region", "region_id"),
        ):
            raw = (params.get(name) or "").strip()
            if raw.isdigit():
                attr_conds.append(Q(**{f"{pp}__{field}": int(raw)}))
        if not attr_conds:
            return None

        posted_date = TruncDate("social_media_post__posted_at")
        active = (
            Q(**{f"{pp}__start_date__isnull": True})
            | Q(**{f"{pp}__start_date__lte": posted_date})
        ) & (
            Q(**{f"{pp}__end_date__isnull": True})
            | Q(**{f"{pp}__end_date__gte": posted_date})
        )
        for cond in attr_conds:
            active &= cond
        return active

    @staticmethod
    def _regions_by_evidence(evidences):
        """Map evidence pk → the region(s) of the political function its posting
        person held when it was posted, as a display string (e.g. ``"Bayern"``).

        Mirrors `_political_position_q`'s active-at-post-time test (the position's
        start on or before and end on or after the post date, a null bound
        counting as unbounded) but only to *show* the Bundesland the originator's
        function was anchored in. Evidence with no posting person, no dated post,
        or no region-bearing active position is simply absent from the map. A
        person holding several matching positions yields their distinct regions,
        comma-joined.
        """
        # Posting person + post date per evidence. Only social-media evidence
        # reaches a person (document-backed evidence has no account/actor).
        person_ids = set()
        ev_meta = []  # (evidence_pk, person_id, post_date)
        for ev in evidences:
            post = ev.social_media_post if ev.social_media_post_id else None
            if post is None or post.posted_at is None or not post.account_id:
                continue
            account = post.account
            if not account.actor_id:
                continue
            person_id = account.actor.person_id
            if not person_id:
                continue
            person_ids.add(person_id)
            ev_meta.append((ev.pk, person_id, post.posted_at.date()))
        if not person_ids:
            return {}

        # Region-bearing positions for those persons, grouped by person so the
        # per-evidence date match below is a cheap in-memory scan.
        positions_by_person = defaultdict(list)
        for pos in (
            PoliticalPosition.objects.filter(
                person_id__in=person_ids, region__isnull=False
            )
            .select_related("region")
            .only("person_id", "start_date", "end_date", "region__name")
        ):
            positions_by_person[pos.person_id].append(pos)

        regions = {}
        for ev_pk, person_id, post_date in ev_meta:
            names = []
            for pos in positions_by_person.get(person_id, ()):
                if pos.start_date and pos.start_date > post_date:
                    continue
                if pos.end_date and pos.end_date < post_date:
                    continue
                name = pos.region.name
                if name and name not in names:
                    names.append(name)
            if names:
                regions[ev_pk] = ", ".join(names)
        return regions

    def _filter_qs(self):
        # `.only()` is load-bearing: SocialMediaPost has wide JSONFields
        # (`raw`, `user_snapshot`, `reactions`) that would otherwise be
        # fetched + deserialized for every joined row, and they're not used
        # here. The account fields are pulled via select_related so the SR
        # outline reads them without an N+1. ``topic_fit_at__isnull=False`` is
        # the "is fitted" gate — only fitted evidence has the embedding coords
        # the cloud plots; the cluster itself is no longer surfaced (and the
        # keyword fit no longer sets the `topic` FK at all).
        # Account-derived facets traverse the `social_media_post` source.
        qs = (
            Evidence.objects.filter(topic_fit_at__isnull=False)
            .select_related(
                "social_media_post__account__actor",
            )
            # Media is a reverse FK (to-many), so it can't ride select_related;
            # prefetch it for `_snippet`'s transcription read. `keywords` is the
            # facet index, read in-memory by `_build_facets`; prefetch only the
            # enabled ones so curator-disabled keywords never reach the view.
            .prefetch_related(
                "social_media_post__images",
                "social_media_post__videos__excerpts",
                Prefetch("keywords", queryset=Keyword.objects.filter(enabled=True)),
                # Theme membership for the dot's dominant-theme colour: the
                # directly assigned themes, and the mentions' chapters (the
                # chapter-derived themes are resolved via `chapter_theme_map`).
                # Only the chapter FK is needed off each mention, so keep it
                # light.
                "themes",
                Prefetch(
                    "mentions",
                    queryset=EvidenceMention.objects.only(
                        "id", "evidence_id", "chapter_id"
                    ),
                ),
            )
            .only(
                "pk",
                "citation",
                "description",
                "topic_id",
                "topic_x",
                "topic_y",
                "social_media_post__url",
                "social_media_post__title",
                "social_media_post__text",
                "social_media_post__posted_at",
                "social_media_post__account__platform",
                "social_media_post__account__username",
                "social_media_post__account__actor",
            )
            .order_by("-social_media_post__posted_at", "-pk")
        )

        params = self.request.GET
        q = (params.get("q") or "").strip()
        if q:
            # The media join (a to-many) can multiply rows, so de-dupe.
            qs = qs.filter(
                Q(social_media_post__title__icontains=q)
                | Q(social_media_post__text__icontains=q)
                | Q(social_media_post__images__content_text__icontains=q)
                | Q(social_media_post__images__content_text_override__icontains=q)
                | Q(social_media_post__videos__excerpts__text__icontains=q)
                | Q(social_media_post__videos__excerpts__text_override__icontains=q)
                | Q(citation__icontains=q)
                | Q(description__icontains=q)
            ).distinct()

        # Theme: the single broad entry point, single-select. A theme's evidence
        # is the union of directly assigned evidence (the `themes` M2M) and
        # everything filed under a chapter mapped to the theme or a descendant
        # (resolved by `chapter_theme_map`). Both halves are a DB filter, so the
        # narrowing happens here in one query — no per-evidence Python cut.
        # distinct() because either join (the M2M or the mention→chapter path)
        # can match a row several times.
        theme_id = self._selected_theme_id(params)
        if theme_id is not None:
            themed_chapters = [
                cid
                for cid, tid in Chapter.chapter_theme_map().items()
                if tid == theme_id
            ]
            theme_q = Q(themes__id=theme_id)
            if themed_chapters:
                theme_q |= Q(mentions__chapter__in=themed_chapters)
            qs = qs.filter(theme_q).distinct()

        # Keyword facets: narrow to evidence whose text actually contains the
        # selected keyword lemma(s), via the Evidence↔Keyword index. Several
        # selected keywords AND together (each a separate join on the M2M), so
        # the result is evidence carrying *all* of them — the drill-down that
        # makes the narrowed facet list meaningful. Disabled/unknown lemmas are
        # dropped (not applied), so a stale URL can't filter on a suppressed
        # keyword.
        for kw in self._selected_enabled_keywords(params):
            qs = qs.filter(keywords__lemma=kw)

        platform = (params.get("platform") or "").strip()
        if platform:
            qs = qs.filter(social_media_post__account__platform=platform)

        for name, lookup in (
            ("posted_after", "social_media_post__posted_at__date__gte"),
            ("posted_before", "social_media_post__posted_at__date__lte"),
        ):
            value = (params.get(name) or "").strip()
            if value:
                qs = qs.filter(**{lookup: value})

        actor = (params.get("actor") or "").strip()
        if actor:
            try:
                qs = qs.filter(social_media_post__account__actor_id=int(actor))
            except ValueError:
                pass

        # Originator-function filters (role / institutional level / region of the
        # political position the posting person held at post time). Bound to a
        # single active position via one join, so distinct() to fold the to-many.
        pp_q = self._political_position_q(params)
        if pp_q is not None:
            qs = qs.filter(pp_q).distinct()

        return qs

    @classmethod
    def _snippet(cls, evidence):
        # Source text for the outline. Reads the post's own fields directly
        # rather than `search_text` so it stays within the prefetched columns
        # and doesn't recurse into redistributed posts.
        # Media text is read from the prefetched `images` / `videos__excerpts`
        # relations: an image's on-screen text plus each video excerpt's text.
        post = evidence.social_media_post
        if post is not None:
            media_bits = [
                img.resolved_content_text
                for img in post.images.all()
                if img.resolved_content_text
            ]
            media_bits += [
                excerpt.resolved_text
                for video in post.videos.all()
                for excerpt in video.excerpts.all()
                if excerpt.resolved_text
            ]
            media_text = " ".join(media_bits)
            raw_parts = (post.title, post.text, media_text)
        else:
            raw_parts = (evidence.description,)
        parts = [p.strip() for p in raw_parts if p and p.strip()]
        text = " — ".join(parts)
        if len(text) > cls.SNIPPET_CHARS:
            head = text[: cls.SNIPPET_CHARS]
            cut = head.rsplit(" ", 1)[0] if " " in head[-60:] else head
            text = cut.rstrip() + "…"
        return text

    def _build_themes(self, params):
        """Theme bar data — one chip per theme.

        Returns ``(selected_theme_id, theme_options)``, where each option is
        ``{id, label, count, selected}``. ``count`` is the theme's full evidence
        set (direct assignment ∪ chapter-mapped, restricted to fitted evidence,
        which is the cloud's universe). Themes are listed in the curator's
        explicit ``Theme.order`` (then label) — fixed entry points that don't
        reshuffle as the user drills in — and that order also drives the palette
        assignment (see ``_assign_theme_colors``). Empty themes are dropped.
        """
        selected_theme_id = self._selected_theme_id(params)

        # chapter_id -> theme_id (with inheritance), inverted to theme -> chapters.
        chapters_by_theme: dict[int, list[int]] = {}
        for cid, tid in Chapter.chapter_theme_map().items():
            chapters_by_theme.setdefault(tid, []).append(cid)

        theme_options = []
        for theme in Theme.objects.all():  # ordered by (order, label)
            theme_q = Q(themes=theme)
            chapter_ids = chapters_by_theme.get(theme.id)
            if chapter_ids:
                theme_q |= Q(mentions__chapter__in=chapter_ids)
            count = (
                Evidence.objects.filter(topic_fit_at__isnull=False)
                .filter(theme_q)
                .distinct()
                .count()
            )
            if count == 0:
                continue
            theme_options.append(
                {
                    "id": theme.id,
                    "label": theme.label,
                    "count": count,
                    "selected": theme.id == selected_theme_id,
                }
            )

        # Drop a stale/empty selection so the chip state stays honest.
        if selected_theme_id is not None and not any(
            o["selected"] for o in theme_options
        ):
            selected_theme_id = None
        return selected_theme_id, theme_options

    def _assign_theme_colors(self, theme_options):
        """Give each theme its identity colour — used on its chip and its dots.

        ``theme_options`` arrives from ``_build_themes`` in the curator's
        explicit ``Theme.order``, so the first ``len(GROUP_PALETTE)`` themes get
        a palette hue and the rest fall back to the neutral ``DOT_COLOR``. The
        order is curator-fixed, so the mapping is stable across requests — a
        theme keeps its colour as the user filters.

        Mutates each option with a ``color`` key and returns ``color_by_theme``
        (id → colour) for the dot fill.
        """
        color_by_theme = {}
        for i, opt in enumerate(theme_options):
            color = (
                self.GROUP_PALETTE[i] if i < len(self.GROUP_PALETTE) else self.DOT_COLOR
            )
            opt["color"] = color
            color_by_theme[opt["id"]] = color
        return color_by_theme

    @staticmethod
    def _dominant_theme(evidence, theme_by_chapter, theme_order):
        """Pick the one theme that colours this evidence's dot.

        An evidence can belong to several themes (directly and/or via the
        chapters it's filed under). Precedence: a **directly assigned** theme
        beats a **chapter-derived** one (the curator's explicit, evidence-level
        pick is the stronger signal); within a tier the theme with the lowest
        ``Theme.order`` wins, then the lowest id, so the choice is deterministic
        and curator-controlled.

        Reads only the evidence's own prefetched ``themes`` and ``mentions`` (the
        latter mapped through ``theme_by_chapter``), so it is filter-independent:
        a dot keeps its colour as the set narrows. Returns a theme id, or
        ``None`` when the evidence belongs to no theme.
        """

        def _rank(tid):
            return (theme_order.get(tid, 1 << 30), tid)

        direct = [t.id for t in evidence.themes.all()]
        if direct:
            return min(direct, key=_rank)
        chapter_themes = [
            theme_by_chapter[m.chapter_id]
            for m in evidence.mentions.all()
            if m.chapter_id in theme_by_chapter
        ]
        if chapter_themes:
            return min(chapter_themes, key=_rank)
        return None

    @classmethod
    def _dot_fill(cls, lens_color, dominant_theme_id, theme_color):
        """The fill colour for one dot. When a theme is selected, ``lens_color``
        is that theme's hue and every visible dot takes it (the set was already
        narrowed to the theme, so the cloud reads as "these are your {theme}").
        With no theme selected (``lens_color`` is None) each dot falls back to
        its own dominant theme's hue, then the neutral ink."""
        if lens_color is not None:
            return lens_color
        return theme_color.get(dominant_theme_id, cls.DOT_COLOR)

    def _build_facets(
        self,
        evidences,
        selected_lemmas,
        keyness,
    ):
        """Keyword facets over the *currently filtered* evidence set.

        Each facet is one keyword, counted by how many of the given evidence
        actually carry it (via the prefetched Evidence↔Keyword index). The
        already-selected keywords are dropped — what remains is exactly the set
        of keywords that still co-occur with the current selection, so the
        cloud narrows as the user drills in. Counting runs over the in-memory
        evidence list (already capped at ``MAX_EVIDENCE``), reading the
        ``keywords`` prefetch, so it costs no extra query.

        Ranking depends on whether a filter is active:

        * ``keyness=False`` (nothing narrowed — the slice *is* the corpus):
          rank by raw frequency. Keyness would be degenerate here, since every
          keyword is averagely represented against its own baseline.
        * ``keyness=True`` (a filter narrowed the set): rank by **keyness** —
          how over-represented each keyword is in the slice vs. the whole
          corpus — using the log-odds-ratio with an informative Dirichlet prior
          (Monroe et al. 2008), the variance-stabilised z-score. This surfaces
          what's *distinctive* about the slice rather than what's merely common,
          so each drill-down reveals new characterising terms. The corpus
          baseline is each keyword's cached ``Keyword.df``.

        Returns up to ``MAX_FACETS`` facets, most relevant first, each with a
        1..``FACET_WEIGHT_BUCKETS`` size weight for the cloud's font scale.
        """
        selected = set(selected_lemmas)
        counts: dict[str, int] = {}
        labels: dict[str, str] = {}
        dfs: dict[str, int] = {}
        # Total keyword incidences in the slice — the n_i normaliser for the
        # log-odds prior. Counts every occurrence, including selected keywords,
        # so it reflects the slice's full keyword mass.
        n_i = 0
        for ev in evidences:
            for kw in ev.keywords.all():
                # Curator-disabled keywords are suppressed entirely: not shown,
                # not counted toward the slice mass, not offered as a facet.
                if not kw.enabled:
                    continue
                n_i += 1
                if kw.lemma in selected:
                    continue
                counts[kw.lemma] = counts.get(kw.lemma, 0) + 1
                labels[kw.lemma] = kw.display_label
                dfs[kw.lemma] = kw.df

        if keyness:
            # a0 = total corpus keyword mass (sum of every enabled keyword's df);
            # n_j = the reference mass outside the slice. One cheap aggregate,
            # restricted to enabled keywords to match the displayed universe.
            a0 = (
                Keyword.objects.filter(enabled=True).aggregate(total=Sum("df"))["total"]
                or 0
            )
            n_j = max(a0 - n_i, 1e-9)
            score: dict[str, float] = {}
            for lemma, y_i in counts.items():
                alpha = dfs[lemma] or 1  # prior = corpus df for this keyword
                y_j = alpha - y_i  # occurrences outside the slice
                den_i = max(n_i + a0 - y_i - alpha, 1e-9)
                den_j = max(n_j + a0 - y_j - alpha, 1e-9)
                delta = math.log((y_i + alpha) / den_i) - math.log(
                    (y_j + alpha) / den_j
                )
                var = 1.0 / (y_i + alpha) + 1.0 / (y_j + alpha)
                score[lemma] = delta / math.sqrt(var)
        else:
            score = {lemma: float(n) for lemma, n in counts.items()}

        facets = [
            {
                "lemma": lemma,
                "keyword": labels[lemma],
                "count": counts[lemma],
                "score": score[lemma],
            }
            for lemma in counts
        ]
        facets.sort(key=lambda f: (-f["score"], f["keyword"]))
        facets = facets[: self.MAX_FACETS]
        if facets:
            hi = facets[0]["score"]
            lo = facets[-1]["score"]
            span = hi - lo or 1
            for f in facets:
                f["weight"] = 1 + round(
                    (f["score"] - lo) / span * (self.FACET_WEIGHT_BUCKETS - 1)
                )
        return facets

    def _project(self, posts, bounds=None):
        """Map post x/y into SVG pixel coordinates. Coords are formatted as
        plain strings (always a ``.`` decimal) so Django's locale-aware
        templating doesn't slip a German comma into the SVG attributes.

        ``bounds`` (``(xmin, xmax, ymin, ymax)``) lets the caller pin the
        projection to the unfiltered dataset's extents so dots keep the
        same screen position when filters shrink the visible set.
        """
        if not posts:
            return []
        if bounds is None:
            xs = [p.topic_x for p in posts]
            ys = [p.topic_y for p in posts]
            xmin, xmax = min(xs), max(xs)
            ymin, ymax = min(ys), max(ys)
        else:
            xmin, xmax, ymin, ymax = bounds
        # Guard against degenerate ranges (single point, or all-equal).
        x_span = xmax - xmin or 1.0
        y_span = ymax - ymin or 1.0

        usable_w = self.SVG_WIDTH - 2 * self.SVG_PADDING
        usable_h = self.SVG_HEIGHT - 2 * self.SVG_PADDING
        out = []
        for p in posts:
            cx = self.SVG_PADDING + (p.topic_x - xmin) / x_span * usable_w
            # Flip Y so positive UMAP-y goes up visually.
            cy = self.SVG_PADDING + (1 - (p.topic_y - ymin) / y_span) * usable_h
            out.append({"post": p, "cx": f"{cx:.1f}", "cy": f"{cy:.1f}"})
        return out

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)

        _t0 = time.perf_counter()
        _last = [_t0]

        def _mark(label):
            now = time.perf_counter()
            print(
                f"topiccloud[{label}] {(now - _last[0]) * 1000:.1f} ms "
                f"(total {(now - _t0) * 1000:.1f} ms)",
                file=sys.stderr,
                flush=True,
            )
            _last[0] = now

        qs = self._filter_qs()
        # Fetch one extra row to detect "more than MAX_EVIDENCE" without
        # running a second COUNT query against the filtered set.
        fetched = list(qs[: self.MAX_EVIDENCE + 1])
        truncated = len(fetched) > self.MAX_EVIDENCE
        evidences = fetched[: self.MAX_EVIDENCE]
        _mark(f"fetch evidence ({len(evidences)})")

        # Dot positions are pinned to the *unfiltered* embedding extents so a
        # dot keeps the same screen position as filters narrow the visible set.
        bounds_agg = Evidence.objects.filter(
            topic__isnull=False,
            topic_x__isnull=False,
            topic_y__isnull=False,
        ).aggregate(
            xmin=Min("topic_x"),
            xmax=Max("topic_x"),
            ymin=Min("topic_y"),
            ymax=Max("topic_y"),
        )
        bounds = None
        if bounds_agg["xmin"] is not None:
            bounds = (
                bounds_agg["xmin"],
                bounds_agg["xmax"],
                bounds_agg["ymin"],
                bounds_agg["ymax"],
            )
        _mark("bounds_agg")

        # Theme bar: a chip per theme, in the curator's explicit `Theme.order`
        # (which also fixes the palette assignment below). The actual narrowing
        # to a selected theme's evidence already happened in `_filter_qs` (a DB
        # filter), so there is no per-evidence Python cut here.
        selected_theme_id, theme_options = self._build_themes(self.request.GET)
        # Theme identity colours: the first themes (by order) get a palette
        # colour, the rest the neutral ink. The same map tints the chips and the
        # dots.
        theme_color = self._assign_theme_colors(theme_options)
        # Resolution maps for the dot's dominant theme: chapter_id → theme_id
        # (with inheritance) and theme_id → order (the tie-breaker). Built once
        # here and reused for every dot.
        theme_by_chapter = Chapter.chapter_theme_map()
        theme_order = dict(Theme.objects.values_list("id", "order"))
        _mark("theme colours")

        # Cloud points — keep dotted only if we have coordinates. Colouring is a
        # lens: when a theme is selected, every visible dot belongs to it (the
        # set was already narrowed to that theme), so they all take the selected
        # theme's colour — the cloud reads as "these are your {theme}". With no
        # theme selected, each dot falls back to its own dominant theme's hue.
        lens_color = (
            theme_color.get(selected_theme_id, self.DOT_COLOR)
            if selected_theme_id is not None
            else None
        )
        plottable = [
            e for e in evidences if e.topic_x is not None and e.topic_y is not None
        ]
        # Render every <circle> as a single string in Python instead of
        # looping in the template. With ~1000 points the template loop is the
        # dominant cost in render_to_response; building the markup directly here
        # (with html.escape on each value) cuts it by an order of magnitude.
        esc = html.escape
        circle_parts = []
        for pt in self._project(plottable, bounds=bounds):
            ev = pt["post"]
            # Account-derived bits come from the social-media-post source.
            account = ev.social_media_post.account if ev.social_media_post_id else None
            platform = account.get_platform_display() if account else ""
            username = account.username if account and account.username else ""
            # The dot's actor, so the side panel can highlight one actor's dots.
            actor_id = account.actor_id if account and account.actor_id else ""
            pub_date = ev.source.publication_date if ev.source else None
            posted_on = pub_date.isoformat() if pub_date else ""
            # `data-theme` carries the dot's own dominant theme (for a later
            # client-side highlight); the fill applies the selection lens.
            theme_id = self._dominant_theme(ev, theme_by_chapter, theme_order)
            fill = self._dot_fill(lens_color, theme_id, theme_color)
            circle_parts.append(
                f'<circle data-href="{esc(ev.get_absolute_url())}"'
                f' data-platform="{esc(platform)}"'
                f' data-username="{esc(username)}"'
                f' data-actor="{actor_id}"'
                f' data-theme="{theme_id if theme_id is not None else ""}"'
                f' data-posted-on="{posted_on}"'
                f' data-snippet="{esc(self._snippet(ev))}"'
                f' cx="{pt["cx"]}" cy="{pt["cy"]}"'
                f' r="4"'
                f' fill="{fill}"></circle>'
            )
        cloud_circles_svg = mark_safe("".join(circle_parts))
        cloud_point_count = len(circle_parts)
        _mark(f"cloud_circles ({cloud_point_count})")

        # Actors present in the filtered set, tallied over the visible evidence
        # (account → actor, already select_related so no extra per-row query).
        # Drives the "Actors in view" side panel; clicking a name highlights
        # that actor's dots client-side rather than filtering. One `in_bulk`
        # resolves the names/labels so the panel reads like the actor dropdown.
        actor_counts = {}
        for ev in evidences:
            post = ev.social_media_post if ev.social_media_post_id else None
            actor_id = post.account.actor_id if post and post.account_id else None
            if actor_id:
                actor_counts[actor_id] = actor_counts.get(actor_id, 0) + 1
        actor_objs = Actor.objects.in_bulk(list(actor_counts))
        actors_in_view = sorted(
            (
                {"pk": pk, "name": str(actor_objs[pk]), "count": count}
                for pk, count in actor_counts.items()
                if pk in actor_objs
            ),
            key=lambda a: (-a["count"], a["name"].lower()),
        )
        _mark(f"actors_in_view ({len(actors_in_view)})")

        # SR-only / mobile outline: a single flat list of the matching
        # evidence, newest first (the queryset is already date-ordered). Capped
        # at OUTLINE_MAX_EVIDENCE so the hidden DOM stays bounded; the remainder
        # is summarised with a "narrow the filters" note.
        outline_shown = evidences[: self.OUTLINE_MAX_EVIDENCE]
        # Region of the political function each posting person held at post time,
        # shown next to the actor in the table view. One grouped query over the
        # shown set (see `_regions_by_evidence`).
        region_by_ev = self._regions_by_evidence(outline_shown)
        outline_items = [
            {
                # `post` feeds the optional account/title line; it is the post
                # source. `url` always points at the evidence detail page
                # so every source type gets a working link. `posted_on` uses the
                # source's publication date.
                "post": ev.social_media_post,
                "url": ev.get_absolute_url(),
                "snippet": self._snippet(ev),
                "posted_on": ev.source.publication_date if ev.source else None,
                # Region of the originator's function at post time (table view).
                "region": region_by_ev.get(ev.pk, ""),
            }
            for ev in outline_shown
        ]
        outline_hidden_count = max(0, len(evidences) - len(outline_shown))
        _mark(f"outline ({len(outline_items)})")

        # Look up the currently-selected actor so the combobox button can
        # display its name on the initial server-rendered page.
        selected_actor = None
        raw_actor = (self.request.GET.get("actor") or "").strip()
        if raw_actor:
            try:
                selected_actor = Actor.objects.filter(pk=int(raw_actor)).first()
            except ValueError:
                pass

        # Keyword facet cloud over the *filtered* evidence set: only keywords
        # that still co-occur with the current selection remain. Several can be
        # active at once (AND); the active ones render as removable chips.
        # Enabled-only so a disabled keyword neither filters, nor shows as a
        # chip, nor lingers in the resubmitted form state.
        selected_keywords = self._selected_enabled_keywords(self.request.GET)
        facets = self._build_facets(
            evidences,
            selected_keywords,
            # An enabled keyword selection (or a theme) narrows the set too, so
            # it also turns on keyness — but only once disabled/unknown lemmas
            # are dropped.
            keyness=self._has_active_filter(self.request.GET)
            or bool(selected_keywords),
        )
        kw_label_by_lemma = {
            kw.lemma: kw.display_label
            for kw in Keyword.objects.filter(lemma__in=selected_keywords, enabled=True)
        }
        selected_facets = [
            {"lemma": lemma, "keyword": kw_label_by_lemma.get(lemma, lemma)}
            for lemma in selected_keywords
        ]
        _mark(f"facets ({len(facets)})")

        actors = list(
            Actor.objects.filter(
                social_media_accounts__posts__evidence__topic__isnull=False
            )
            .distinct()
            .order_by("name")
        )
        _mark(f"actors ({len(actors)})")

        # Originator-function filter options: the roles, institutional levels and
        # regions that actually occur on a political position of some person who
        # has posted topic-fitted evidence. Bounding to occurring values keeps
        # each dropdown to options that can yield a non-empty result (like the
        # actor list above). The three filters select against the *active*
        # position at post time (see `_political_position_q`); the options here
        # are just the universe of values, not time-bounded.
        pp_qs = PoliticalPosition.objects.filter(
            person__actor__social_media_accounts__posts__evidence__topic_fit_at__isnull=False
        )
        role_ids = set(
            pp_qs.filter(role__isnull=False).values_list("role_id", flat=True)
        )
        level_ids = set(
            pp_qs.filter(institutional_level__isnull=False).values_list(
                "institutional_level_id", flat=True
            )
        )
        region_ids = set(
            pp_qs.filter(region__isnull=False).values_list("region_id", flat=True)
        )
        roles = list(Role.objects.filter(id__in=role_ids).order_by("name"))
        levels = list(
            InstitutionalLevel.objects.filter(id__in=level_ids).order_by("name")
        )
        regions = list(GeoRegion.objects.filter(id__in=region_ids).order_by("name"))
        _mark(f"function options ({len(roles)}/{len(levels)}/{len(regions)})")

        # Year-range slider bounds: earliest/latest post year across the whole
        # topic-bearing corpus, so the slider extent stays fixed regardless of
        # the active filters (like the embedding bounds above). The current
        # selection is parsed back out of the date params the filter applies, so
        # an empty selection lands the handles at the full extent.
        year_agg = Evidence.objects.filter(
            # topic__isnull=False,
            social_media_post__posted_at__isnull=False,
        ).aggregate(
            earliest=Min("social_media_post__posted_at"),
            latest=Max("social_media_post__posted_at"),
        )
        year_min = year_agg["earliest"].year if year_agg["earliest"] else None
        year_max = year_agg["latest"].year if year_agg["latest"] else None
        selected_year_from = (
            self._param_year(self.request.GET.get("posted_after")) or year_min
        )
        selected_year_to = (
            self._param_year(self.request.GET.get("posted_before")) or year_max
        )
        _mark(f"year bounds ({year_min}-{year_max})")

        context.update(
            {
                "outline_items": outline_items,
                "outline_hidden_count": outline_hidden_count,
                "cloud_circles_svg": cloud_circles_svg,
                "cloud_point_count": cloud_point_count,
                "svg_width": self.SVG_WIDTH,
                "svg_height": self.SVG_HEIGHT,
                "evidence_count": len(evidences),
                "truncated": truncated,
                "max_evidence": self.MAX_EVIDENCE,
                "themes": theme_options,
                "selected_theme_id": selected_theme_id,
                "facets": facets,
                "selected_keywords": selected_keywords,
                "selected_facets": selected_facets,
                # Only actors that have actually posted — keeps the
                # searchable dropdown bounded to options that can yield
                # a non-empty result.
                "actors": actors,
                "actors_in_view": actors_in_view,
                "selected_actor": selected_actor,
                "platforms": SocialMediaAccount.Platform.choices,
                # Originator-function filters: the function (role), the
                # institutional level of that function, and its region, as held
                # by the posting person when the evidence was posted. Selected
                # values ride through `request.GET` in the template, like the
                # platform select.
                "roles": roles,
                "levels": levels,
                "regions": regions,
                "year_min": year_min,
                "year_max": year_max,
                "selected_year_from": selected_year_from,
                "selected_year_to": selected_year_to,
                "has_filters": any(
                    (self.request.GET.get(p) or "").strip()
                    for p in (
                        "q",
                        "theme",
                        "keyword",
                        "platform",
                        "posted_after",
                        "posted_before",
                        "actor",
                        "role",
                        "level",
                        "region",
                    )
                ),
                "reset_url": reverse("evidencecollection:evidence-topic-cloud"),
            }
        )
        _mark("context.update")
        return context
