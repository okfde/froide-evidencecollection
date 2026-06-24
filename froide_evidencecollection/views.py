import csv
import html
import io
import math
import sys
import time
from collections import defaultdict

from django.conf import settings
from django.core.exceptions import BadRequest
from django.db.models import Max, Min, Prefetch, Q, QuerySet, Sum
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
    Chapter,
    Evidence,
    EvidenceMention,
    InstitutionalLevel,
    Keyword,
    Organization,
    Person,
    PoliticalPosition,
    Role,
    SocialMediaAccount,
    Theme,
)
from .templatetags.evidence_tags import compact_number


def get_by_key(obj, key):
    parts = key.split("__")
    value = obj
    for part in parts:
        value = getattr(value, part)
    return str(value)


class EvidenceExporter:
    FORMATS = [
        "csv",
        "xlsx",
        # Disabled for now
        # "pdf"
    ]

    TABLE_EXPORT = [
        "id",
        "slug",
        "documentation_date",
        "citation",
        "description",
        "social_media_post__url",
        "text_segment_label",
        "text_segment_text",
    ]

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

    def export(self, queryset, related_object=None):
        queryset = (
            queryset.select_related("social_media_post").order_by("-pk").distinct()
        )

        return getattr(self, f"generate_{self.format}")(
            queryset, related_object=related_object
        )

    def _generate_table(self, rows):
        table = []
        table.append(self.TABLE_EXPORT)
        for evidence in rows:
            for text_segment in evidence.text_segments:
                table.append(
                    [
                        str(getattr(text_segment, key.replace("text_segment_", "")))
                        if key.startswith("text_segment_")
                        else get_by_key(evidence, key)
                        for key in self.TABLE_EXPORT
                    ]
                )
        return table

    def generate_csv(self, rows, related_object=None):
        f = io.StringIO()
        writer = csv.writer(f)
        writer.writerows(self._generate_table(rows))

        return f.getvalue().encode(), "text/csv"

    def generate_xlsx(self, rows, related_object=None):
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

    def generate_pdf(self, queryset, related_object=None):
        html = render_to_string(
            "froide_evidencecollection/pdf_export.html",
            context={
                "SITE_NAME": settings.SITE_NAME,
                "rows": queryset,
                "related_object": related_object,
            },
        )
        # Uncomment for testing
        # return html, "text/html"
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


class NoIndexMixin:
    """Keep a per-evidence view out of search engine indexes.

    `X-Robots-Tag: noindex` is honored by Google et al. and works no matter
    what the surrounding CMS page template emits in <head>. To actually
    de-index a page the crawler must be able to *read* this header, so the
    URL must stay crawlable (do not also Disallow it in robots.txt).
    """

    def dispatch(self, *args, **kwargs):
        response = super().dispatch(*args, **kwargs)
        response["X-Robots-Tag"] = "noindex"
        return response


def apphook_page_url(request):
    """Absolute URL of the CMS page the evidencecollection apphook is attached
    to, i.e. the overview page that hosts the topic-cloud plugin
    """
    page = getattr(request, "current_page", None)
    return page.get_absolute_url() if page else ""


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


class EvidenceDetailView(NoIndexMixin, EvidenceMixin, DetailView):
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
)

ACTOR_PROFILE_EVIDENCE_LIMIT = 20


class ActorDetailView(NoIndexMixin, DetailView):
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
            "person__political_positions",
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
        context["topic_cloud_page_url"] = apphook_page_url(self.request)

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
    filterset = EvidenceFilterSet
    document = EvidenceDocument
    model = Evidence
    search_url_name = "evidencecollection:evidence-export"
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


class ExportMixin:
    def get_export_queryset(self) -> QuerySet:
        raise NotImplementedError()

    def get_export_related_object(self):
        return

    def get(self, request, *args, **kwargs):
        format = request.GET.get("format", "pdf")
        if format not in EvidenceExporter.FORMATS:
            raise BadRequest("Invalid format")

        exporter = EvidenceExporter(format=format)
        related_obj = self.get_export_related_object()
        content, content_type = exporter.export(
            queryset=self.get_export_queryset(),
            related_object=related_obj,
        )

        filename = f"export_{related_obj.id}" if related_obj else "export"

        response = HttpResponse(content, content_type=content_type)
        response["Content-Disposition"] = f"inline; filename={filename}.{format}"
        return response


class NeverCacheMixin:
    def dispatch(self, *args, **kwargs):
        return never_cache(super().dispatch)(*args, **kwargs)


class EvidenceExportView(NoIndexMixin, NeverCacheMixin, ExportMixin, EvidenceListView):
    def get_export_queryset(self):
        import ipdb

        ipdb.set_trace()
        sqs = self.get_queryset()
        sqs.update_query()
        return sqs.to_queryset()


class EvidenceDetailExportView(
    NoIndexMixin, NeverCacheMixin, ExportMixin, EvidenceMixin, DetailView
):
    def get_export_queryset(self):
        queryset = self.get_queryset().filter(slug=self.kwargs["slug"])
        if not queryset.exists():
            raise Http404(
                _("No %(verbose_name)s found matching the query")
                % {"verbose_name": queryset.model._meta.verbose_name}
            )
        return queryset


class ActorDetailExportView(NoIndexMixin, NeverCacheMixin, ExportMixin, DetailView):
    model = Actor

    def get_export_related_object(self):
        return self.get_object()

    def get_export_queryset(self):
        actor = self.get_object()
        return actor.originated_evidence.all()


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

    # Safety bound on rows fetched from the DB. The cloud SVG renders one
    # circle per row; the screen-reader outline is further trimmed by
    # OUTLINE_MAX_EVIDENCE so the hidden HTML payload stays small. Set well
    # above the fitted corpus so it never trips in practice — it only exists
    # so the page degrades gracefully (via the "Result capped at…" notice)
    # should the corpus grow by an order of magnitude.
    MAX_EVIDENCE = 5000

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

    def get(self, request, *args, **kwargs):
        if request.headers.get("HX-Request") != "true":
            raise Http404
        return super().get(request, *args, **kwargs)

    def render_to_response(self, context, **response_kwargs):
        return HttpResponse(
            render_to_string(self.PARTIAL_TEMPLATE, context, request=self.request)
        )

    # Query params that narrow the evidence set in `_filter_qs`. If any is
    # present the facet ranking switches from frequency to keyness, since the
    # filtered set is then a real slice to contrast against the corpus.
    # `keyword` is handled separately (against the enabled selection), since a
    # disabled lemma in the URL is dropped and must not trigger keyness.
    NARROWING_PARAMS = (
        "q",
        "theme",
        "chapter",
        "platform",
        "posted_after",
        "posted_before",
        "actor",
        "role",
        "level",
        "verband",
    )

    # Relation path from an Evidence to the political positions held by an
    # originator who is a person. Evidence whose originators are all
    # organizations (or that has none) falls out.
    POLITICAL_POSITION_PREFIX = "originators__person__political_positions"

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

    @staticmethod
    def _selected_chapter_id(params):
        """Selected main topic = the first valid ``chapter`` query param (a
        Chapter pk), or ``None`` when none is set. The main-topic tree is
        single-select drill-down: clicking a node narrows the cloud to the
        evidence filed under that chapter or any of its descendants, so two
        selected nodes has no meaning. Non-numeric values are skipped; any extra
        ``chapter`` params are ignored. This is independent of the theme bar — the
        two stack (AND)."""
        for raw in params.getlist("chapter"):
            raw = (raw or "").strip()
            if raw.isdigit():
                return int(raw)
        return None

    @classmethod
    def _political_position_q(cls, params):
        """Filter on a *function the originator held* — an originator person's
        political position, narrowed by either of two params:

        * ``role`` — the function/role of that position (a ``Role`` pk),
        * ``level`` — its institutional level (an ``InstitutionalLevel`` pk).

        Both bind to a *single* position (one join), so combining them narrows
        to one position that matches both — "the same function".

        Returns a ``Q`` to AND into the queryset, or ``None`` when neither
        param is set. Only evidence with a person originator can match (the
        path runs through originators → person); the caller applies it with
        ``distinct()`` to fold the to-many originators join.
        """
        pp = cls.POLITICAL_POSITION_PREFIX
        position_q = None
        for name, field in (
            ("role", "role_id"),
            ("level", "institutional_level_id"),
        ):
            raw = (params.get(name) or "").strip()
            if raw.isdigit():
                cond = Q(**{f"{pp}__{field}": int(raw)})
                position_q = cond if position_q is None else position_q & cond
        return position_q

    @staticmethod
    def _verband_q(params):
        """Filter on the *Verband* of an originator — the regional chapter
        recorded on the posting Person/Organization (`AbstractActor.verband`),
        a ``GeoRegion`` pk in the ``verband`` param.

        Unlike the role/level function filters, this is a direct actor attribute,
        not tied to a political position or the post date. The originator is an
        ``Actor`` wrapping either a person or an organization, so the value can
        sit on either side; both are matched. Returns a ``Q`` to AND into the
        queryset (the caller folds the to-many originators join with
        ``distinct()``), or ``None`` when the param is absent or non-numeric.
        """
        raw = (params.get("verband") or "").strip()
        if not raw.isdigit():
            return None
        vid = int(raw)
        return Q(originators__person__verband_id=vid) | Q(
            originators__organization__verband_id=vid
        )

    @staticmethod
    def _originators_with_verband(evidences):
        """Map evidence pk → a display string pairing each originator with its
        own Verband, in originator order: ``"Ada Lovelace (Bayern), Acme
        (Bund)"``. The Verband (``"Bund"`` for the federal level, the Bundesland
        name otherwise — see `AbstractActor.verband_label`) is shown in
        parentheses after the name, and omitted entirely for an originator that
        has none. Evidence with no originator is absent from the map.
        """
        # Originator actor ids per evidence, in originator order (originators are
        # prefetched, so this iterates in-memory).
        actor_ids = set()
        ev_meta = []  # (evidence_pk, [actor_id, ...])
        for ev in evidences:
            ids = [actor.id for actor in ev.originators.all()]
            if ids:
                actor_ids.update(ids)
                ev_meta.append((ev.pk, ids))
        if not actor_ids:
            return {}

        # One query for the name + verband label of every originator in view; the
        # FK may sit on the person or the organization side of the Actor.
        # `verband` is a GeoRegion, whose geometry columns (`geom`, `geom_detail`,
        # `gov_seat`) are large multipolygons GEOS-deserialized per row — defer
        # them, since `verband_label` only reads `kind`/`name`. Without this the
        # join dominates the topic-cloud render (seconds).
        info_by_actor = {}  # actor_id -> (name, verband_label)
        for actor in (
            Actor.objects.filter(id__in=actor_ids)
            .select_related("person__verband", "organization__verband")
            .defer(
                "person__verband__geom",
                "person__verband__geom_detail",
                "person__verband__gov_seat",
                "organization__verband__geom",
                "organization__verband__geom_detail",
                "organization__verband__gov_seat",
            )
        ):
            target = actor.person or actor.organization
            label = target.verband_label if target else ""
            info_by_actor[actor.id] = (str(actor), label)

        result = {}
        for ev_pk, ids in ev_meta:
            parts = []
            for actor_id in ids:
                name, label = info_by_actor.get(actor_id, ("", ""))
                if not name:
                    continue
                parts.append(f"{name} ({label})" if label else name)
            if parts:
                result[ev_pk] = ", ".join(parts)
        return result

    @staticmethod
    def _post_interaction_stats(post):
        """Compact engagement line for a dot tooltip — the same view / like /
        comment / share counts the evidence detail page shows, each formatted
        with ``compact_number`` (1500 → "1.5K") and joined with separators.
        Empty when the post is absent or carries no counts.
        """
        if post is None:
            return ""
        parts = []
        for icon, value in (
            ("👁", post.view_count),
            ("❤", post.like_count),
            ("💬", post.comment_count),
            ("↗", post.share_count),
        ):
            if value:
                parts.append(f"{icon} {compact_number(value)}")
        return " · ".join(parts)

    @staticmethod
    def _chapters_by_evidence(evidences):
        """Map evidence pk → the chapter(s) it is filed under, as a display
        string (the chapters' ``custom_label``, comma-joined).

        Reads the prefetched ``mentions`` (which carry only ``chapter_id``); one
        extra query then resolves the labels for every chapter in view. Evidence
        whose mentions all lack a chapter is simply absent from the map.
        """
        chapter_ids = set()
        ev_meta = []  # (evidence_pk, [chapter_id, ...])
        for ev in evidences:
            ids = [m.chapter_id for m in ev.mentions.all() if m.chapter_id]
            if ids:
                chapter_ids.update(ids)
                ev_meta.append((ev.pk, ids))
        if not chapter_ids:
            return {}

        # One query for the label of every chapter in view.
        label_by_chapter = dict(
            Chapter.objects.filter(id__in=chapter_ids).values_list("id", "custom_label")
        )

        chapters = {}
        for ev_pk, ids in ev_meta:
            names = []
            for cid in ids:
                label = label_by_chapter.get(cid)
                if label and label not in names:
                    names.append(label)
            if names:
                chapters[ev_pk] = ", ".join(names)
        return chapters

    def _filter_qs(self):
        # `.only()` is load-bearing: SocialMediaPost has wide JSONFields
        # (`user_snapshot`, `reactions`) that would otherwise be fetched +
        # deserialized for every joined row, and they're not used here. The
        # account fields are pulled via select_related so the SR outline reads
        # them without an N+1. ``topic_fit_at__isnull=False`` is the "is fitted"
        # gate — only fitted evidence has the embedding coords the cloud plots;
        # the cluster itself is no longer surfaced (and the keyword fit no longer
        # sets the `topic` FK at all).
        # Account-derived facets traverse the `social_media_post` source.
        qs = (
            Evidence.objects.filter(topic_fit_at__isnull=False)
            .select_related(
                "social_media_post__account",
            )
            # `keywords` is the facet index, read in-memory by `_build_facets`;
            # prefetch only the enabled ones so curator-disabled keywords never
            # reach the view.
            .prefetch_related(
                Prefetch("keywords", queryset=Keyword.objects.filter(enabled=True)),
                # Originators drive the actor display/panel and the
                # verband-by-evidence read; person/organization are needed for
                # the actor's display name (`Actor.name`).
                "originators__person",
                "originators__organization",
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
                # `slug` backs `get_absolute_url()`, read once per circle in the
                # cloud loop — load it here so it isn't a deferred per-row query.
                "slug",
                "citation",
                "description",
                "topic_x",
                "topic_y",
                "social_media_post__url",
                "social_media_post__title",
                "social_media_post__text",
                "social_media_post__transcription",
                "social_media_post__posted_at",
                # Engagement counts for the dot tooltip's stats line — load
                # them here so reading each isn't a deferred per-row query.
                "social_media_post__view_count",
                "social_media_post__like_count",
                "social_media_post__comment_count",
                "social_media_post__share_count",
                "social_media_post__account__platform",
                "social_media_post__account__username",
            )
            .order_by("-social_media_post__posted_at", "-pk")
        )

        params = self.request.GET
        q = (params.get("q") or "").strip()
        if q:
            # The mentions join (a to-many) can multiply rows, so de-dupe.
            qs = qs.filter(
                Q(social_media_post__title__icontains=q)
                | Q(social_media_post__text__icontains=q)
                | Q(social_media_post__description__icontains=q)
                | Q(social_media_post__transcription__icontains=q)
                | Q(mentions__raw_transcript__icontains=q)
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

        # Main topic (report chapter): the hierarchical entry point, single-
        # select. Selecting a main-topic node narrows to evidence filed under
        # that chapter or any of its descendants (its subtree in the full chapter
        # tree) — so a parent node matches a superset of its children, the
        # "higher level → more evidence" behaviour of the tree. Independent of
        # the theme bar above — the two stack (AND). distinct() because the
        # mention join can match through several mentions.
        chapter_id = self._selected_chapter_id(params)
        if chapter_id is not None:
            chapter = Chapter.objects.filter(pk=chapter_id).first()
            if chapter is not None:
                subtree = Chapter.get_tree(chapter)
                qs = qs.filter(mentions__chapter__in=subtree).distinct()

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
                # originators is a to-many, so de-dupe.
                qs = qs.filter(originators__id=int(actor)).distinct()
            except ValueError:
                pass

        # Originator-function filters (role / institutional level of a political
        # position the posting person held). Bound to a single position via one
        # join, so distinct() to fold the to-many.
        pp_q = self._political_position_q(params)
        if pp_q is not None:
            qs = qs.filter(pp_q).distinct()

        # Verband filter: the regional chapter recorded on an originator
        # (person or organization). A direct actor attribute, so independent of
        # the function filters above; distinct() folds the to-many join.
        verband_q = self._verband_q(params)
        if verband_q is not None:
            qs = qs.filter(verband_q).distinct()

        return qs

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

    def _build_main_topic_tree(self, params):
        """Hierarchical main-topic filter data, drawn from the chapter tree.

        The report's chapters form a deep tree (``Chapter``), of which only the
        nodes flagged ``is_main_topic`` name a thematic topic. This builds a
        *condensed* tree of those main-topic nodes alone: each one hangs off its
        nearest main-topic ancestor, collapsing the non-main intermediate
        chapters between them (main topics aren't located at a uniform depth, so
        the gaps are merged away). The result is a single pre-order-flattened
        list ready for an indented render.

        Coverage is cumulative and corpus-wide: a node's ``count`` is the number
        of distinct topic-fitted evidence filed under that chapter *or any of its
        descendants* (the same subtree the filter narrows to). A parent therefore
        always subsumes its children — the higher the level, the more evidence
        matches. Only nodes that subsume at least one evidence are kept; since a
        parent's count is ≥ each child's, dropping the empties never orphans a
        surviving child.

        The tree is collapsed by default: only root nodes are ``visible``, and a
        node is ``expanded`` (its children revealed) only along the path to the
        selected node, so a drill-down keeps its context after the section is
        re-rendered.

        Returns ``(selected_chapter_id, nodes)`` where each node is
        ``{id, parent_id, label, count, depth, guides, has_children, expanded,
        visible, selected}`` (``guides`` is one entry per ancestor level, for
        drawing the indentation rails), pre-order flattened with siblings ordered
        by coverage (then label) so the biggest topics lead.
        """
        selected_chapter_id = self._selected_chapter_id(params)

        chapters = list(
            Chapter.objects.only("id", "path", "is_main_topic", "custom_label")
        )
        by_path = {c.path: c for c in chapters}
        label_of = {c.id: c.custom_label for c in chapters}
        steplen = Chapter.steplen

        # For every chapter, the main-topic node ids on its root-to-leaf path,
        # nearest first (including itself when it is a main topic). Walking the
        # materialised path upward in fixed ``steplen`` chunks yields the
        # ancestors; this is the backbone for both the condensed parent links and
        # the cumulative coverage tally.
        main_chain: dict[int, list[int]] = {}
        for c in chapters:
            chain = []
            path = c.path
            while path:
                node = by_path.get(path)
                if node is not None and node.is_main_topic:
                    chain.append(node.id)
                path = path[:-steplen]  # step to the parent path
            main_chain[c.id] = chain

        # Distinct topic-fitted evidence per main-topic node, in one pass over the
        # mention↔chapter pairs: a mention at chapter ``c`` counts toward every
        # main-topic node on ``c``'s path, so subtree subsumption falls out for
        # free (and a node inherits all its descendants' evidence).
        coverage: dict[int, set[int]] = defaultdict(set)
        pairs = EvidenceMention.objects.filter(
            evidence__topic_fit_at__isnull=False, chapter__isnull=False
        ).values_list("evidence_id", "chapter_id")
        for ev_id, ch_id in pairs:
            for mt_id in main_chain.get(ch_id, ()):
                coverage[mt_id].add(ev_id)
        counts = {mt_id: len(ev_ids) for mt_id, ev_ids in coverage.items()}

        # Condensed parent links among the evidence-bearing main-topic nodes:
        # a node's parent is the second entry of its chain (the first being
        # itself); a node with no main-topic ancestor is a root.
        children: dict[int, list[int]] = defaultdict(list)
        roots: list[int] = []
        for c in chapters:
            if not c.is_main_topic or counts.get(c.id, 0) == 0:
                continue
            chain = main_chain[c.id]
            parent_id = chain[1] if len(chain) > 1 else None
            if parent_id is None:
                roots.append(c.id)
            else:
                children[parent_id].append(c.id)

        # Drop a stale/empty selection so the active state stays honest.
        if selected_chapter_id is not None and counts.get(selected_chapter_id, 0) == 0:
            selected_chapter_id = None

        # Collapsed by default: expand only the selected node and its ancestors
        # (its whole main-chain), so the drilled-into path stays open and the
        # selected node's children are revealed; everything else starts closed.
        expanded_ids: set[int] = set()
        if selected_chapter_id is not None:
            expanded_ids = set(main_chain.get(selected_chapter_id, ()))

        # Pre-order flatten, siblings by coverage then label. Each node carries
        # its depth (indentation rails), parent id and child/expanded/visible
        # flags (collapse state) for the template + client toggle.
        nodes: list[dict] = []

        def _walk(node_id, depth, parent_id, parent_visible, parent_expanded):
            visible = depth == 0 or (parent_visible and parent_expanded)
            expanded = node_id in expanded_ids
            nodes.append(
                {
                    "id": node_id,
                    "parent_id": parent_id,
                    "label": label_of.get(node_id, ""),
                    "count": counts.get(node_id, 0),
                    "depth": depth,
                    # One guide cell per ancestor level, so the template can draw
                    # a vertical connector rail at each level of indentation.
                    "guides": list(range(depth)),
                    "has_children": bool(children[node_id]),
                    "expanded": expanded,
                    "visible": visible,
                    "selected": node_id == selected_chapter_id,
                }
            )
            for kid in sorted(
                children[node_id], key=lambda cid: (-counts[cid], label_of[cid])
            ):
                _walk(kid, depth + 1, node_id, visible, expanded)

        for root_id in sorted(roots, key=lambda cid: (-counts[cid], label_of[cid])):
            _walk(root_id, 0, None, True, True)

        return selected_chapter_id, nodes

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

    # ------------------------------------------------------------------
    # Actor surfaces. The actor of an evidence is its `originators` (the
    # import-populated relation); the scraped account is never linked to an
    # actor. All four read the prefetched `originators`, so they cost no extra
    # query, and are pure functions of their inputs so they unit-test without a
    # request.
    # ------------------------------------------------------------------
    @staticmethod
    def _originator_ids(evidence):
        """Space-separated originator ids for a dot's ``data-actor`` hook (an
        evidence may have several); the side panel highlights by membership."""
        return " ".join(str(a.id) for a in evidence.originators.all())

    @staticmethod
    def _actors_in_view(evidences):
        """The "Actors in view" side panel: each originator across the visible
        evidence with the number of those evidence it originated, sorted by
        descending count then name. An evidence with several originators counts
        toward each of them."""
        actor_counts = {}
        actor_objs = {}
        for ev in evidences:
            for actor in ev.originators.all():
                actor_counts[actor.id] = actor_counts.get(actor.id, 0) + 1
                actor_objs[actor.id] = actor
        return sorted(
            (
                {"pk": pk, "name": str(actor_objs[pk]), "count": count}
                for pk, count in actor_counts.items()
            ),
            key=lambda a: (-a["count"], a["name"].lower()),
        )

    @staticmethod
    def _actor_options():
        """Actors that originated at least one topic-fitted evidence — the
        searchable dropdown's options, bounded to values that can yield a
        non-empty result. `Actor.name` is a Python property (not a column), so
        sort in Python after select_relating its person/organization."""
        return sorted(
            Actor.objects.filter(originated_evidence__topic_fit_at__isnull=False)
            .distinct()
            .select_related("person", "organization"),
            key=lambda a: a.name.casefold(),
        )

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
            topic_fit_at__isnull=False,
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

        # Main-topic bar: a hierarchical, single-select filter over the report's
        # `is_main_topic` chapters (condensed so each node hangs off its nearest
        # main-topic ancestor). Independent of the theme bar — the two stack
        # (AND). Coverage is corpus-wide and cumulative, so the order/counts
        # don't reshuffle as the user drills in.
        selected_chapter_id, main_topics = self._build_main_topic_tree(self.request.GET)
        _mark(f"main topics ({len(main_topics)})")

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
        # Originator-with-Verband and chapter display strings, computed once over
        # the whole filtered set (two grouped queries each — see the helpers) and
        # shared by both the dot tooltips and the outline/table below. The dot
        # tooltip mirrors the table's metadata columns, so it needs the same maps.
        originators_by_ev = self._originators_with_verband(evidences)
        chapters_by_ev = self._chapters_by_evidence(evidences)
        esc = html.escape
        circle_parts = []
        for pt in self._project(plottable, bounds=bounds):
            ev = pt["post"]
            # Account- and engagement-derived bits come from the social-media-
            # post source.
            post = ev.social_media_post if ev.social_media_post_id else None
            account = post.account if post else None
            platform = account.get_platform_display() if account else ""
            username = account.username if account and account.username else ""
            # The dot's originators (space-separated ids), so the side panel can
            # highlight one actor's dots — an evidence may have several.
            actor_id = self._originator_ids(ev)
            pub_date = ev.source.publication_date if ev.source else None
            posted_on = pub_date.isoformat() if pub_date else ""
            # `data-theme` carries the dot's own dominant theme (for a later
            # client-side highlight); the fill applies the selection lens.
            theme_id = self._dominant_theme(ev, theme_by_chapter, theme_order)
            fill = self._dot_fill(lens_color, theme_id, theme_color)
            # Tooltip metadata — the same columns the table shows (originator
            # with Verband, chapters); no text snippet. `data-stats` adds the
            # engagement line from the evidence detail view (views/likes/…).
            originators = originators_by_ev.get(ev.pk, "")
            chapters = chapters_by_ev.get(ev.pk, "")
            stats = self._post_interaction_stats(post)
            circle_parts.append(
                f'<circle data-href="{esc(ev.get_absolute_url())}"'
                f' data-platform="{esc(platform)}"'
                f' data-username="{esc(username)}"'
                f' data-actor="{actor_id}"'
                f' data-theme="{theme_id if theme_id is not None else ""}"'
                f' data-posted-on="{posted_on}"'
                f' data-originators="{esc(originators)}"'
                f' data-chapters="{esc(chapters)}"'
                f' data-stats="{esc(stats)}"'
                f' cx="{pt["cx"]}" cy="{pt["cy"]}"'
                f' r="4"'
                f' fill="{fill}"></circle>'
            )
        cloud_circles_svg = mark_safe("".join(circle_parts))
        cloud_point_count = len(circle_parts)
        _mark(f"cloud_circles ({cloud_point_count})")

        # Actors present in the filtered set, tallied over the visible evidence
        # via each evidence's originators (prefetched, so no extra per-row
        # query). Drives the "Actors in view" side panel; clicking a name
        # highlights that actor's dots client-side rather than filtering.
        actors_in_view = self._actors_in_view(evidences)
        _mark(f"actors_in_view ({len(actors_in_view)})")

        # SR-only / mobile outline: a single flat list of the matching
        # evidence, newest first (the queryset is already date-ordered). Capped
        # at OUTLINE_MAX_EVIDENCE so the hidden DOM stays bounded; the remainder
        # is summarised with a "narrow the filters" note.
        outline_shown = evidences[: self.OUTLINE_MAX_EVIDENCE]
        # The originator-with-Verband and chapter maps are computed above (over
        # the whole filtered set) for the dot tooltips; the outline reuses them.
        outline_items = [
            {
                # `post` feeds the optional account/platform line; it is the post
                # source. `url` always points at the evidence detail page
                # so every source type gets a working link. `posted_on` uses the
                # source's publication date.
                "post": ev.social_media_post,
                "url": ev.get_absolute_url(),
                "posted_on": ev.source.publication_date if ev.source else None,
                # Originator(s) each with their own Verband, e.g.
                # "Name (Bayern), Other (Bund)" — table view.
                "originators": originators_by_ev.get(ev.pk, ""),
                # Chapter(s) the evidence is filed under (table view).
                "chapters": chapters_by_ev.get(ev.pk, ""),
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

        actors = self._actor_options()
        _mark(f"actors ({len(actors)})")

        # Originator-function filter options: the roles and institutional levels
        # that actually occur on a political position of some person who has
        # posted topic-fitted evidence. Bounding to occurring values keeps each
        # dropdown to options that can yield a non-empty result (like the actor
        # list above). The filters select against a matching position (see
        # `_political_position_q`); the options here are just the universe of
        # values.
        # `.distinct()` is load-bearing: the `originated_evidence` join multiplies
        # each position by every topic-fitted evidence its person originated, so
        # without it the DB streams that full (heavily duplicated) row set back
        # just to collapse it into a `set()` here. DISTINCT collapses it in the DB.
        pp_qs = PoliticalPosition.objects.filter(
            person__actor__originated_evidence__topic_fit_at__isnull=False
        )
        role_ids = set(
            pp_qs.filter(role__isnull=False)
            .values_list("role_id", flat=True)
            .distinct()
        )
        level_ids = set(
            pp_qs.filter(institutional_level__isnull=False)
            .values_list("institutional_level_id", flat=True)
            .distinct()
        )
        roles = list(Role.objects.filter(id__in=role_ids).order_by("name"))
        levels = list(
            InstitutionalLevel.objects.filter(id__in=level_ids).order_by("name")
        )

        # Verband filter options: the verbände recorded on originators (person or
        # organization) that have posted topic-fitted evidence. Labelled like the
        # display ("Bund" for the country level), with Bund first then the
        # Bundesländer alphabetically.
        verband_ids = set()
        for model in (Person, Organization):
            verband_ids.update(
                model.objects.filter(
                    actor__originated_evidence__topic_fit_at__isnull=False,
                    verband__isnull=False,
                )
                # Same join-multiplication as above; dedupe in the DB.
                .values_list("verband_id", flat=True)
                .distinct()
            )
        # `verband` is a GeoRegion; defer its large geometry columns since only
        # `kind`/`name` are read here (see `_originators_with_verband`).
        verbaende = sorted(
            (
                {"id": r.id, "label": "Bund" if r.kind == "country" else r.name}
                for r in GeoRegion.objects.filter(id__in=verband_ids).defer(
                    "geom", "geom_detail", "gov_seat"
                )
            ),
            key=lambda v: (v["label"] != "Bund", v["label"]),
        )
        _mark(f"function options ({len(roles)}/{len(levels)}/{len(verbaende)})")

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
                "main_topics": main_topics,
                "selected_chapter_id": selected_chapter_id,
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
                # Originator-function filters: the function (role) and its
                # institutional level, as held by the posting person when the
                # evidence was posted. Selected values ride through
                # `request.GET` in the template, like the platform select.
                "roles": roles,
                "levels": levels,
                # Verband of the originator (a direct actor attribute, not
                # function-derived).
                "verbaende": verbaende,
                "year_min": year_min,
                "year_max": year_max,
                "selected_year_from": selected_year_from,
                "selected_year_to": selected_year_to,
                "has_filters": any(
                    (self.request.GET.get(p) or "").strip()
                    for p in (
                        "q",
                        "theme",
                        "chapter",
                        "keyword",
                        "platform",
                        "posted_after",
                        "posted_before",
                        "actor",
                        "role",
                        "level",
                        "verband",
                    )
                ),
                "reset_url": apphook_page_url(self.request),
                "topic_cloud_url": reverse("evidencecollection:evidence-topic-cloud"),
            }
        )
        _mark("context.update")
        return context
