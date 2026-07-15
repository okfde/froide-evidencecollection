import csv
import html
import io
import logging
from collections import defaultdict
from datetime import date

from django.core.exceptions import BadRequest
from django.db.models import F, Max, Min, Prefetch, Q, QuerySet
from django.http import Http404, HttpResponse
from django.urls import reverse
from django.utils.functional import cached_property
from django.utils.safestring import mark_safe
from django.utils.translation import gettext as _
from django.views.decorators.cache import never_cache
from django.views.generic import DetailView, TemplateView

from elasticsearch_dsl.query import Q as SearchQ

from froide.georegion.models import GeoRegion
from froide.helper.breadcrumbs import BreadcrumbView
from froide.helper.search import get_query_preprocessor

from .documents import EvidenceDocument
from .models import (
    Actor,
    Chapter,
    Evidence,
    EvidenceMention,
    InstitutionalLevel,
    Organization,
    Person,
    PoliticalPosition,
    Role,
    SocialMediaAccount,
)
from .templatetags.evidence_tags import compact_number

logger = logging.getLogger(__name__)


class SearchUnavailable(Exception):
    """Elasticsearch could not answer a free-text query."""


# A post segment's `kind` -> the column its text lands in.
SEGMENT_COLUMNS = {
    "title": "post_title",
    "body": "post_text",
    "description": "post_description",
}

# GeoRegion's geometry columns are large multipolygons that get GEOS-deserialized
# per row. Defer them wherever only `kind`/`name` is readq.
GEOREGION_GEOMETRY_FIELDS = ("geom", "geom_detail", "gov_seat")

# The same columns as seen through an Actor: the FK may sit on the person or the
# organization side.
VERBAND_GEOMETRY_FIELDS = tuple(
    f"{side}__verband__{field}"
    for side in ("person", "organization")
    for field in GEOREGION_GEOMETRY_FIELDS
)


def actor_queryset() -> QuerySet:
    """Actors carrying everything their display columns read: the person /
    organization side backs `Actor.name`, and its Verband backs `verband_label`
    (which reads only `kind`/`name`, so the geometry is deferred).
    """
    return Actor.objects.select_related(
        "person__verband", "organization__verband"
    ).defer(*VERBAND_GEOMETRY_FIELDS)


def originator_prefetch() -> Prefetch:
    """The originators of the exported mentions, with everything their columns read."""
    return Prefetch("mentions__originator", queryset=actor_queryset())


class EvidenceExporter:
    """Exports evidence as one row per mention — one footnote of the report."""

    FORMATS = [
        "csv",
        "xlsx",
    ]

    TABLE_EXPORT = [
        "slug",
        "documentation_date",
        "post_url",
        "post_date",
        "posted_by",
        "post_title",
        "post_text",
        "post_description",
        "repost_text",
        "repost_attribution",
        "footnote",
        "originator",
        "political_position",
        "verband",
        "chapter",
        "start",
        "end",
        "citation",
        "report_url",
    ]

    def __init__(self, format):
        if format not in self.FORMATS:
            raise ValueError(f"format {format} is not supported")
        self.format = format

    def export(self, queryset, related_object=None):
        queryset = (
            queryset.select_related(
                "social_media_post",
                "social_media_post__account__actor__person",
                "social_media_post__account__actor__organization",
                "social_media_post__redistributes__account__actor__person",
                "social_media_post__redistributes__account__actor__organization",
            )
            .prefetch_related(
                "social_media_post__redaction_rules",
                "mentions__chapter",
                originator_prefetch(),
            )
            .order_by("-pk")
            .distinct()
        )

        return getattr(self, f"generate_{self.format}")(
            queryset, related_object=related_object
        )

    def _evidence_columns(self, evidence) -> dict[str, str]:
        # Read off the redacted text block, not off the raw post fields: the
        # export leaves the system, so masked terms must not ride along.
        source = evidence.source
        columns = {
            "slug": evidence.slug,
            "documentation_date": str(evidence.documentation_date or ""),
            "post_url": evidence.url,
            "post_date": str(source.publication_date or "") if source else "",
            # `account` is a social media post's, not part of `EvidenceSource`.
            "posted_by": str(source.account) if source else "",
        }
        block = evidence.redacted_text_block
        if block is None:
            return columns

        for segment in block.segments:
            columns[SEGMENT_COLUMNS[segment.kind]] = segment.text
        if block.repost:
            columns["repost_text"] = block.repost.text
            columns["repost_attribution"] = block.repost.attribution
        return columns

    def _mention_columns(self, mention) -> dict[str, str]:
        chapter = " > ".join(mention.chapter_structure) or str(mention.chapter or "")
        originator = mention.originator

        return {
            "footnote": mention.footnote,
            "originator": originator.name,
            "political_position": originator.political_position_label or "",
            "verband": originator.target.verband_label,
            "chapter": chapter,
            "start": str(mention.start or ""),
            "end": str(mention.end or ""),
            "citation": mention.redacted_citation,
            "report_url": mention.report_url,
        }

    def _generate_table(self, rows):
        table = []
        table.append(self.TABLE_EXPORT)
        for evidence in rows:
            evidence_columns = self._evidence_columns(evidence)
            mentions = evidence.mentions.all()
            rows_columns = [
                {**evidence_columns, **self._mention_columns(mention)}
                for mention in mentions
            ] or [evidence_columns]
            for columns in rows_columns:
                table.append([columns.get(key, "") for key in self.TABLE_EXPORT])
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


class AppHookBreadcrumbMixin(BreadcrumbView):
    """Breadcrumbs for internal pages served under the apphook page."""

    def get_breadcrumbs(self, context):
        request = context.get("request")
        page = getattr(request, "current_page", None) if request else None
        if page is None:
            return []

        pages = list(page.get_ancestor_pages()) + [page]
        return [
            (p.get_menu_title(), p.get_absolute_url()) for p in pages if not p.is_home
        ]


class EvidenceMixin(AppHookBreadcrumbMixin):
    def get_queryset(self):
        return Evidence.objects.all()


class EvidenceDetailView(NoIndexMixin, EvidenceMixin, DetailView):
    template_name = "froide_evidencecollection/detail.html"

    def get_queryset(self):
        # The rules are prefetched because both the post's text block and each
        # mention's citation are redacted against them.
        return Evidence.objects.select_related(
            "social_media_post__account",
        ).prefetch_related(
            "originators__organization__institutional_level",
            "mentions__originator",
            "mentions__chapter",
            "social_media_post__redaction_rules",
        )

    def get_breadcrumbs(self, context):
        obj = self.object
        return super().get_breadcrumbs(context) + [
            (_("Evidence #%s") % obj.pk, obj.get_absolute_url()),
        ]


# Prefetches shared by every place that renders a list of evidence cards.
EVIDENCE_CARD_SELECT_RELATED = ("social_media_post__account",)
EVIDENCE_CARD_PREFETCH_RELATED = ("originators__organization__institutional_level",)

ACTOR_PROFILE_EVIDENCE_LIMIT = 20


class ActorDetailView(NoIndexMixin, AppHookBreadcrumbMixin, DetailView):
    model = Actor
    template_name = "froide_evidencecollection/actor_detail.html"
    context_object_name = "actor"

    def get_queryset(self):
        return Actor.objects.select_related(
            "person",
            "organization",
            "organization__institutional_level",
        ).prefetch_related(
            "social_media_accounts",
        )

    def get_breadcrumbs(self, context):
        actor = self.object
        return super().get_breadcrumbs(context) + [
            (str(actor), actor.get_absolute_url()),
        ]

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        actor = self.object

        # Evidence originated by this actor (the `originators` M2M on Evidence).
        # The actor profile lists each piece by date / platform / chapter, so the
        # rows carry the mentions' chapters on top of the shared card prefetch.
        list_prefetch = (*EVIDENCE_CARD_PREFETCH_RELATED, "mentions__chapter")
        # Most recent first, by the same date the rows show (the post's
        # `posted_at`); undated pieces sort last, with `-pk` as a stable
        # tiebreaker.
        date_ordering = (
            F("social_media_post__posted_at").desc(nulls_last=True),
            "-pk",
        )
        originated = (
            Evidence.objects.filter(originators=actor)
            .select_related(*EVIDENCE_CARD_SELECT_RELATED)
            .prefetch_related(*list_prefetch)
            .order_by(*date_ordering)
            .distinct()
        )
        context["originated_evidence"] = self._with_chapters(
            originated[:ACTOR_PROFILE_EVIDENCE_LIMIT]
        )
        context["originated_total"] = originated.count()
        context["evidence_limit"] = ACTOR_PROFILE_EVIDENCE_LIMIT
        context["topic_cloud_page_url"] = apphook_page_url(self.request)

        return context

    @staticmethod
    def _with_chapters(evidence_iterable):
        """Attach each evidence's distinct chapter labels as ``chapters``.

        A piece is filed under a chapter through each of its mentions. We show
        the leaf chapter the piece is filed under: the linked ``chapter`` node,
        falling back to the last label of the mention's ``chapter_structure``
        (the root-to-leaf path) when no node is linked. Reads only the
        prefetched ``mentions`` and deduplicates while preserving order.
        """
        evidence_list = list(evidence_iterable)
        for evidence in evidence_list:
            chapters = []
            for mention in evidence.mentions.all():
                if mention.chapter_id:
                    label = str(mention.chapter)
                elif mention.chapter_structure:
                    label = mention.chapter_structure[-1]
                else:
                    label = ""
                if label and label not in chapters:
                    chapters.append(label)
            evidence.chapters = chapters
        return evidence_list


class ExportMixin:
    def get_export_queryset(self) -> QuerySet:
        raise NotImplementedError()

    def get_export_related_object(self):
        return

    def get(self, request, *args, **kwargs):
        format = request.GET.get("format", "csv")
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
    """View over topic-fitted pieces of evidence, browsed by main topic.

    The primary structure is a server-rendered, screen-reader-navigable
    outline listing the matching evidence. A small SVG scatter sits on top
    as a visual aid — ``aria-hidden`` because the list below carries the
    same information in semantic form. Browsing is by main-topic tree; the
    toolbar contains additional filters.

    The free-text search runs against Elasticsearch (`EvidenceDocument`), whose
    `content` field is the redacted `Evidence.search_text`. Every other filter
    is a plain ORM narrowing; the two meet as a `pk__in` over the ids the index
    returns.

    Dot *positions* come from the fit's 2D embedding (``topic_x`` /
    ``topic_y``); every dot is drawn in the same neutral ink, set in CSS.

    Account-derived filters (platform, actor) are sourced from each evidence's
    social-media-post source.
    """

    # Safety bound on rows fetched from the DB, and on the ids the free-text
    # search pulls out of the index. The cloud SVG renders one circle per row;
    # the screen-reader outline is further trimmed by
    # OUTLINE_MAX_EVIDENCE so the hidden HTML payload stays small. Set well
    # above the fitted corpus so it never trips in practice — it only exists
    # so the page degrades gracefully (via the "Result capped at…" notice)
    # should the corpus grow by an order of magnitude.
    MAX_EVIDENCE = 5000

    # Evidence listed in the SR-only / mobile outline. Keeps the hidden DOM
    # bounded even when the filtered set is large; users hunting a specific
    # item can narrow via the toolbar filters.
    OUTLINE_MAX_EVIDENCE = 100

    # SVG viewport. The data x/y are projected into this box; the actual
    # rendered size is fluid (width:100%) so it adapts to mobile widths.
    SVG_WIDTH = 1000
    SVG_HEIGHT = 600
    SVG_PADDING = 16

    # This view only ever answers htmx's in-place filter refresh, so it renders
    # the fragment alone — the context `get_context_data` builds. The full page
    # is assembled by the CMS plugin from topic_cloud.html, which wraps that
    # fragment in the filter toolbar and so asks for `get_page_context`.
    template_name = "froide_evidencecollection/_topic_cloud_partial.html"

    def get(self, request, *args, **kwargs):
        if request.headers.get("HX-Request") != "true":
            raise Http404
        return super().get(request, *args, **kwargs)

    # Relation path from an Evidence to the political positions held by an
    # originator who is a person. Evidence whose originators are all
    # organizations (or that has none) falls out.
    POLITICAL_POSITION_PREFIX = "originators__person__political_positions"

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
    def _param_int(params, name):
        """Parse a query param as an integer pk, or None when it is absent or
        not all-digits."""
        raw = (params.get(name) or "").strip()
        return int(raw) if raw.isdigit() else None

    @staticmethod
    def _param_date(params, name):
        """Parse a ``YYYY-MM-DD`` query param as a date, or None when it is
        absent or not a valid ISO date."""
        raw = (params.get(name) or "").strip()
        try:
            return date.fromisoformat(raw)
        except ValueError:
            return None

    @staticmethod
    def _selected_chapter_id(params):
        """Selected main topic = the first valid ``chapter`` query param (a
        Chapter pk), or ``None`` when none is set. The main-topic tree is
        single-select drill-down: clicking a node narrows the cloud to the
        evidence filed under that chapter or any of its descendants, so two
        selected nodes has no meaning. Non-numeric values are skipped; any extra
        ``chapter`` params are ignored."""
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
            value = cls._param_int(params, name)
            if value is not None:
                cond = Q(**{f"{pp}__{field}": value})
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
        vid = EvidenceTopicCloudView._param_int(params, "verband")
        if vid is None:
            return None
        return Q(originators__person__verband_id=vid) | Q(
            originators__organization__verband_id=vid
        )

    def _search_ids(self, q):
        """Evidence pks whose indexed text matches the free-text query ``q``.

        Only document ids are requested (`source(False)`) — the rows themselves
        come from the ORM, which the remaining filters narrow. Capped at
        MAX_EVIDENCE + 1 so the caller's "capped" detection still trips on an
        over-large result. Raises `SearchUnavailable` if the index cannot answer.
        """
        query = get_query_preprocessor().prepare_query(q)
        search = (
            EvidenceDocument.search()
            .query(
                SearchQ(
                    "simple_query_string",
                    query=query,
                    fields=["content"],
                    default_operator="and",
                    lenient=True,
                )
            )
            .source(False)[: self.MAX_EVIDENCE + 1]
        )
        try:
            response = search.execute()
        except Exception as e:
            logger.error("Elasticsearch error on topic cloud search: %s", e)
            raise SearchUnavailable from e
        return [int(hit.meta.id) for hit in response]

    @staticmethod
    def _originators_with_verband(evidences):
        """Map evidence pk → a display string pairing each originator with its
        own Verband, in originator order: ``"Ada Lovelace (Bayern), Acme
        (Bund)"``. The Verband (``"Bund"`` for the federal level, the Bundesland
        name otherwise — see `AbstractActor.verband_label`) is shown in
        parentheses after the name, and omitted entirely for an originator that
        has none. Evidence with no originator is absent from the map.

        The originators are prefetched with their person / organization and its
        Verband, so this costs no query.
        """
        result = {}
        for ev in evidences:
            parts = []
            for actor in ev.originators.all():
                target = actor.person or actor.organization
                name = str(target) if target else ""
                if not name:
                    continue
                label = target.verband_label
                parts.append(f"{name} ({label})" if label else name)
            if parts:
                result[ev.pk] = ", ".join(parts)
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

    @cached_property
    def main_topic_tree(self):
        """``(selected_chapter_id, nodes)`` for this request — the single source
        of truth for the main-topic selection.

        The ``chapter`` param is validated here and nowhere else: the id it
        yields is one the tree actually renders as a selectable node, so the
        filter, the tree's active state and the Reset button can never disagree
        about whether a main topic is selected. Cached because the filtered
        queryset, the tree and the Reset button all need it within one request.
        """
        return self._build_main_topic_tree(self.request.GET)

    def _filter_qs(self):
        # `.only()` is load-bearing: SocialMediaPost has wide JSONFields
        # (`user_snapshot`, `reactions`) that would otherwise be fetched +
        # deserialized for every joined row, and they're not used here. The
        # account fields are pulled via select_related so the SR outline reads
        # them without an N+1. ``topic_fit_at__isnull=False`` is the "is fitted"
        # gate — only fitted evidence has the embedding coords the cloud plots.
        # Account-derived filters traverse the `social_media_post` source.
        qs = (
            Evidence.objects.filter(topic_fit_at__isnull=False)
            .select_related(
                "social_media_post__account",
            )
            .prefetch_related(
                # Originators drive the actor display/panel and the
                # verband-by-evidence read; both the display name and the
                # Verband label ride along on the actor, so those two read
                # in-memory.
                Prefetch("originators", queryset=actor_queryset()),
                # Mentions back the per-dot chapter display.
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
                "topic_x",
                "topic_y",
                # `posted_at` backs the outline's date (via `source.publication_date`)
                # and the queryset ordering.
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
        # Free text: answered by the index, not by SQL — see `_search_ids`. The
        # ids come back unordered; the queryset's own date ordering stands, which
        # is what the cloud wants (a scatter, not a ranked list).
        q = (params.get("q") or "").strip()
        if q:
            qs = qs.filter(pk__in=self._search_ids(q))

        # Main topic (report chapter): the hierarchical entry point, single-
        # select. Selecting a main-topic node narrows to evidence filed under
        # that chapter or any of its descendants (its subtree in the full chapter
        # tree) — so a parent node matches a superset of its children, the
        # "higher level → more evidence" behaviour of the tree. distinct()
        # because the mention join can match through several mentions.
        chapter_id, _nodes = self.main_topic_tree
        if chapter_id is not None:
            chapter = Chapter.objects.filter(pk=chapter_id).first()
            subtree = Chapter.get_tree(chapter)
            qs = qs.filter(mentions__chapter__in=subtree).distinct()

        platform = (params.get("platform") or "").strip()
        if platform:
            qs = qs.filter(social_media_post__account__platform=platform)

        for name, lookup in (
            ("posted_after", "social_media_post__posted_at__date__gte"),
            ("posted_before", "social_media_post__posted_at__date__lte"),
        ):
            value = self._param_date(params, name)
            if value is not None:
                qs = qs.filter(**{lookup: value})

        actor = self._param_int(params, "actor")
        if actor is not None:
            # originators is a to-many, so de-dupe.
            qs = qs.filter(originators__id=actor).distinct()

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

    def _project(self, posts, bounds):
        """Map post x/y into SVG pixel coordinates. Coords are formatted as
        plain strings (always a ``.`` decimal) so Django's locale-aware
        templating doesn't slip a German comma into the SVG attributes.

        ``bounds`` (``(xmin, xmax, ymin, ymax)``) pins the projection to the
        unfiltered dataset's extents so dots keep the same screen position when
        filters shrink the visible set. It is None only when nothing is fitted,
        in which case there is nothing to plot either.
        """
        if not posts or bounds is None:
            return []
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
        """Context for the htmx partial: the filtered set and everything drawn
        from it.
        """
        context = super().get_context_data(**kwargs)
        context.update(self._results_context())
        return context

    def get_page_context(self):
        """Context for the CMS plugin's full page — the partial's, plus the
        toolbar's.
        """
        return {**self.get_context_data(), **self._toolbar_context()}

    def _results_context(self):
        search_unavailable = False
        try:
            qs = self._filter_qs()
            # Fetch one extra row to detect "more than MAX_EVIDENCE" without
            # running a second COUNT query against the filtered set.
            fetched = list(qs[: self.MAX_EVIDENCE + 1])
        except SearchUnavailable:
            search_unavailable = True
            fetched = []
        truncated = len(fetched) > self.MAX_EVIDENCE
        evidences = fetched[: self.MAX_EVIDENCE]

        # Dot positions are pinned to the *unfiltered* embedding extents so a
        # dot keeps the same screen position as filters narrow the visible set.
        bounds_agg = Evidence.objects.filter(
            topic_fit_at__isnull=False,
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

        # Main-topic bar: a hierarchical, single-select filter over the report's
        # `is_main_topic` chapters (condensed so each node hangs off its nearest
        # main-topic ancestor). Coverage is corpus-wide and cumulative, so the
        # order/counts don't reshuffle as the user drills in.
        selected_chapter_id, main_topics = self.main_topic_tree

        # Render every <circle> as a single string in Python instead of
        # looping in the template. With ~1000 points the template loop dominates
        # the render; building the markup directly here (with html.escape on each
        # value) cuts it by an order of magnitude.
        # Originator-with-Verband and chapter display strings, computed once over
        # the whole filtered set (two grouped queries each — see the helpers) and
        # shared by both the dot tooltips and the outline/table below. The dot
        # tooltip mirrors the table's metadata columns, so it needs the same maps.
        originators_by_ev = self._originators_with_verband(evidences)
        chapters_by_ev = self._chapters_by_evidence(evidences)
        esc = html.escape
        circle_parts = []
        for pt in self._project(evidences, bounds=bounds):
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
                f' data-posted-on="{posted_on}"'
                f' data-originators="{esc(originators)}"'
                f' data-chapters="{esc(chapters)}"'
                f' data-stats="{esc(stats)}"'
                f' cx="{pt["cx"]}" cy="{pt["cy"]}"'
                f' r="4"></circle>'
            )
        cloud_circles_svg = mark_safe("".join(circle_parts))

        # Actors present in the filtered set, tallied over the visible evidence
        # via each evidence's originators (prefetched, so no extra per-row
        # query). Drives the "Actors in view" side panel; clicking a name
        # highlights that actor's dots client-side rather than filtering.
        actors_in_view = self._actors_in_view(evidences)

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

        return {
            "outline_items": outline_items,
            "outline_hidden_count": outline_hidden_count,
            "cloud_circles_svg": cloud_circles_svg,
            "svg_width": self.SVG_WIDTH,
            "svg_height": self.SVG_HEIGHT,
            "evidence_count": len(evidences),
            "truncated": truncated,
            "search_unavailable": search_unavailable,
            "max_evidence": self.MAX_EVIDENCE,
            "main_topics": main_topics,
            "selected_chapter_id": selected_chapter_id,
            "actors_in_view": actors_in_view,
            "has_filters": self._has_filters(),
        }

    # Params that count as an active filter — drives the Reset button and the
    # "no evidence matches these filters" wording. Read by both templates.
    FILTER_PARAMS = (
        "q",
        "platform",
        "posted_after",
        "posted_before",
        "actor",
        "role",
        "level",
        "verband",
    )

    def _has_filters(self):
        if self.main_topic_tree[0] is not None:
            return True
        params = self.request.GET
        return any((params.get(p) or "").strip() for p in self.FILTER_PARAMS)

    def _toolbar_context(self):
        """Filter-option universe for the toolbar, plus the form's URLs.

        Every option list here is drawn from the whole topic-fitted corpus, not
        from the filtered set: the dropdowns keep their full range as the user
        drills in. Only the full page renders them.
        """
        # Look up the currently-selected actor so the combobox button can
        # display its name on the initial server-rendered page.
        selected_actor = None
        actor_id = self._param_int(self.request.GET, "actor")
        if actor_id is not None:
            selected_actor = Actor.objects.filter(pk=actor_id).first()

        actors = self._actor_options()

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
        verbaende = sorted(
            (
                {"id": r.id, "label": "Bund" if r.kind == "country" else r.name}
                for r in GeoRegion.objects.filter(id__in=verband_ids).defer(
                    *GEOREGION_GEOMETRY_FIELDS
                )
            ),
            key=lambda v: (v["label"] != "Bund", v["label"]),
        )

        # Year-range slider bounds: earliest/latest post year across the whole
        # topic-bearing corpus, so the slider extent stays fixed regardless of
        # the active filters (like the embedding bounds above). The current
        # selection is parsed back out of the date params the filter applies, so
        # an empty selection lands the handles at the full extent.
        year_agg = Evidence.objects.filter(
            topic_fit_at__isnull=False,
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

        return {
            # Only actors that have actually posted — keeps the
            # searchable dropdown bounded to options that can yield
            # a non-empty result.
            "actors": actors,
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
            "reset_url": apphook_page_url(self.request),
            "topic_cloud_url": reverse("evidencecollection:evidence-topic-cloud"),
        }
