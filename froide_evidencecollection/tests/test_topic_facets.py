"""Tests for the topic cloud view's browse surfaces: the keyword facets
(curator controls + frequency-vs-keyness ranking) and the `Theme` bar
(membership union, dominant-theme dot colouring, palette).
"""

from django.http import QueryDict
from django.utils import timezone

import pytest

from froide_evidencecollection.models import (
    Actor,
    Category,
    Chapter,
    Evidence,
    EvidenceMention,
    Keyword,
    SocialMediaAccount,
    SocialMediaPost,
    Theme,
)
from froide_evidencecollection.views import EvidenceTopicCloudView

from .factories import OrganizationFactory


def _make_evidence(ext_id, *, fitted=False):
    """A minimal social-media-backed piece of evidence (satisfies the
    has-source check constraint). ``fitted`` sets ``topic_fit_at`` so the
    evidence counts toward the theme bar's coverage (which gates on it)."""
    account = SocialMediaAccount.objects.create(
        platform=SocialMediaAccount.Platform.TELEGRAM,
        username=f"u{ext_id}",
        platform_user_id=str(ext_id),
    )
    post = SocialMediaPost.objects.create(
        account=account,
        platform_post_id=str(ext_id),
        url=f"https://t.me/example/{ext_id}",
        text="post body",
    )
    return Evidence.objects.create(
        social_media_post=post,
        topic_fit_at=timezone.now() if fitted else None,
    )


def _make_chapter(label, *, is_main_topic=False, parent=None):
    """Create a chapter node, optionally as a child of ``parent``."""
    if parent is None:
        return Chapter.add_root(custom_label=label, is_main_topic=is_main_topic)
    # Refresh so treebeard's child counters are current before appending.
    parent.refresh_from_db()
    return parent.add_child(custom_label=label, is_main_topic=is_main_topic)


def _file_under(evidence, chapter, category):
    """File ``evidence`` under ``chapter`` via an EvidenceMention."""
    return EvidenceMention.objects.create(
        evidence=evidence,
        category=category,
        chapter=chapter,
        originator=Actor.objects.create(organization=OrganizationFactory()),
    )


def _kw(lemma, label, df, *, enabled=True, custom_label=""):
    return Keyword.objects.create(
        lemma=lemma,
        label=label,
        custom_label=custom_label,
        enabled=enabled,
        df=df,
        fit_at=timezone.now(),
    )


def _lemmas(facets):
    return [f["lemma"] for f in facets]


@pytest.mark.django_db
class TestBuildFacets:
    def setup_method(self):
        self.view = EvidenceTopicCloudView()

    def test_disabled_keyword_is_excluded(self):
        ev = _make_evidence(1)
        shown = _kw("impfung", "Impfung", df=1)
        hidden = _kw("vakzin", "Vakzin", df=1, enabled=False)
        ev.keywords.add(shown, hidden)

        facets = self.view._build_facets([ev], [], keyness=False)

        assert _lemmas(facets) == ["impfung"]

    def test_custom_label_overrides_display(self):
        ev = _make_evidence(1)
        kw = _kw("impfung", "Impfung", df=1, custom_label="Impfpflicht")
        ev.keywords.add(kw)

        [facet] = self.view._build_facets([ev], [], keyness=False)

        assert facet["keyword"] == "Impfpflicht"

    def test_frequency_mode_orders_by_count(self):
        common = _kw("common", "Common", df=2)
        rare = _kw("rare", "Rare", df=1)
        evs = []
        for i in range(2):
            ev = _make_evidence(i)
            ev.keywords.add(common)
            evs.append(ev)
        evs[0].keywords.add(rare)

        facets = self.view._build_facets(evs, [], keyness=False)

        # common appears in both evidence, rare in one → common first.
        assert _lemmas(facets) == ["common", "rare"]

    def test_keyness_mode_promotes_distinctive_over_frequent(self):
        # "common" is corpus-wide (df=10); "rare" is globally uncommon (df=2)
        # but both occur in every evidence of this slice. Keyness should rank
        # the distinctive "rare" above the ubiquitous "common", even though
        # their in-slice counts are equal.
        common = _kw("common", "Common", df=10)
        rare = _kw("rare", "Rare", df=2)
        evs = []
        for i in range(2):
            ev = _make_evidence(i)
            ev.keywords.add(common, rare)
            evs.append(ev)

        facets = self.view._build_facets(evs, [], keyness=True)

        assert _lemmas(facets)[0] == "rare"

    def test_selected_keyword_dropped_from_facets(self):
        ev = _make_evidence(1)
        a = _kw("a", "A", df=1)
        b = _kw("b", "B", df=1)
        ev.keywords.add(a, b)

        facets = self.view._build_facets([ev], ["a"], keyness=False)

        assert _lemmas(facets) == ["b"]


def _map_chapter_to_theme(chapter, theme):
    """Set the bulk Chapter→Theme mapping ('everything in chapter X belongs to
    theme Y')."""
    chapter.theme = theme
    chapter.save()


class TestSelectedThemeId:
    def test_takes_first_valid_and_ignores_the_rest(self):
        # Single-select: the first numeric value wins; extra/junk values ignored.
        params = QueryDict(mutable=True)
        params.setlist("theme", ["x", "", "7", "3"])

        assert EvidenceTopicCloudView._selected_theme_id(params) == 7

    def test_none_when_nothing_selected(self):
        assert EvidenceTopicCloudView._selected_theme_id(QueryDict()) is None


@pytest.mark.django_db
class TestChapterThemeMap:
    """`Chapter.chapter_theme_map` resolves the mapping with inheritance."""

    def test_descendant_inherits_nearest_themed_ancestor(self):
        t = Theme.objects.create(label="T")
        parent = _make_chapter("Parent")
        _map_chapter_to_theme(parent, t)
        child = _make_chapter("Child", parent=parent)
        grandchild = _make_chapter("Grandchild", parent=child)

        mapping = Chapter.chapter_theme_map()

        # Every node in the subtree resolves to the ancestor's theme.
        assert mapping[parent.id] == t.id
        assert mapping[child.id] == t.id
        assert mapping[grandchild.id] == t.id

    def test_nearer_mapping_overrides_a_higher_one(self):
        outer = Theme.objects.create(label="Outer")
        inner = Theme.objects.create(label="Inner")
        parent = _make_chapter("Parent")
        _map_chapter_to_theme(parent, outer)
        child = _make_chapter("Child", parent=parent)
        _map_chapter_to_theme(child, inner)
        grandchild = _make_chapter("Grandchild", parent=child)

        mapping = Chapter.chapter_theme_map()

        assert mapping[parent.id] == outer.id
        # The nearer mapping on `child` wins for it and its descendants.
        assert mapping[child.id] == inner.id
        assert mapping[grandchild.id] == inner.id

    def test_unmapped_chapters_are_omitted(self):
        _make_chapter("Lonely")
        assert Chapter.chapter_theme_map() == {}


@pytest.mark.django_db
class TestThemeEvidenceQueryset:
    """`Theme.evidence_queryset` is the union of direct + chapter-mapped."""

    def setup_method(self):
        self.cat = Category.objects.create(name="C")

    def test_unions_direct_and_chapter_subtree_without_double_counting(self):
        theme = Theme.objects.create(label="T")
        parent = _make_chapter("Parent")
        _map_chapter_to_theme(parent, theme)
        child = _make_chapter("Child", parent=parent)

        e_direct = _make_evidence(1)
        theme.evidences.add(e_direct)
        e_chapter = _make_evidence(2)
        _file_under(e_chapter, child, self.cat)  # inherited mapping
        e_both = _make_evidence(3)
        theme.evidences.add(e_both)
        _file_under(e_both, parent, self.cat)
        _make_evidence(4)  # unrelated, in no theme

        members = set(theme.evidence_queryset().values_list("id", flat=True))

        assert members == {e_direct.id, e_chapter.id, e_both.id}


@pytest.mark.django_db
class TestBuildThemes:
    def setup_method(self):
        self.view = EvidenceTopicCloudView()
        self.cat = Category.objects.create(name="C")

    def _params(self, theme=None):
        q = QueryDict(mutable=True)
        if theme is not None:
            q["theme"] = str(theme)
        return q

    def test_count_unions_direct_and_chapter_and_gates_on_fitted(self):
        theme = Theme.objects.create(label="Health", order=0)
        chapter = _make_chapter("Health chapter")
        _map_chapter_to_theme(chapter, theme)
        e_direct = _make_evidence(1, fitted=True)
        theme.evidences.add(e_direct)
        e_chapter = _make_evidence(2, fitted=True)
        _file_under(e_chapter, chapter, self.cat)
        # An unfitted chapter evidence is not in the cloud's universe → uncounted.
        _file_under(_make_evidence(3, fitted=False), chapter, self.cat)

        selected, options = self.view._build_themes(self._params())

        assert selected is None
        assert options == [
            {"id": theme.id, "label": "Health", "count": 2, "selected": False}
        ]

    def test_evidence_in_both_paths_counted_once(self):
        theme = Theme.objects.create(label="T", order=0)
        chapter = _make_chapter("Ch")
        _map_chapter_to_theme(chapter, theme)
        e = _make_evidence(1, fitted=True)
        theme.evidences.add(e)
        _file_under(e, chapter, self.cat)

        _, options = self.view._build_themes(self._params())

        assert options[0]["count"] == 1

    def test_listed_in_curator_order(self):
        # `Theme.order` (not coverage) fixes the chip order.
        b = Theme.objects.create(label="B", order=1)
        a = Theme.objects.create(label="A", order=0)
        for i, t in enumerate((a, b)):
            t.evidences.add(_make_evidence(i, fitted=True))

        _, options = self.view._build_themes(self._params())

        assert [o["id"] for o in options] == [a.id, b.id]

    def test_empty_theme_is_dropped(self):
        Theme.objects.create(label="Empty", order=0)
        _, options = self.view._build_themes(self._params())
        assert options == []

    def test_stale_selection_is_dropped(self):
        theme = Theme.objects.create(label="Empty", order=0)
        selected, options = self.view._build_themes(self._params(theme=theme.id))
        assert selected is None
        assert options == []

    def test_selection_marks_chip(self):
        theme = Theme.objects.create(label="T", order=0)
        theme.evidences.add(_make_evidence(1, fitted=True))

        selected, options = self.view._build_themes(self._params(theme=theme.id))

        assert selected == theme.id
        assert options[0]["selected"] is True


@pytest.mark.django_db
class TestDominantTheme:
    def setup_method(self):
        self.cat = Category.objects.create(name="C")

    def _maps(self):
        return (
            Chapter.chapter_theme_map(),
            dict(Theme.objects.values_list("id", "order")),
        )

    def test_none_when_no_theme(self):
        ev = _make_evidence(1)
        assert EvidenceTopicCloudView._dominant_theme(ev, {}, {}) is None

    def test_direct_beats_chapter(self):
        # Direct wins even though the chapter-derived theme has the lower order.
        direct = Theme.objects.create(label="Direct", order=5)
        chap_theme = Theme.objects.create(label="Chapter", order=0)
        chapter = _make_chapter("Ch")
        _map_chapter_to_theme(chapter, chap_theme)
        ev = _make_evidence(1)
        ev.themes.add(direct)
        _file_under(ev, chapter, self.cat)

        tbc, order = self._maps()
        assert EvidenceTopicCloudView._dominant_theme(ev, tbc, order) == direct.id

    def test_direct_tie_breaks_on_lowest_order(self):
        low = Theme.objects.create(label="Low", order=0)
        high = Theme.objects.create(label="High", order=9)
        ev = _make_evidence(1)
        ev.themes.add(low, high)

        _, order = self._maps()
        assert EvidenceTopicCloudView._dominant_theme(ev, {}, order) == low.id

    def test_chapter_derived_when_no_direct(self):
        t = Theme.objects.create(label="T", order=0)
        chapter = _make_chapter("Ch")
        _map_chapter_to_theme(chapter, t)
        ev = _make_evidence(1)
        _file_under(ev, chapter, self.cat)

        tbc, order = self._maps()
        assert EvidenceTopicCloudView._dominant_theme(ev, tbc, order) == t.id

    def test_chapter_tie_breaks_on_lowest_order(self):
        a = Theme.objects.create(label="A", order=0)
        b = Theme.objects.create(label="B", order=9)
        ca = _make_chapter("CA")
        _map_chapter_to_theme(ca, a)
        cb = _make_chapter("CB")
        _map_chapter_to_theme(cb, b)
        ev = _make_evidence(1)
        _file_under(ev, ca, self.cat)
        _file_under(ev, cb, self.cat)

        tbc, order = self._maps()
        assert EvidenceTopicCloudView._dominant_theme(ev, tbc, order) == a.id


class TestAssignThemeColors:
    def setup_method(self):
        self.view = EvidenceTopicCloudView()

    def test_themes_get_palette_colours_in_order(self):
        options = [{"id": 7, "label": "First"}, {"id": 3, "label": "Second"}]

        color_by_theme = self.view._assign_theme_colors(options)

        palette = EvidenceTopicCloudView.GROUP_PALETTE
        assert color_by_theme == {7: palette[0], 3: palette[1]}
        # The colour is also attached to each option for the chip swatch.
        assert options[0]["color"] == palette[0]
        assert options[1]["color"] == palette[1]

    def test_themes_beyond_palette_fall_back_to_neutral_ink(self):
        palette_len = len(EvidenceTopicCloudView.GROUP_PALETTE)
        options = [{"id": i, "label": f"T{i}"} for i in range(palette_len + 2)]

        color_by_theme = self.view._assign_theme_colors(options)

        overflow = options[palette_len]
        assert overflow["color"] == EvidenceTopicCloudView.DOT_COLOR
        assert color_by_theme[overflow["id"]] == EvidenceTopicCloudView.DOT_COLOR


class TestDotFill:
    """The selection lens: when a theme is selected, every visible dot takes
    that theme's colour, overriding its own dominant-theme hue."""

    def test_lens_colour_overrides_the_dominant_theme(self):
        # Theme B selected (lens = its hue); the dot's own dominant theme is A.
        # The lens wins, so the dot reads as one of "your B".
        fill = EvidenceTopicCloudView._dot_fill(
            lens_color="#bbb", dominant_theme_id=1, theme_color={1: "#aaa"}
        )
        assert fill == "#bbb"

    def test_falls_back_to_dominant_theme_when_no_lens(self):
        fill = EvidenceTopicCloudView._dot_fill(
            lens_color=None, dominant_theme_id=1, theme_color={1: "#aaa"}
        )
        assert fill == "#aaa"

    def test_neutral_ink_when_no_lens_and_no_theme(self):
        fill = EvidenceTopicCloudView._dot_fill(
            lens_color=None, dominant_theme_id=None, theme_color={}
        )
        assert fill == EvidenceTopicCloudView.DOT_COLOR


class TestSelectedChapterId:
    def test_takes_first_valid_and_ignores_the_rest(self):
        # Single-select: the first numeric value wins; extra/junk values ignored.
        params = QueryDict(mutable=True)
        params.setlist("chapter", ["x", "", "7", "3"])

        assert EvidenceTopicCloudView._selected_chapter_id(params) == 7

    def test_none_when_nothing_selected(self):
        assert EvidenceTopicCloudView._selected_chapter_id(QueryDict()) is None


@pytest.mark.django_db
class TestBuildMainTopicTree:
    """`_build_main_topic_tree` condenses the `is_main_topic` chapters into an
    indented, cumulatively-counted, single-select drill-down tree."""

    def setup_method(self):
        self.view = EvidenceTopicCloudView()
        self.cat = Category.objects.create(name="C")

    def _params(self, chapter=None):
        q = QueryDict(mutable=True)
        if chapter is not None:
            q["chapter"] = str(chapter)
        return q

    def _by_id(self, nodes):
        return {n["id"]: n for n in nodes}

    def test_condenses_non_main_intermediates_and_counts_cumulatively(self):
        # A main-topic parent with a main-topic grandchild, bridged by a non-main
        # intermediate chapter. The intermediate is merged away, so the child
        # hangs directly off the parent; coverage is cumulative up the chain.
        parent = _make_chapter("Parent", is_main_topic=True)
        middle = _make_chapter("Middle", parent=parent)  # not a main topic
        child = _make_chapter("Child", is_main_topic=True, parent=middle)

        # One evidence filed under the deep child, one directly under the parent.
        _file_under(_make_evidence(1, fitted=True), child, self.cat)
        _file_under(_make_evidence(2, fitted=True), parent, self.cat)

        selected, nodes = self.view._build_main_topic_tree(self._params())
        by_id = self._by_id(nodes)

        assert selected is None
        # The non-main intermediate is not a node.
        assert middle.id not in by_id
        # Condensed parent link: child hangs off the nearest main-topic ancestor.
        assert by_id[child.id]["parent_id"] == parent.id
        assert by_id[child.id]["depth"] == 1
        # Cumulative coverage: the child's evidence is subsumed by the parent.
        assert by_id[child.id]["count"] == 1
        assert by_id[parent.id]["count"] == 2

    def test_only_fitted_evidence_counts(self):
        ch = _make_chapter("Topic", is_main_topic=True)
        _file_under(_make_evidence(1, fitted=True), ch, self.cat)
        _file_under(_make_evidence(2, fitted=False), ch, self.cat)

        _, nodes = self.view._build_main_topic_tree(self._params())

        assert self._by_id(nodes)[ch.id]["count"] == 1

    def test_empty_main_topics_are_dropped(self):
        _make_chapter("Empty", is_main_topic=True)
        _, nodes = self.view._build_main_topic_tree(self._params())
        assert nodes == []

    def test_non_main_chapters_are_never_nodes(self):
        ch = _make_chapter("Plain")  # not a main topic
        _file_under(_make_evidence(1, fitted=True), ch, self.cat)
        _, nodes = self.view._build_main_topic_tree(self._params())
        assert nodes == []

    def test_collapsed_by_default_only_roots_visible(self):
        parent = _make_chapter("Parent", is_main_topic=True)
        child = _make_chapter("Child", is_main_topic=True, parent=parent)
        _file_under(_make_evidence(1, fitted=True), child, self.cat)

        _, nodes = self.view._build_main_topic_tree(self._params())
        by_id = self._by_id(nodes)

        assert by_id[parent.id]["visible"] is True
        assert by_id[parent.id]["has_children"] is True
        assert by_id[parent.id]["expanded"] is False
        # Child hidden until the parent is expanded.
        assert by_id[child.id]["visible"] is False

    def test_selection_marks_node_and_expands_its_path(self):
        parent = _make_chapter("Parent", is_main_topic=True)
        child = _make_chapter("Child", is_main_topic=True, parent=parent)
        _file_under(_make_evidence(1, fitted=True), child, self.cat)

        selected, nodes = self.view._build_main_topic_tree(
            self._params(chapter=child.id)
        )
        by_id = self._by_id(nodes)

        assert selected == child.id
        assert by_id[child.id]["selected"] is True
        # The path to the selection is expanded, so the child is revealed.
        assert by_id[parent.id]["expanded"] is True
        assert by_id[child.id]["visible"] is True

    def test_stale_selection_is_dropped(self):
        _make_chapter("Empty", is_main_topic=True)
        selected, nodes = self.view._build_main_topic_tree(self._params(chapter=999))
        assert selected is None
        assert nodes == []

    def test_siblings_ordered_by_coverage_then_label(self):
        big = _make_chapter("Big", is_main_topic=True)
        small = _make_chapter("Small", is_main_topic=True)
        _file_under(_make_evidence(1, fitted=True), big, self.cat)
        _file_under(_make_evidence(2, fitted=True), big, self.cat)
        _file_under(_make_evidence(3, fitted=True), small, self.cat)

        _, nodes = self.view._build_main_topic_tree(self._params())

        # Biggest coverage leads.
        assert [n["id"] for n in nodes] == [big.id, small.id]
