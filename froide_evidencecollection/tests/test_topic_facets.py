"""Tests for the keyword-facet behaviour of the topic cloud view:
curator controls (enabled / custom_label) and the frequency-vs-keyness ranking.
"""

from django.http import QueryDict
from django.utils import timezone

import pytest

from froide_evidencecollection.models import (
    Category,
    Chapter,
    Evidence,
    EvidenceMention,
    Keyword,
    KeywordGroup,
    SocialMediaAccount,
    SocialMediaPost,
)
from froide_evidencecollection.views import EvidenceTopicCloudView


def _make_evidence(ext_id, *, fitted=False):
    """A minimal social-media-backed piece of evidence (satisfies the
    has-source check constraint). ``fitted`` sets ``topic_fit_at`` so the
    evidence counts toward the main-topic tree's coverage (which gates on it)."""
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
        raw={},
    )
    return Evidence.objects.create(
        social_media_post=post,
        external_id=ext_id,
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
        evidence=evidence, category=category, chapter=chapter
    )


def _kw(lemma, label, df, *, enabled=True, custom_label="", group=None):
    return Keyword.objects.create(
        lemma=lemma,
        label=label,
        custom_label=custom_label,
        enabled=enabled,
        df=df,
        group=group,
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


@pytest.mark.django_db
class TestSelectedEnabledKeywords:
    def test_drops_disabled_and_unknown_lemmas(self):
        _kw("a", "A", df=1)
        _kw("b", "B", df=1, enabled=False)
        params = QueryDict(mutable=True)
        params.setlist("keyword", ["a", "b", "ghost"])

        result = EvidenceTopicCloudView._selected_enabled_keywords(params)

        assert result == ["a"]

    def test_empty_when_nothing_selected(self):
        assert EvidenceTopicCloudView._selected_enabled_keywords(QueryDict()) == []


class TestSelectedGroupIds:
    def test_single_select_takes_first_valid_and_ignores_the_rest(self):
        # Single-select: the first numeric value wins; any extra group params
        # (and leading non-numeric junk) are ignored.
        params = QueryDict(mutable=True)
        params.setlist("group", ["x", "", "7", "3"])

        assert EvidenceTopicCloudView._selected_group_ids(params) == [7]

    def test_empty_when_nothing_selected(self):
        assert EvidenceTopicCloudView._selected_group_ids(QueryDict()) == []


@pytest.mark.django_db
class TestBuildGroups:
    def setup_method(self):
        self.view = EvidenceTopicCloudView()

    def _params(self, groups=(), **kw):
        q = QueryDict(mutable=True)
        for k, v in kw.items():
            q[k] = str(v)
        if groups:
            q.setlist("group", [str(g) for g in groups])
        return q

    def test_coverage_counts_distinct_evidence_and_excludes_disabled(self):
        group = KeywordGroup.objects.create(label="Vaccination")
        impfung = _kw("impfung", "Impfung", df=2, group=group)
        vakzin = _kw("vakzin", "Vakzin", df=1, group=group)
        hidden = _kw("booster", "Booster", df=1, enabled=False, group=group)
        e1, e2 = _make_evidence(1), _make_evidence(2)
        e1.keywords.add(impfung, vakzin, hidden)
        e2.keywords.add(impfung, hidden)

        selected_ids, options, lemmas = self.view._build_groups(self._params())

        assert selected_ids == []
        # 2 distinct evidence carry an *enabled* member (the disabled "booster"
        # contributes nothing, even though it would otherwise add coverage).
        assert options == [
            {"id": group.id, "label": "Vaccination", "count": 2, "selected": False}
        ]
        assert lemmas == set()

    def test_selection_marks_chip_and_returns_enabled_member_lemmas(self):
        group = KeywordGroup.objects.create(label="Vaccination")
        impfung = _kw("impfung", "Impfung", df=1, group=group)
        _kw("booster", "Booster", df=1, enabled=False, group=group)
        ev = _make_evidence(1)
        ev.keywords.add(impfung)

        selected_ids, options, lemmas = self.view._build_groups(
            self._params(groups=[group.id])
        )

        assert selected_ids == [group.id]
        assert options[0]["selected"] is True
        assert lemmas == {"impfung"}  # disabled "booster" excluded

    def test_single_select_honours_only_the_first_of_several_topics(self):
        # Single-select: passing two topics keeps only the first; just that
        # chip marks active and only its member lemmas are highlighted.
        health = KeywordGroup.objects.create(label="Health")
        school = KeywordGroup.objects.create(label="School")
        impfung = _kw("impfung", "Impfung", df=1, group=health)
        schule = _kw("schule", "Schule", df=1, group=school)
        ev = _make_evidence(1)
        ev.keywords.add(impfung, schule)

        selected_ids, options, lemmas = self.view._build_groups(
            self._params(groups=[health.id, school.id])
        )

        assert selected_ids == [health.id]
        selected = {o["id"]: o["selected"] for o in options}
        assert selected[health.id] is True
        assert selected[school.id] is False
        assert lemmas == {"impfung"}

    def test_stale_selection_is_dropped(self):
        # A group id that has no enabled, evidence-bearing keywords isn't a real
        # option, so the selection is cleared rather than left dangling.
        group = KeywordGroup.objects.create(label="Empty")
        selected_ids, options, lemmas = self.view._build_groups(
            self._params(groups=[group.id])
        )
        assert selected_ids == []
        assert options == []

    def test_build_facets_marks_group_members(self):
        ev = _make_evidence(1)
        member = _kw("impfung", "Impfung", df=1)
        other = _kw("schule", "Schule", df=1)
        ev.keywords.add(member, other)

        facets = self.view._build_facets(
            [ev], [], keyness=False, group_lemmas={"impfung"}
        )

        by_lemma = {f["lemma"]: f["in_group"] for f in facets}
        assert by_lemma == {"impfung": True, "schule": False}


@pytest.mark.django_db
class TestDominantGroup:
    def test_none_when_no_grouped_keyword(self):
        ev = _make_evidence(1)
        ev.keywords.add(_kw("frei", "Frei", df=1))  # ungrouped

        assert EvidenceTopicCloudView._dominant_group(ev, n_docs=100) is None

    def test_ungrouped_keywords_are_ignored(self):
        group = KeywordGroup.objects.create(label="Health")
        ev = _make_evidence(1)
        ev.keywords.add(
            _kw("impfung", "Impfung", df=10, group=group),
            _kw("frei", "Frei", df=1),  # ungrouped, lower df — still ignored
        )

        assert EvidenceTopicCloudView._dominant_group(ev, n_docs=100) == group.id

    def test_specificity_weight_beats_raw_count(self):
        # Health contributes one *rare* keyword; Freedom two *common* ones.
        # Raw count would pick Freedom (2 vs 1); the IDF-weighted vote picks
        # Health, because the rare term is the sharper signal of belonging.
        health = KeywordGroup.objects.create(label="Health")
        freedom = KeywordGroup.objects.create(label="Freedom")
        ev = _make_evidence(1)
        ev.keywords.add(
            _kw("impfpflicht", "Impfpflicht", df=40, group=health),
            _kw("freiheit", "Freiheit", df=600, group=freedom),
            _kw("grundrechte", "Grundrechte", df=500, group=freedom),
        )

        assert EvidenceTopicCloudView._dominant_group(ev, n_docs=2000) == health.id

    def test_tie_breaks_on_lowest_group_id(self):
        # Equal score and equal strongest-keyword weight → deterministic
        # fallback to the lowest group id.
        first = KeywordGroup.objects.create(label="First")
        second = KeywordGroup.objects.create(label="Second")
        ev = _make_evidence(1)
        ev.keywords.add(
            _kw("a", "A", df=5, group=first),
            _kw("b", "B", df=5, group=second),
        )

        assert EvidenceTopicCloudView._dominant_group(ev, n_docs=100) == first.id


class TestAssignGroupColors:
    def setup_method(self):
        self.view = EvidenceTopicCloudView()

    def test_groups_get_palette_colours_in_coverage_order(self):
        options = [
            {"id": 7, "label": "Big"},
            {"id": 3, "label": "Small"},
        ]

        color_by_group = self.view._assign_group_colors(options)

        palette = EvidenceTopicCloudView.GROUP_PALETTE
        assert color_by_group == {7: palette[0], 3: palette[1]}
        # The colour is also attached to each option for the chip swatch.
        assert options[0]["color"] == palette[0]
        assert options[1]["color"] == palette[1]

    def test_groups_beyond_palette_fall_back_to_neutral_ink(self):
        palette_len = len(EvidenceTopicCloudView.GROUP_PALETTE)
        options = [{"id": i, "label": f"G{i}"} for i in range(palette_len + 2)]

        color_by_group = self.view._assign_group_colors(options)

        # The overflow topics take the neutral ink, both on the option (for the
        # swatch) and in the map (so the dot fill is consistent with the chip).
        overflow = options[palette_len]
        assert overflow["color"] == EvidenceTopicCloudView.DOT_COLOR
        assert color_by_group[overflow["id"]] == EvidenceTopicCloudView.DOT_COLOR


def _ids(nodes):
    return [n["id"] for n in nodes]


@pytest.mark.django_db
class TestSelectedChapterId:
    def test_single_select_takes_first_valid_and_ignores_the_rest(self):
        # Single-select: the first numeric value wins; extra/junk values ignored.
        params = QueryDict(mutable=True)
        params.setlist("chapter", ["x", "", "7", "3"])

        assert EvidenceTopicCloudView._selected_chapter_id(params) == 7

    def test_none_when_nothing_selected(self):
        assert EvidenceTopicCloudView._selected_chapter_id(QueryDict()) is None


@pytest.mark.django_db
class TestBuildMainTopicTree:
    def setup_method(self):
        self.view = EvidenceTopicCloudView()
        self.cat = Category.objects.create(name="C")

    def _params(self, chapter=None):
        q = QueryDict(mutable=True)
        if chapter is not None:
            q["chapter"] = str(chapter)
        return q

    def test_only_main_topics_appear_and_counts_are_subtree_cumulative(self):
        # Tree: Health (main) → Vaccines (not main) → Boosters (main).
        # e1 filed at Health, e2 under Boosters. Health subsumes both (2);
        # Boosters subsumes only e2 (1). The non-main "Vaccines" never appears.
        health = _make_chapter("Health", is_main_topic=True)
        vaccines = _make_chapter("Vaccines", parent=health)
        boosters = _make_chapter("Boosters", is_main_topic=True, parent=vaccines)
        e1, e2 = _make_evidence(1, fitted=True), _make_evidence(2, fitted=True)
        _file_under(e1, health, self.cat)
        _file_under(e2, boosters, self.cat)

        selected, nodes = self.view._build_main_topic_tree(self._params())

        assert selected is None
        by_id = {n["id"]: n for n in nodes}
        assert set(by_id) == {health.id, boosters.id}
        assert by_id[health.id]["count"] == 2
        assert by_id[boosters.id]["count"] == 1
        # Boosters is re-parented under Health (the non-main "Vaccines" collapsed),
        # so it is indented one level deeper and links back to Health.
        assert by_id[health.id]["depth"] == 0
        assert by_id[health.id]["parent_id"] is None
        assert by_id[boosters.id]["depth"] == 1
        assert by_id[boosters.id]["parent_id"] == health.id

    def test_distinct_evidence_not_double_counted(self):
        root = _make_chapter("Topic", is_main_topic=True)
        child = _make_chapter("Sub", parent=root)
        e1 = _make_evidence(1, fitted=True)
        # The same evidence filed twice (at the node and under a descendant)
        # must count once.
        _file_under(e1, root, self.cat)
        _file_under(e1, child, self.cat)

        _, nodes = self.view._build_main_topic_tree(self._params())

        assert [n["count"] for n in nodes] == [1]

    def test_unfitted_evidence_is_excluded(self):
        root = _make_chapter("Topic", is_main_topic=True)
        _file_under(_make_evidence(1, fitted=False), root, self.cat)

        _, nodes = self.view._build_main_topic_tree(self._params())

        # No fitted evidence subsumed → the node isn't offered at all.
        assert nodes == []

    def test_merges_separate_branches_to_their_own_roots(self):
        # Two main topics with no main-topic ancestor are both condensed-tree
        # roots; a non-main root in between is collapsed away.
        bucket = _make_chapter("Bucket")  # not a main topic
        a = _make_chapter("A", is_main_topic=True, parent=bucket)
        b = _make_chapter("B", is_main_topic=True, parent=bucket)
        ea, eb = _make_evidence(1, fitted=True), _make_evidence(2, fitted=True)
        _file_under(ea, a, self.cat)
        _file_under(eb, b, self.cat)

        _, nodes = self.view._build_main_topic_tree(self._params())

        # Both surface at depth 0 (their shared non-main parent is collapsed).
        assert {n["id"] for n in nodes} == {a.id, b.id}
        assert all(n["depth"] == 0 for n in nodes)

    def test_siblings_ordered_by_coverage_then_label(self):
        big = _make_chapter("Big", is_main_topic=True)
        small = _make_chapter("Small", is_main_topic=True)
        for i in range(2):
            _file_under(_make_evidence(i, fitted=True), big, self.cat)
        _file_under(_make_evidence(9, fitted=True), small, self.cat)

        _, nodes = self.view._build_main_topic_tree(self._params())

        assert _ids(nodes) == [big.id, small.id]

    def test_selection_marks_node(self):
        root = _make_chapter("Topic", is_main_topic=True)
        _file_under(_make_evidence(1, fitted=True), root, self.cat)

        selected, nodes = self.view._build_main_topic_tree(
            self._params(chapter=root.id)
        )

        assert selected == root.id
        assert nodes[0]["selected"] is True

    def test_stale_selection_is_dropped(self):
        # A main-topic chapter that subsumes no evidence isn't a real option,
        # so the selection is cleared rather than left dangling.
        empty = _make_chapter("Empty", is_main_topic=True)

        selected, nodes = self.view._build_main_topic_tree(
            self._params(chapter=empty.id)
        )

        assert selected is None
        assert nodes == []

    def test_collapsed_by_default(self):
        # With no selection the tree is closed: the root is visible but not
        # expanded, and its child is hidden until the user opens it.
        root = _make_chapter("Parent", is_main_topic=True)
        child = _make_chapter("Child", is_main_topic=True, parent=root)
        _file_under(_make_evidence(1, fitted=True), child, self.cat)

        _, nodes = self.view._build_main_topic_tree(self._params())

        by_id = {n["id"]: n for n in nodes}
        assert by_id[root.id]["visible"] is True
        assert by_id[root.id]["has_children"] is True
        assert by_id[root.id]["expanded"] is False
        assert by_id[child.id]["visible"] is False

    def test_selected_path_is_expanded(self):
        # Selecting a nested node opens the path to it: its ancestors are
        # expanded and it becomes visible.
        root = _make_chapter("Parent", is_main_topic=True)
        child = _make_chapter("Child", is_main_topic=True, parent=root)
        _file_under(_make_evidence(1, fitted=True), child, self.cat)

        _, nodes = self.view._build_main_topic_tree(self._params(chapter=child.id))

        by_id = {n["id"]: n for n in nodes}
        assert by_id[root.id]["expanded"] is True
        assert by_id[child.id]["visible"] is True
        assert by_id[child.id]["selected"] is True
