"""Tests for the topic cloud view's browse surfaces: the main-topic tree."""

from django.http import QueryDict
from django.utils import timezone

import pytest

from froide_evidencecollection.models import (
    Actor,
    Category,
    Chapter,
    Evidence,
    EvidenceMention,
    SocialMediaAccount,
    SocialMediaPost,
)
from froide_evidencecollection.views import EvidenceTopicCloudView

from .factories import OrganizationFactory


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
