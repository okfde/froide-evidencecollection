from types import SimpleNamespace
from unittest import mock

from django.test import RequestFactory
from django.utils import timezone

import pytest

from froide_evidencecollection.documents import EvidenceDocument
from froide_evidencecollection.models import (
    Evidence,
    SocialMediaAccount,
    SocialMediaPost,
)
from froide_evidencecollection.views import EvidenceTopicCloudView, SearchUnavailable


class FakeSearch:
    """Stand-in for the elasticsearch-dsl Search chain `_search_ids` builds.

    Records the query body it is handed and returns hits whose `meta.id` are the
    given evidence pks (ES ids are strings, as they are in a real response).
    """

    def __init__(self, hit_ids=(), error=None):
        self.hit_ids = hit_ids
        self.error = error
        self.query_body = None
        self.source_value = None
        self.slice = None

    def query(self, q):
        self.query_body = q.to_dict()
        return self

    def source(self, value):
        self.source_value = value
        return self

    def __getitem__(self, key):
        self.slice = key
        return self

    def execute(self):
        if self.error is not None:
            raise self.error
        return [SimpleNamespace(meta=SimpleNamespace(id=str(i))) for i in self.hit_ids]


def _fitted_evidence(ext_id, platform=SocialMediaAccount.Platform.TELEGRAM):
    """A topic-fitted, social-media-backed evidence — the corpus the cloud plots."""
    account = SocialMediaAccount.objects.create(
        platform=platform,
        username=f"u{ext_id}",
        platform_user_id=str(ext_id),
    )
    post = SocialMediaPost.objects.create(
        account=account,
        platform_post_id=str(ext_id),
        url=f"https://t.me/example/{ext_id}",
        text="post body",
        posted_at=timezone.now(),
    )
    return Evidence.objects.create(
        social_media_post=post,
        topic_fit_at=timezone.now(),
    )


def _view(params):
    view = EvidenceTopicCloudView()
    view.setup(RequestFactory().get("/", params))
    return view


@pytest.mark.django_db
class TestFreeTextSearch:
    def setup_method(self):
        self.ev1 = _fitted_evidence(1)
        self.ev2 = _fitted_evidence(2)
        self.ev3 = _fitted_evidence(3, platform=SocialMediaAccount.Platform.YOUTUBE)

    def test_only_evidence_the_index_returns_matches(self):
        fake = FakeSearch(hit_ids=[self.ev1.pk, self.ev3.pk])
        with mock.patch.object(EvidenceDocument, "search", return_value=fake):
            ids = set(
                _view({"q": "test query"})._filter_qs().values_list("pk", flat=True)
            )
        assert ids == {self.ev1.pk, self.ev3.pk}

    def test_search_ands_with_the_other_filters(self):
        # The index matches ev1 (Telegram) and ev3 (YouTube); the platform filter
        # keeps only ev1. The two narrowings compose rather than one winning.
        fake = FakeSearch(hit_ids=[self.ev1.pk, self.ev3.pk])
        params = {"q": "test query", "platform": SocialMediaAccount.Platform.TELEGRAM}
        with mock.patch.object(EvidenceDocument, "search", return_value=fake):
            ids = set(_view(params)._filter_qs().values_list("pk", flat=True))
        assert ids == {self.ev1.pk}

    def test_no_hits_matches_nothing(self):
        fake = FakeSearch(hit_ids=[])
        with mock.patch.object(EvidenceDocument, "search", return_value=fake):
            ids = set(_view({"q": "nothing"})._filter_qs().values_list("pk", flat=True))
        assert ids == set()

    def test_blank_query_does_not_touch_the_index(self):
        with mock.patch.object(EvidenceDocument, "search") as search:
            ids = set(_view({"q": "   "})._filter_qs().values_list("pk", flat=True))
        search.assert_not_called()
        assert ids == {self.ev1.pk, self.ev2.pk, self.ev3.pk}

    def test_terms_are_anded_over_the_redacted_content_field(self):
        # `content` is the redacted `search_text`; AND matches the narrowing
        # behaviour of every other control in the toolbar.
        fake = FakeSearch(hit_ids=[])
        with mock.patch.object(EvidenceDocument, "search", return_value=fake):
            _view({"q": "test query"})._filter_qs().count()
        sqs = fake.query_body["simple_query_string"]
        assert sqs["query"] == "test query"
        assert sqs["fields"] == ["content"]
        assert sqs["default_operator"] == "and"
        # Only ids are pulled back; the rows come from the ORM.
        assert fake.source_value is False

    def test_result_is_capped_above_max_evidence(self):
        fake = FakeSearch(hit_ids=[])
        with mock.patch.object(EvidenceDocument, "search", return_value=fake):
            _view({"q": "x"})._filter_qs().count()
        # One past the cap, so the caller's "capped at…" detection still trips.
        assert fake.slice == slice(None, EvidenceTopicCloudView.MAX_EVIDENCE + 1, None)


@pytest.mark.django_db
# `get_context_data` builds each row's evidence link, whose URL the CMS apphook
# mounts in production; these tests build no CMS page, so mount it directly.
@pytest.mark.urls("froide_evidencecollection.tests.urls")
class TestSearchUnavailable:
    def setup_method(self):
        self.evidence = _fitted_evidence(1)

    def test_index_error_raises(self):
        fake = FakeSearch(error=RuntimeError("connection refused"))
        with mock.patch.object(EvidenceDocument, "search", return_value=fake):
            with pytest.raises(SearchUnavailable):
                _view({"q": "test query"})._filter_qs().count()

    def test_view_degrades_to_an_empty_result_and_says_so(self):
        fake = FakeSearch(error=RuntimeError("connection refused"))
        with mock.patch.object(EvidenceDocument, "search", return_value=fake):
            context = _view({"q": "test query"}).get_context_data()
        assert context["search_unavailable"] is True
        # No dots, no rows — and *not* presented as "nothing matched".
        assert context["evidence_count"] == 0
        assert context["outline_items"] == []

    def test_unfiltered_page_is_unaffected_by_a_broken_index(self):
        # No `q`, so the index is never consulted: the cloud still renders.
        with mock.patch.object(EvidenceDocument, "search") as search:
            context = _view({}).get_context_data()
        search.assert_not_called()
        assert context["search_unavailable"] is False
        assert context["evidence_count"] == 1
