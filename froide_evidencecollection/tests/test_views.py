import csv
import datetime
import io

import pytest

from froide_evidencecollection.models import (
    Actor,
    Evidence,
    EvidenceMention,
    RedactionRule,
    SocialMediaAccount,
    SocialMediaPost,
)
from froide_evidencecollection.views import EvidenceDetailView, EvidenceExporter

from .factories import OrganizationFactory


def _make_evidence(suffix, **overrides):
    account = SocialMediaAccount.objects.create(
        platform=SocialMediaAccount.Platform.TELEGRAM,
        username=f"kanal_{suffix}",
        platform_user_id=suffix,
    )
    fields = {
        "account": account,
        "platform_post_id": suffix,
        "url": f"https://t.me/kanal_{suffix}/1",
        "text": "post body",
    }
    fields.update(overrides)
    post = SocialMediaPost.objects.create(**fields)
    return Evidence.objects.create(
        social_media_post=post, documentation_date=datetime.date(2026, 1, 15)
    )


def _rows(queryset):
    payload, content_type = EvidenceExporter("csv").export(queryset)
    assert content_type == "text/csv"
    return list(csv.reader(io.StringIO(payload.decode())))


@pytest.mark.django_db
class TestEvidenceExporter:
    def test_one_row_per_text_segment(self):
        inner = _make_evidence("inner", text="quoted text").social_media_post
        evidence = _make_evidence(
            "outer", title="the title", description="a description"
        )
        evidence.social_media_post.redistributes = inner
        evidence.social_media_post.save(update_fields=["redistributes"])

        rows = _rows(Evidence.objects.filter(pk=evidence.pk))
        pk, slug = str(evidence.pk), evidence.slug
        url = "https://t.me/kanal_outer/1"

        # Every segment repeats the evidence's identifying columns. The repost
        # row is indistinguishable from the post's own body row: same "Post text"
        # label, and the segment's attribution has no column of its own.
        assert rows == [
            [
                "id",
                "slug",
                "documentation_date",
                "social_media_post__url",
                "text_segment_label",
                "text_segment_text",
            ],
            [pk, slug, "2026-01-15", url, "Post title", "the title"],
            [pk, slug, "2026-01-15", url, "Post text", "post body"],
            [pk, slug, "2026-01-15", url, "Description", "a description"],
            [pk, slug, "2026-01-15", url, "Post text", "quoted text"],
        ]

    def test_evidence_without_text_contributes_no_rows(self):
        _make_evidence("empty", text="")
        assert len(_rows(Evidence.objects.all())) == 1  # header only

    def test_rows_follow_the_descending_pk_order_of_the_queryset(self):
        first = _make_evidence("a", text="erst")
        second = _make_evidence("b", text="dann")

        assert first.pk < second.pk
        assert [row[-1] for row in _rows(Evidence.objects.all())[1:]] == [
            "dann",
            "erst",
        ]

    def test_unsupported_format_is_rejected(self):
        with pytest.raises(ValueError, match="format json is not supported"):
            EvidenceExporter("json")


@pytest.mark.django_db
class TestEvidenceDetailQueryset:
    def test_citations_are_masked_without_a_query_per_mention(
        self, django_assert_num_queries
    ):
        # The detail page redacts every mention's citation against the post's
        # scoped rules, so the view prefetches them.
        evidence = _make_evidence("v", video_source_path="./video/a.mp4")
        rule = RedactionRule.objects.create(pattern="Badword", placeholder="[X]")
        rule.posts.add(evidence.social_media_post)
        for i in range(3):
            EvidenceMention.objects.create(
                evidence=evidence,
                footnote=f"fn{i}",
                citation=f"the Badword, take {i}",
                originator=Actor.objects.create(organization=OrganizationFactory()),
            )

        # Warm the module-level global redactor, so what's left to count is the
        # queryset and its prefetches.
        assert evidence.mentions.first().redacted_citation

        obj = EvidenceDetailView().get_queryset().get(pk=evidence.pk)
        with django_assert_num_queries(0):
            citations = [m.redacted_citation for m in obj.mentions.all()]
        assert citations == [f"the [X], take {i}" for i in range(3)]
