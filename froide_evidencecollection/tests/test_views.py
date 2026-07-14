import csv
import datetime
import io

import pytest

from froide_evidencecollection.models import (
    Actor,
    Evidence,
    EvidenceMention,
    PoliticalPosition,
    RedactionRule,
    SocialMediaAccount,
    SocialMediaPost,
)
from froide_evidencecollection.views import EvidenceDetailView, EvidenceExporter

from .factories import GeoRegionFactory, OrganizationFactory, PersonFactory


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
        "posted_at": datetime.datetime(2026, 1, 2, 9, 30, tzinfo=datetime.UTC),
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


HEADER = [
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


def _cell(row, column):
    return row[HEADER.index(column)]


def _mention(evidence, footnote, **overrides):
    fields = {
        "evidence": evidence,
        "footnote": footnote,
        "originator": Actor.objects.create(organization=OrganizationFactory()),
    }
    fields.update(overrides)
    return EvidenceMention.objects.create(**fields)


@pytest.mark.django_db
class TestEvidenceExporter:
    def test_post_columns_carry_the_repost_separately(self):
        inner = _make_evidence("inner", text="quoted text").social_media_post
        evidence = _make_evidence(
            "outer", title="the title", description="a description"
        )
        evidence.social_media_post.redistributes = inner
        evidence.social_media_post.save(update_fields=["redistributes"])

        rows = _rows(Evidence.objects.filter(pk=evidence.pk))
        outer = evidence.social_media_post

        assert rows == [
            HEADER,
            [
                evidence.slug,
                "2026-01-15",
                "https://t.me/kanal_outer/1",
                "2026-01-02",
                str(outer.account),
                "the title",
                "post body",
                "a description",
                "quoted text",
                str(inner.account),
                *[""] * 9,
            ],
        ]

    def test_one_row_per_mention_repeating_the_post_columns(self):
        evidence = _make_evidence("m")
        _mention(
            evidence,
            "fn1",
            chapter_structure=["Kapitel", "Thema"],
            citation="the first quote",
            report_url="https://report.example/1",
            start=datetime.timedelta(seconds=5),
            end=datetime.timedelta(minutes=1, seconds=3),
        )
        _mention(evidence, "fn2", citation="the second quote")

        rows = _rows(Evidence.objects.all())[1:]
        assert len(rows) == 2
        # Mentions come out in footnote order, each repeating the post text.
        assert [_cell(row, "post_text") for row in rows] == ["post body"] * 2
        assert [_cell(row, "footnote") for row in rows] == ["fn1", "fn2"]
        assert [_cell(row, "citation") for row in rows] == [
            "the first quote",
            "the second quote",
        ]

        first, second = rows
        assert _cell(first, "chapter") == "Kapitel > Thema"
        assert _cell(first, "start") == "0:00:05"
        assert _cell(first, "end") == "0:01:03"
        assert _cell(first, "report_url") == "https://report.example/1"
        # A mention carrying none of the optional fields leaves them empty.
        assert _cell(second, "chapter") == ""
        assert _cell(second, "start") == ""
        assert _cell(second, "end") == ""
        assert _cell(second, "report_url") == ""

    def test_originator_columns_describe_the_mentions_own_actor(self):
        person = PersonFactory(
            first_name="Ada",
            last_name="Lovelace",
            verband=GeoRegionFactory(name="Bayern", kind="state"),
        )
        PoliticalPosition.objects.create(person=person, label="MdL")
        evidence = _make_evidence("p")
        _mention(evidence, "fn1", originator=Actor.objects.create(person=person))

        row = _rows(Evidence.objects.all())[1]
        assert _cell(row, "originator") == "Ada Lovelace"
        assert _cell(row, "political_position") == "MdL (Stand 24. Juni 2026)"
        assert _cell(row, "verband") == "Bayern"

    def test_an_organization_originator_has_no_political_position(self):
        evidence = _make_evidence("o")
        _mention(evidence, "fn1")  # originator is an organization

        row = _rows(Evidence.objects.all())[1]
        assert _cell(row, "political_position") == ""
        assert _cell(row, "verband") == ""

    def test_evidence_without_mentions_keeps_its_row(self):
        evidence = _make_evidence("lonely")
        rows = _rows(Evidence.objects.all())

        assert len(rows) == 2
        assert _cell(rows[1], "slug") == evidence.slug
        assert _cell(rows[1], "post_text") == "post body"
        assert _cell(rows[1], "footnote") == ""

    def test_evidence_without_text_keeps_its_row(self):
        evidence = _make_evidence("empty", text="")
        rows = _rows(Evidence.objects.all())

        assert rows[1] == [
            evidence.slug,
            "2026-01-15",
            "https://t.me/kanal_empty/1",
            "2026-01-02",
            str(evidence.social_media_post.account),
            *[""] * 14,
        ]

    def test_citation_column_is_redacted(self):
        evidence = _make_evidence("c")
        _mention(evidence, "fn1", citation="the Badword was spoken")
        RedactionRule.objects.create(pattern="Badword", placeholder="[X]")

        row = _rows(Evidence.objects.all())[1]
        assert _cell(row, "citation") == "the [X] was spoken"

    def test_post_columns_are_redacted(self):
        inner = _make_evidence("inner", text="Badword quoted").social_media_post
        evidence = _make_evidence("outer", title="Badword title", text="Badword mine")
        evidence.social_media_post.redistributes = inner
        evidence.social_media_post.save(update_fields=["redistributes"])
        RedactionRule.objects.create(pattern="Badword", placeholder="[X]")

        row = _rows(Evidence.objects.filter(pk=evidence.pk))[1]
        assert _cell(row, "post_title") == "[X] title"
        assert _cell(row, "post_text") == "[X] mine"
        assert _cell(row, "repost_text") == "[X] quoted"

    def test_rows_follow_the_descending_pk_order_of_the_queryset(self):
        first = _make_evidence("a", text="erst")
        second = _make_evidence("b", text="dann")

        assert first.pk < second.pk
        assert [
            _cell(row, "post_text") for row in _rows(Evidence.objects.all())[1:]
        ] == ["dann", "erst"]

    def test_rows_cost_no_query_per_evidence(self, django_assert_num_queries):
        inner = _make_evidence("inner").social_media_post
        for i in range(3):
            evidence = _make_evidence(f"q{i}")
            evidence.social_media_post.redistributes = inner
            evidence.social_media_post.save(update_fields=["redistributes"])
            person = PersonFactory(
                first_name=f"Ada{i}",
                last_name="Lovelace",
                verband=GeoRegionFactory(name="Bayern", kind="state"),
            )
            PoliticalPosition.objects.create(person=person, label="MdL")
            _mention(
                evidence,
                "fn1",
                citation="a quote",
                originator=Actor.objects.create(person=person),
            )

        # Warm the module-level global redactor, so what's left to count is the
        # queryset and its prefetches.
        assert Evidence.objects.first().redacted_text_block is not None

        with django_assert_num_queries(5):
            rows = _rows(Evidence.objects.all())

        # Header, the three reposting evidence, and the reposted one.
        assert len(rows) == 5

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
