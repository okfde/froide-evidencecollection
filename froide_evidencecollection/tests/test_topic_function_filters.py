"""Tests for the topic cloud's originator filters: narrowing the evidence set
by the role (function) and institutional level of a political position the
posting person held, and by the originator's Verband (a direct actor
attribute).
"""

import datetime

from django.test import RequestFactory
from django.utils import timezone

import pytest

from froide_evidencecollection.models import (
    Actor,
    Chapter,
    Evidence,
    EvidenceMention,
    PoliticalPosition,
    SocialMediaAccount,
    SocialMediaPost,
)
from froide_evidencecollection.tests.factories import (
    GeoRegionFactory,
    InstitutionalLevelFactory,
    OrganizationFactory,
    PersonFactory,
    RoleFactory,
)
from froide_evidencecollection.views import EvidenceTopicCloudView


def _posted_evidence(actor, posted_at, ext_id):
    """A topic-fitted, social-media-backed evidence with ``actor`` as its
    originator, posted at ``posted_at`` (an aware datetime). The actor is linked
    via ``Evidence.originators`` (the import-populated relation); the scraped
    account is intentionally not linked to an actor, mirroring production."""
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
        posted_at=posted_at,
    )
    evidence = Evidence.objects.create(
        social_media_post=post,
        topic_fit_at=timezone.now(),
    )
    evidence.originators.add(actor)
    return evidence


def _filtered_ids(params):
    """Run the view's queryset filter for ``params`` and return the matching
    evidence ids as a set."""
    view = EvidenceTopicCloudView()
    view.request = RequestFactory().get("/", params)
    return set(view._filter_qs().values_list("pk", flat=True))


@pytest.mark.django_db
class TestPoliticalPositionFilter:
    def setup_method(self):
        self.role_a = RoleFactory()
        self.role_b = RoleFactory()
        self.level = InstitutionalLevelFactory()

        # Person holds role_a at self.level; the two evidences below are posted
        # on different dates, but the filter is not time-bound, so both match.
        self.person = PersonFactory(first_name="Ada", last_name="Lovelace")
        self.actor = Actor.objects.create(person=self.person)
        self.position = PoliticalPosition.objects.create(
            person=self.person,
            role=self.role_a,
            institutional_level=self.level,
        )

        tz = timezone.get_current_timezone()
        self.ev1 = _posted_evidence(
            self.actor, datetime.datetime(2020, 6, 1, 12, tzinfo=tz), 1
        )
        self.ev2 = _posted_evidence(
            self.actor, datetime.datetime(2023, 6, 1, 12, tzinfo=tz), 2
        )

    def test_role_matches_all_posts_of_the_holder(self):
        ids = _filtered_ids({"role": str(self.role_a.id)})
        assert ids == {self.ev1.pk, self.ev2.pk}

    def test_unheld_role_matches_nothing(self):
        assert _filtered_ids({"role": str(self.role_b.id)}) == set()

    def test_level_matches_all_posts_of_the_holder(self):
        ids = _filtered_ids({"level": str(self.level.id)})
        assert ids == {self.ev1.pk, self.ev2.pk}

    def test_combined_params_bind_to_the_same_position(self):
        ids = _filtered_ids(
            {
                "role": str(self.role_a.id),
                "level": str(self.level.id),
            }
        )
        assert ids == {self.ev1.pk, self.ev2.pk}

    def test_role_and_level_from_different_positions_do_not_match(self):
        # A second position carries a different role and its own level, while the
        # first carries role_a and self.level. Selecting role_b + self.level must
        # not match by stitching the two positions together — the filter binds
        # all attributes to one position.
        other_level = InstitutionalLevelFactory()
        PoliticalPosition.objects.create(
            person=self.person,
            role=self.role_b,
            institutional_level=other_level,
        )
        # role_b lives on the second position, self.level on the first.
        ids = _filtered_ids({"role": str(self.role_b.id), "level": str(self.level.id)})
        assert ids == set()
        # role_b with its own level does match.
        ids = _filtered_ids({"role": str(self.role_b.id), "level": str(other_level.id)})
        assert ids == {self.ev1.pk, self.ev2.pk}

    def test_no_function_params_leaves_set_unfiltered(self):
        assert _filtered_ids({}) == {self.ev1.pk, self.ev2.pk}


@pytest.mark.django_db
class TestVerbandFilter:
    def setup_method(self):
        tz = timezone.get_current_timezone()
        self.bayern = GeoRegionFactory(name="Bayern", kind="state")
        self.bund = GeoRegionFactory(name="Deutschland", kind="country")

        # A person in the Bayern verband and an organization at the Bund level,
        # each posting one (date-irrelevant) evidence.
        person = PersonFactory(
            first_name="Ada", last_name="Lovelace", verband=self.bayern
        )
        self.person_actor = Actor.objects.create(person=person)
        self.person_ev = _posted_evidence(
            self.person_actor, datetime.datetime(2020, 6, 1, 12, tzinfo=tz), 1
        )

        org = OrganizationFactory(organization_name="Bundespartei", verband=self.bund)
        self.org_actor = Actor.objects.create(organization=org)
        self.org_ev = _posted_evidence(
            self.org_actor, datetime.datetime(2023, 6, 1, 12, tzinfo=tz), 2
        )

        # A verband-less originator never matches a verband filter.
        bare = PersonFactory(first_name="No", last_name="Verband", verband=None)
        self.bare_ev = _posted_evidence(
            Actor.objects.create(person=bare),
            datetime.datetime(2021, 6, 1, 12, tzinfo=tz),
            3,
        )

    def test_state_verband_matches_person_originator(self):
        assert _filtered_ids({"verband": str(self.bayern.id)}) == {self.person_ev.pk}

    def test_country_verband_matches_organization_originator(self):
        # "Bund" is the country-level region on the organization side.
        assert _filtered_ids({"verband": str(self.bund.id)}) == {self.org_ev.pk}

    def test_unset_verband_param_leaves_set_unfiltered(self):
        assert _filtered_ids({}) == {
            self.person_ev.pk,
            self.org_ev.pk,
            self.bare_ev.pk,
        }

    def test_verband_is_not_time_bounded(self):
        # Unlike role/level, the verband filter ignores the post date: the
        # org evidence posted in 2023 still matches its Bund verband.
        assert self.org_ev.pk in _filtered_ids({"verband": str(self.bund.id)})

    def test_originators_with_verband_pairs_each_name_with_its_label(self):
        # The verband-less originator shows its bare name (no parentheses); the
        # others append their own label.
        rows = EvidenceTopicCloudView._originators_with_verband(
            [self.person_ev, self.org_ev, self.bare_ev]
        )
        assert rows == {
            self.person_ev.pk: "Ada Lovelace (Bayern)",
            self.org_ev.pk: "Bundespartei (Bund)",
            self.bare_ev.pk: "No Verband",
        }

    def test_originators_with_verband_lines_up_several_originators(self):
        # An evidence with two originators lists each name with its own Verband,
        # comma-joined in originator order.
        tz = timezone.get_current_timezone()
        multi_ev = _posted_evidence(
            self.person_actor, datetime.datetime(2022, 6, 1, 12, tzinfo=tz), 4
        )
        multi_ev.originators.add(self.org_actor)
        rows = EvidenceTopicCloudView._originators_with_verband([multi_ev])
        # Order follows `originators`, which isn't a guaranteed sort here, so
        # compare the pieces rather than their order.
        assert set(rows[multi_ev.pk].split(", ")) == {
            "Ada Lovelace (Bayern)",
            "Bundespartei (Bund)",
        }


@pytest.mark.django_db
class TestChaptersByEvidence:
    """`_chapters_by_evidence` resolves each evidence's mention chapters to their
    labels, comma-joins distinct ones, and omits evidence with no chapter."""

    def setup_method(self):
        tz = timezone.get_current_timezone()
        self.actor = Actor.objects.create(
            person=PersonFactory(first_name="Ada", last_name="Lovelace")
        )
        self.ch_a = Chapter.add_root(custom_label="Climate")
        self.ch_b = Chapter.add_root(custom_label="Migration")

        # One evidence filed under two distinct chapters (and a duplicate, which
        # must collapse), one under a single chapter, one with a chapterless
        # mention (absent from the map).
        self.multi_ev = self._evidence(1, tz)
        self._mention(self.multi_ev, self.ch_a)
        self._mention(self.multi_ev, self.ch_b)
        self._mention(self.multi_ev, self.ch_a)
        self.single_ev = self._evidence(2, tz)
        self._mention(self.single_ev, self.ch_a)
        self.bare_ev = self._evidence(3, tz)
        self._mention(self.bare_ev, None)

    def _evidence(self, ext_id, tz):
        return _posted_evidence(
            self.actor, datetime.datetime(2021, 1, 1, 12, tzinfo=tz), ext_id
        )

    def _mention(self, evidence, chapter):
        # Footnotes only need to be unique per evidence; a running counter keeps
        # the several mentions on ``multi_ev`` distinct.
        self._footnote_seq = getattr(self, "_footnote_seq", 0) + 1
        EvidenceMention.objects.create(
            evidence=evidence,
            originator=self.actor,
            chapter=chapter,
            footnote=str(self._footnote_seq),
        )

    def test_labels_distinct_chapters_and_omits_chapterless(self):
        labels = EvidenceTopicCloudView._chapters_by_evidence(
            [self.multi_ev, self.single_ev, self.bare_ev]
        )
        # The chapterless evidence is absent; the single-chapter one is plain.
        assert self.bare_ev.pk not in labels
        assert labels[self.single_ev.pk] == "Climate"
        # The multi-chapter one joins its two distinct chapters (the duplicate
        # collapses); mention order isn't a deterministic tiebreak, so compare
        # the set of labels rather than their order.
        assert set(labels[self.multi_ev.pk].split(", ")) == {"Climate", "Migration"}
