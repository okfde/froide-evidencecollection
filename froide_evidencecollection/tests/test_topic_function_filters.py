"""Tests for the topic cloud's originator-function filters: narrowing the
evidence set by the role (function), institutional level and region of the
political position the posting person held *when the evidence was posted*.
"""

import datetime

from django.test import RequestFactory
from django.utils import timezone

import pytest

from froide_evidencecollection.models import (
    Actor,
    Evidence,
    PoliticalPosition,
    SocialMediaAccount,
    SocialMediaPost,
)
from froide_evidencecollection.tests.factories import (
    GeoRegionFactory,
    InstitutionalLevelFactory,
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
        self.region = GeoRegionFactory()

        # Person held role_a from 2020 through 2021; the evidence below is
        # posted inside (2020-06) and outside (2023-06) that window.
        self.person = PersonFactory(first_name="Ada", last_name="Lovelace")
        self.actor = Actor.objects.create(person=self.person)
        self.position = PoliticalPosition.objects.create(
            person=self.person,
            type=PoliticalPosition.Type.MANDATE,
            label="Member",
            role=self.role_a,
            institutional_level=self.level,
            region=self.region,
            start_date=datetime.date(2020, 1, 1),
            end_date=datetime.date(2021, 12, 31),
        )

        tz = timezone.get_current_timezone()
        self.inside = _posted_evidence(
            self.actor, datetime.datetime(2020, 6, 1, 12, tzinfo=tz), 1
        )
        self.outside = _posted_evidence(
            self.actor, datetime.datetime(2023, 6, 1, 12, tzinfo=tz), 2
        )

    def test_role_matches_only_while_position_was_active(self):
        ids = _filtered_ids({"role": str(self.role_a.id)})
        assert ids == {self.inside.pk}

    def test_unheld_role_matches_nothing(self):
        assert _filtered_ids({"role": str(self.role_b.id)}) == set()

    def test_level_is_time_bounded_too(self):
        ids = _filtered_ids({"level": str(self.level.id)})
        assert ids == {self.inside.pk}

    def test_region_is_time_bounded_too(self):
        ids = _filtered_ids({"region": str(self.region.id)})
        assert ids == {self.inside.pk}

    def test_combined_params_bind_to_the_same_active_position(self):
        ids = _filtered_ids(
            {
                "role": str(self.role_a.id),
                "level": str(self.level.id),
                "region": str(self.region.id),
            }
        )
        assert ids == {self.inside.pk}

    def test_role_and_region_from_different_positions_do_not_match(self):
        # A second position carries the region but a *different* role, and the
        # first carries the role but (say) no region. Selecting role_b + region
        # must not match by stitching the two positions together — the filter
        # binds all attributes to one position.
        other_region = GeoRegionFactory()
        PoliticalPosition.objects.create(
            person=self.person,
            type=PoliticalPosition.Type.PARTY,
            label="Spokesperson",
            role=self.role_b,
            region=other_region,
            start_date=datetime.date(2020, 1, 1),
            end_date=datetime.date(2021, 12, 31),
        )
        # role_b lives on the second position, self.region on the first.
        ids = _filtered_ids(
            {"role": str(self.role_b.id), "region": str(self.region.id)}
        )
        assert ids == set()
        # role_b with its own region does match (inside the window).
        ids = _filtered_ids(
            {"role": str(self.role_b.id), "region": str(other_region.id)}
        )
        assert ids == {self.inside.pk}

    def test_open_ended_position_has_no_upper_bound(self):
        self.position.end_date = None
        self.position.save()
        ids = _filtered_ids({"role": str(self.role_a.id)})
        assert ids == {self.inside.pk, self.outside.pk}

    def test_no_function_params_leaves_set_unfiltered(self):
        assert _filtered_ids({}) == {self.inside.pk, self.outside.pk}
