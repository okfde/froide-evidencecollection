"""Tests for the topic cloud's actor surfaces: the "Actors in view" side panel,
the per-dot ``data-actor`` highlight hook, the actor dropdown options, the
``actor`` filter and the table-view originator column.

All of these read the actor from ``Evidence.originators`` (the import-populated
relation); the scraped ``SocialMediaAccount`` is intentionally never linked to
an actor, so none of the fixtures here set ``account.actor``. The panel, dot
hook, dropdown and table column are pure helpers on the view, so they test
directly without a request.
"""

from django.test import RequestFactory
from django.utils import timezone

import pytest

from froide_evidencecollection.models import (
    Actor,
    Evidence,
    SocialMediaAccount,
    SocialMediaPost,
)
from froide_evidencecollection.views import EvidenceTopicCloudView

from .factories import OrganizationFactory, PersonFactory


def _person_actor(first, last):
    return Actor.objects.create(person=PersonFactory(first_name=first, last_name=last))


def _org_actor(name):
    return Actor.objects.create(
        organization=OrganizationFactory(organization_name=name)
    )


def _fitted_evidence(ext_id, originators, *, fitted=True):
    """A social-media-backed evidence with ``originators`` (a list of Actors),
    topic-fitted by default."""
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
        posted_at=timezone.now(),
    )
    ev = Evidence.objects.create(
        social_media_post=post,
        topic_fit_at=timezone.now() if fitted else None,
    )
    ev.originators.add(*originators)
    return ev


@pytest.mark.django_db
class TestActorsInViewPanel:
    def test_tallies_originators_across_visible_evidence(self):
        ada = _person_actor("Ada", "Lovelace")
        org = _org_actor("Acme")
        evidences = [
            _fitted_evidence(1, [ada]),
            _fitted_evidence(2, [ada]),
            _fitted_evidence(3, [org]),
        ]

        panel = EvidenceTopicCloudView._actors_in_view(evidences)

        # Sorted by descending count, then name: Ada (2) before Acme (1).
        assert [(a["pk"], a["count"]) for a in panel] == [
            (ada.id, 2),
            (org.id, 1),
        ]
        assert panel[0]["name"] == str(ada)

    def test_multi_originator_evidence_counts_for_each(self):
        ada = _person_actor("Ada", "Lovelace")
        org = _org_actor("Acme")
        ev = _fitted_evidence(1, [ada, org])

        panel = EvidenceTopicCloudView._actors_in_view([ev])

        assert {(a["pk"], a["count"]) for a in panel} == {
            (ada.id, 1),
            (org.id, 1),
        }

    def test_count_ties_break_on_name(self):
        # Equal counts → alphabetical by name (case-insensitive).
        zed = _person_actor("Zed", "Zebra")
        amy = _person_actor("Amy", "Apple")
        panel = EvidenceTopicCloudView._actors_in_view(
            [_fitted_evidence(1, [zed, amy])]
        )

        assert [a["name"] for a in panel] == [str(amy), str(zed)]

    def test_evidence_without_originator_contributes_nothing(self):
        assert EvidenceTopicCloudView._actors_in_view([_fitted_evidence(1, [])]) == []


@pytest.mark.django_db
class TestDotDataActor:
    def test_lists_all_originator_ids_space_separated(self):
        ada = _person_actor("Ada", "Lovelace")
        org = _org_actor("Acme")
        ev = _fitted_evidence(1, [ada, org])

        # The JS highlight splits on space and tests membership.
        ids = EvidenceTopicCloudView._originator_ids(ev).split()

        assert set(ids) == {str(ada.id), str(org.id)}

    def test_empty_without_originator(self):
        ev = _fitted_evidence(1, [])

        assert EvidenceTopicCloudView._originator_ids(ev) == ""


@pytest.mark.django_db
class TestOutlineActors:
    def test_joins_originator_names(self):
        ada = _person_actor("Ada", "Lovelace")
        org = _org_actor("Acme")
        ev = _fitted_evidence(1, [ada, org])

        names = EvidenceTopicCloudView._originator_names(ev).split(", ")

        assert set(names) == {str(ada), str(org)}

    def test_empty_without_originator(self):
        ev = _fitted_evidence(1, [])

        assert EvidenceTopicCloudView._originator_names(ev) == ""


@pytest.mark.django_db
class TestActorDropdown:
    def test_lists_only_actors_with_fitted_evidence(self):
        with_fit = _person_actor("Ada", "Lovelace")
        _fitted_evidence(1, [with_fit])
        # An actor that only originates an unfitted evidence stays out.
        without_fit = _person_actor("Grace", "Hopper")
        _fitted_evidence(2, [without_fit], fitted=False)

        actor_ids = [a.id for a in EvidenceTopicCloudView._actor_options()]

        assert actor_ids == [with_fit.id]

    def test_sorted_by_name_case_insensitively(self):
        zed = _person_actor("Zed", "Zebra")
        amy = _person_actor("amy", "apple")
        _fitted_evidence(1, [zed])
        _fitted_evidence(2, [amy])

        assert [a.id for a in EvidenceTopicCloudView._actor_options()] == [
            amy.id,
            zed.id,
        ]


@pytest.mark.django_db
class TestActorFilter:
    def test_filters_to_evidence_with_that_originator(self):
        ada = _person_actor("Ada", "Lovelace")
        org = _org_actor("Acme")
        kept = _fitted_evidence(1, [ada])
        _fitted_evidence(2, [org])

        view = EvidenceTopicCloudView()
        view.request = RequestFactory().get("/", {"actor": str(ada.id)})
        ids = set(view._filter_qs().values_list("pk", flat=True))

        assert ids == {kept.pk}

    def test_multi_originator_evidence_not_double_counted(self):
        ada = _person_actor("Ada", "Lovelace")
        org = _org_actor("Acme")
        ev = _fitted_evidence(1, [ada, org])

        view = EvidenceTopicCloudView()
        view.request = RequestFactory().get("/", {"actor": str(ada.id)})
        ids = list(view._filter_qs().values_list("pk", flat=True))

        # distinct() folds the to-many originators join.
        assert ids == [ev.pk]
