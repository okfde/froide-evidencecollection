"""Tests for the topic cloud's actor surfaces: the "Actors in view" side panel,
the per-dot ``data-actor`` highlight hook, the actor dropdown options, the
``actor`` filter and the table-view originator column.

All of these read the actor from ``Evidence.originators`` (the import-populated
relation); the scraped ``SocialMediaAccount`` is intentionally never linked to
an actor, so none of the fixtures here set ``account.actor``.
"""

import re

from django.test import RequestFactory, override_settings
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


def _fitted_evidence(ext_id, originators, *, x=None, y=None):
    """A topic-fitted, social-media-backed evidence with ``originators`` (a list
    of Actors) and optional 2D coords so it renders a dot."""
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
        topic_fit_at=timezone.now(),
        topic_x=x,
        topic_y=y,
    )
    ev.originators.add(*originators)
    return ev


@override_settings(ROOT_URLCONF="froide_evidencecollection.tests.urls")
def _context(params=None):
    view = EvidenceTopicCloudView()
    view.request = RequestFactory().get("/", params or {})
    view.kwargs = {}
    return view.get_context_data()


@pytest.mark.django_db
class TestActorsInViewPanel:
    def test_tallies_originators_across_visible_evidence(self):
        ada = _person_actor("Ada", "Lovelace")
        org = _org_actor("Acme")
        _fitted_evidence(1, [ada])
        _fitted_evidence(2, [ada])
        _fitted_evidence(3, [org])

        panel = _context()["actors_in_view"]

        # Sorted by descending count, then name: Ada (2) before Acme (1).
        assert [(a["pk"], a["count"]) for a in panel] == [
            (ada.id, 2),
            (org.id, 1),
        ]
        assert panel[0]["name"] == str(ada)

    def test_multi_originator_evidence_counts_for_each(self):
        ada = _person_actor("Ada", "Lovelace")
        org = _org_actor("Acme")
        _fitted_evidence(1, [ada, org])

        panel = _context()["actors_in_view"]

        assert {(a["pk"], a["count"]) for a in panel} == {
            (ada.id, 1),
            (org.id, 1),
        }

    def test_evidence_without_originator_contributes_nothing(self):
        _fitted_evidence(1, [])

        assert _context()["actors_in_view"] == []


@pytest.mark.django_db
class TestDotDataActor:
    def _data_actor_values(self, svg):
        """All space-split id lists from the ``data-actor`` attributes in the
        rendered cloud SVG."""
        return [m.split() for m in re.findall(r'data-actor="([^"]*)"', svg)]

    def test_dot_lists_all_originator_ids(self):
        ada = _person_actor("Ada", "Lovelace")
        org = _org_actor("Acme")
        _fitted_evidence(1, [ada, org], x=0.0, y=0.0)

        svg = _context()["cloud_circles_svg"]

        [ids] = self._data_actor_values(svg)
        assert set(ids) == {str(ada.id), str(org.id)}

    def test_dot_without_originator_has_empty_data_actor(self):
        _fitted_evidence(1, [], x=0.0, y=0.0)

        svg = _context()["cloud_circles_svg"]

        # The attribute is present but empty, so the JS membership test misses.
        assert 'data-actor=""' in svg


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


@pytest.mark.django_db
class TestActorDropdown:
    def test_lists_only_actors_with_fitted_evidence(self):
        with_fit = _person_actor("Ada", "Lovelace")
        _fitted_evidence(1, [with_fit])
        # An actor that only originates an unfitted evidence stays out.
        without_fit = _person_actor("Grace", "Hopper")
        unfitted = Evidence.objects.create(
            social_media_post=SocialMediaPost.objects.create(
                account=SocialMediaAccount.objects.create(
                    platform=SocialMediaAccount.Platform.TELEGRAM,
                    username="u9",
                    platform_user_id="9",
                ),
                platform_post_id="9",
                url="https://t.me/example/9",
                text="post body",
            ),
            topic_fit_at=None,
        )
        unfitted.originators.add(without_fit)

        actor_ids = [a.id for a in _context()["actors"]]

        assert actor_ids == [with_fit.id]


@pytest.mark.django_db
class TestOutlineActors:
    def test_table_item_joins_originator_names(self):
        ada = _person_actor("Ada", "Lovelace")
        org = _org_actor("Acme")
        ev = _fitted_evidence(1, [ada, org])

        [item] = [
            it
            for it in _context()["outline_items"]
            if it["post"].pk == ev.social_media_post_id
        ]

        names = item["actors"].split(", ")
        assert set(names) == {str(ada), str(org)}
