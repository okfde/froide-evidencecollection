from django.contrib.gis.geos import MultiPolygon, Polygon

import factory
from factory.django import DjangoModelFactory

from froide.georegion.models import GeoRegion
from froide_evidencecollection.models import (
    Affiliation,
    InstitutionalLevel,
    Organization,
    Person,
    Role,
)


class PersonFactory(DjangoModelFactory):
    class Meta:
        model = Person


class InstitutionalLevelFactory(DjangoModelFactory):
    class Meta:
        model = InstitutionalLevel

    name = factory.Sequence(lambda n: f"institutional level {n}")


class OrganizationFactory(DjangoModelFactory):
    class Meta:
        model = Organization

    institutional_level = factory.SubFactory(InstitutionalLevelFactory)


class RoleFactory(DjangoModelFactory):
    class Meta:
        model = Role

    name = factory.Sequence(lambda n: f"Role {n}")


class AffiliationFactory(DjangoModelFactory):
    class Meta:
        model = Affiliation

    person = factory.SubFactory(PersonFactory)
    organization = factory.SubFactory(OrganizationFactory)
    role = factory.SubFactory(RoleFactory)


class GeoRegionFactory(DjangoModelFactory):
    class Meta:
        model = GeoRegion

    geom = factory.LazyFunction(
        lambda: MultiPolygon(Polygon(((0, 0), (0, 1), (1, 1), (1, 0), (0, 0))))
    )
    depth = 1
    path = factory.Sequence(lambda n: f"{n:04d}")


syncable_model_factories = [
    PersonFactory,
    OrganizationFactory,
    RoleFactory,
    AffiliationFactory,
]
