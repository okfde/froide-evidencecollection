import factory
from factory.django import DjangoModelFactory

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


syncable_model_factories = [
    PersonFactory,
    OrganizationFactory,
    RoleFactory,
    AffiliationFactory,
]
