from django.db import models as db_models

from django_elasticsearch_dsl import Document, fields

from froide.helper.search import (
    get_index,
    get_search_analyzer,
    get_search_quote_analyzer,
    get_text_analyzer,
)

from .models import Affiliation, Evidence, Person

evidence_index = get_index("evidence")
person_index = get_index("person")
analyzer = get_text_analyzer()
search_analyzer = get_search_analyzer()
search_quote_analyzer = get_search_quote_analyzer()


def _make_text_field():
    return fields.TextField(
        analyzer=analyzer,
        search_analyzer=search_analyzer,
        search_quote_analyzer=search_quote_analyzer,
        index_options="offsets",
    )


@evidence_index.document
class EvidenceDocument(Document):
    evidence_type = fields.IntegerField(attr="evidence_type_id")

    originators = fields.ListField(fields.IntegerField())
    originator_names = _make_text_field()

    categories = fields.ListField(fields.IntegerField())
    category_names = fields.KeywordField()

    platform = fields.KeywordField()

    # Originator affiliation metadata, resolved to publishing_date when available.
    originator_organizations = fields.ListField(fields.IntegerField())
    originator_organization_names = _make_text_field()
    originator_roles = fields.ListField(fields.IntegerField())
    originator_institutional_levels = fields.ListField(fields.IntegerField())

    class Django:
        model = Evidence
        fields = ["citation", "description", "publishing_date"]
        # Fields to be indexed for full text search.
        fts_fields = ["citation", "description"]

    def get_queryset(self):
        return (
            super()
            .get_queryset()
            .prefetch_related(
                "originators",
                "originators__person__affiliations__organization__institutional_level",
                "originators__person__affiliations__role",
                "mentions__category",
            )
            .select_related("evidence_type", "posted_by")
        )

    def _get_active_affiliations(self, obj: Evidence):
        """Return affiliations active at publishing_date, or all if no date."""
        person_ids = obj.originators.filter(person__isnull=False).values_list(
            "person_id", flat=True
        )

        affiliations = Affiliation.objects.filter(person_id__in=person_ids)

        if obj.publishing_date:
            affiliations = affiliations.filter(
                db_models.Q(start_date__isnull=True)
                | db_models.Q(start_date__lte=obj.publishing_date),
                db_models.Q(end_date__isnull=True)
                | db_models.Q(end_date__gte=obj.publishing_date),
            )

        return affiliations

    def prepare_originators(self, obj: Evidence):
        return list(obj.originators.values_list("id", flat=True))

    def prepare_originator_names(self, obj: Evidence):
        return list(obj.originators.values_list("name", flat=True))

    def prepare_categories(self, obj: Evidence):
        return list(obj.mentions.values_list("category_id", flat=True).distinct())

    def prepare_category_names(self, obj: Evidence):
        return list(obj.mentions.values_list("category__name", flat=True).distinct())

    def prepare_platform(self, obj: Evidence):
        if obj.posted_by:
            return obj.posted_by.platform
        return None

    def prepare_originator_organizations(self, obj: Evidence):
        return list(
            self._get_active_affiliations(obj)
            .values_list("organization_id", flat=True)
            .distinct()
        )

    def prepare_originator_organization_names(self, obj: Evidence):
        return list(
            self._get_active_affiliations(obj)
            .values_list("organization__organization_name", flat=True)
            .distinct()
        )

    def prepare_originator_roles(self, obj: Evidence):
        return list(
            self._get_active_affiliations(obj)
            .exclude(role_id__isnull=True)
            .values_list("role_id", flat=True)
            .distinct()
        )

    def prepare_originator_institutional_levels(self, obj: Evidence):
        return list(
            self._get_active_affiliations(obj)
            .values_list("organization__institutional_level_id", flat=True)
            .distinct()
        )

    @classmethod
    def to_field(cls, field_name, model_field):
        if field_name in cls.Django.fts_fields:
            return fields.TextField(
                analyzer=analyzer,
                search_analyzer=search_analyzer,
                search_quote_analyzer=search_quote_analyzer,
                index_options="offsets",
            )
        else:
            return super().to_field(field_name, model_field)


@person_index.document
class PersonDocument(Document):
    class Django:
        model = Person
        fields = ["first_name", "last_name"]
