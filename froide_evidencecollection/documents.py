from django_elasticsearch_dsl import Document, fields

from froide.helper.search import (
    get_index,
    get_search_analyzer,
    get_search_quote_analyzer,
    get_text_analyzer,
)

from .models import EvidenceNew, PersonOrOrganization

evidence_index = get_index("evidence")
person_index = get_index("person")
analyzer = get_text_analyzer()
search_analyzer = get_search_analyzer()
search_quote_analyzer = get_search_quote_analyzer()


@evidence_index.document
class EvidenceDocument(Document):
    type = fields.IntegerField(attr="type_id")
    fdgo_features = fields.ListField(fields.IntegerField(attr="fdgo_features__id"))
    spread_level = fields.IntegerField(attr="spread_level_id")
    distribution_channels = fields.ListField(
        fields.IntegerField(attr="distribution_channels__id")
    )
    persons = fields.ListField(fields.IntegerField())
    person_names = fields.TextField(
        analyzer=analyzer,
        search_analyzer=search_analyzer,
        search_quote_analyzer=search_quote_analyzer,
        index_options="offsets",
    )
    public_bodies = fields.ListField(fields.IntegerField())

    class Django:
        model = EvidenceNew
        fields = ["description", "date"]
        fts_fields = ["description"]

    def get_queryset(self):
        return (
            super()
            .get_queryset()
            .prefetch_related(
                "fdgo_features",
                "distribution_channels",
                "sources__persons_or_organizations",
                "sources__recorded_by",
            )
            .select_related("type", "spread_level")
        )

    def prepare_persons(self, obj: EvidenceNew):
        return list(obj.persons_or_organizations.values_list("id", flat=True))

    def prepare_person_names(self, obj: EvidenceNew):
        return list(obj.persons_or_organizations.values_list("name", flat=True))

    def prepare_public_bodies(self, obj: EvidenceNew):
        return list(obj.public_bodies.values_list("id", flat=True))

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
        model = PersonOrOrganization
        fields = ["name"]
