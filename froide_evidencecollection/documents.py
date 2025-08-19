from django_elasticsearch_dsl import Document, fields

from froide.helper.search import (
    get_index,
    get_search_analyzer,
    get_search_quote_analyzer,
    get_text_analyzer,
)

from .models import Evidence, Person

evidence_index = get_index("evidence")
person_index = get_index("person")
analyzer = get_text_analyzer()
search_analyzer = get_search_analyzer()
search_quote_analyzer = get_search_quote_analyzer()


@evidence_index.document
class EvidenceDocument(Document):
    evidence_type = fields.IntegerField(attr="evidence_type_id")
    originators = fields.ListField(fields.IntegerField())
    originator_names = fields.TextField(
        analyzer=analyzer,
        search_analyzer=search_analyzer,
        search_quote_analyzer=search_quote_analyzer,
        index_options="offsets",
    )

    class Django:
        model = Evidence
        fields = ["citation", "description", "event_date"]
        fts_fields = ["citation"]

    def get_queryset(self):
        return (
            super()
            .get_queryset()
            .prefetch_related("originators")
            .select_related("evidence_type")
        )

    def prepare_originators(self, obj: Evidence):
        return list(obj.originators.values_list("id", flat=True))

    def prepare_originator_names(self, obj: Evidence):
        return list(obj.originators.values_list("name", flat=True))

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
