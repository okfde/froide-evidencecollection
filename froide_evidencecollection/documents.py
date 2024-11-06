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
    type = fields.IntegerField(attr="type_id")
    area = fields.IntegerField(attr="area_id")
    person = fields.IntegerField(attr="person_id")
    person_name = fields.TextField(
        analyzer=analyzer,
        search_analyzer=search_analyzer,
        search_quote_analyzer=search_quote_analyzer,
        index_options="offsets",
    )
    quality = fields.IntegerField(attr="quality_id")
    public_body = fields.IntegerField(attr="source__public_body_id")

    class Django:
        model = Evidence
        fields = ["title", "description", "note", "date"]
        fts_fields = ["title", "description", "note"]

    def get_queryset(self):
        return (
            super()
            .get_queryset()
            .select_related("type", "area", "person", "quality", "source__public_body")
        )

    def prepare_person_name(self, obj: Evidence):
        return obj.person.name

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
        fields = ["name", "note"]
