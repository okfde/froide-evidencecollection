from django_elasticsearch_dsl import Document, fields

from froide.helper.search import (
    get_index,
)

from .models import Evidence, Person

evidence_index = get_index("evidence")
person_index = get_index("person")


@evidence_index.document
class EvidenceDocument(Document):
    type = fields.IntegerField(attr="type_id")
    area = fields.IntegerField(attr="area_id")
    person = fields.IntegerField(attr="person_id")
    person_name = fields.TextField()
    quality = fields.IntegerField(attr="quality_id")
    public_body = fields.IntegerField(attr="source__public_body_id")

    class Django:
        model = Evidence
        fields = ["description", "note", "date"]

    def get_queryset(self):
        return (
            super()
            .get_queryset()
            .select_related("type", "area", "person", "quality", "source__public_body")
        )

    def prepare_person_name(self, obj: Evidence):
        return obj.person.name


@person_index.document
class PersonDocument(Document):
    class Django:
        model = Person
        fields = ["name", "note"]
