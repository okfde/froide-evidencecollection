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
    quality = fields.IntegerField(attr="quality_id")
    public_body = fields.IntegerField(attr="source__public_body_id")

    class Django:
        model = Evidence
        fields = ["description", "note", "date"]


@person_index.document
class PersonDocument(Document):
    class Django:
        model = Person
        fields = ["name", "note"]
