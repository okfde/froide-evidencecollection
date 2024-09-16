from django_elasticsearch_dsl import Document

from froide.helper.search import (
    get_index,
)

from .models import Evidence, Person

evidence_index = get_index("evidence")
person_index = get_index("person")


@evidence_index.document
class EvidenceDocument(Document):
    class Django:
        model = Evidence
        fields = ["description"]


@evidence_index.document
class PersonDocument(Document):
    class Django:
        model = Person
        fields = ["name", "note"]
