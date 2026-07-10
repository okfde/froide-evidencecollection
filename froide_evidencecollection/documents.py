from django_elasticsearch_dsl import Document as DSLDocument
from django_elasticsearch_dsl import fields

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


def _make_text_field():
    return fields.TextField(
        analyzer=analyzer,
        search_analyzer=search_analyzer,
        search_quote_analyzer=search_quote_analyzer,
        index_options="offsets",
    )


@evidence_index.document
class EvidenceDocument(DSLDocument):
    # Concatenated source text (post body/title/description, redistributed
    # content, …), assembled by Evidence.search_text with redaction rules
    # already applied.
    content = _make_text_field()

    class Django:
        model = Evidence
        fields = []

    def get_queryset(self):
        # `content` (= search_text) walks the source's text segments, including
        # redistributed posts, and redacts them against the post's scoped rules;
        # pull the source, its redistribution chain and those rules in one go so
        # indexing doesn't fan out per row.
        return (
            super()
            .get_queryset()
            .select_related(
                "social_media_post__account",
                "social_media_post__redistributes__account",
            )
            .prefetch_related("social_media_post__redaction_rules")
        )

    def prepare_content(self, obj: Evidence):
        return obj.search_text


@person_index.document
class PersonDocument(DSLDocument):
    class Django:
        model = Person
        fields = ["first_name", "last_name"]
