from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import translation
from froide_evidencecollection.tasks import import_evidence_gsheet


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("--ignore-ids", action="store_true")
        return super().add_arguments(parser)

    def handle(self, *args, **options):
        translation.activate(settings.LANGUAGE_CODE)
        import_evidence_gsheet(ignore_existing_ids=options["ignore_ids"])
