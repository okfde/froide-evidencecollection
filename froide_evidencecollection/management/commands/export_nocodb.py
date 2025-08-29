from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import translation

from froide_evidencecollection.tasks import export_evidence_nocodb


class Command(BaseCommand):
    help = "Export data to NocoDB via API"

    def handle(self, *args, **options):
        translation.activate(settings.LANGUAGE_CODE)
        export_evidence_nocodb()
