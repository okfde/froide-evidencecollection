from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import translation

from froide_evidencecollection.enricher import AbgeordnetenwatchEnricher


class Command(BaseCommand):
    help = "Import data from NocoDB via API"

    def handle(self, *args, **options):
        translation.activate(settings.LANGUAGE_CODE)
        enricher = AbgeordnetenwatchEnricher()
        enricher.setup()
