from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import translation

from froide_evidencecollection.tasks import import_data_wikidata


class Command(BaseCommand):
    help = "Import data from Wikidata via API"

    def handle(self, *args, **options):
        translation.activate(settings.LANGUAGE_CODE)
        import_data_wikidata()
