from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import translation

from froide_evidencecollection.tasks import import_data_abgeordnetenwatch


class Command(BaseCommand):
    help = "Import data from abgeordnetenwatch.de via API"

    def add_arguments(self, parser):
        parser.add_argument("--only-setup", action="store_true")

    def handle(self, *args, **options):
        translation.activate(settings.LANGUAGE_CODE)
        import_data_abgeordnetenwatch(only_setup=options["only_setup"])
