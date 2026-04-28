from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import translation

from froide_evidencecollection.json_importer import JSONImporter


class Command(BaseCommand):
    help = "Import social-media posts as Evidence from a partner-provided JSON dump"

    def add_arguments(self, parser):
        parser.add_argument("json_file", help="Path to the JSON file")
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Print what would be imported without making changes",
        )

    def handle(self, *args, **options):
        translation.activate(settings.LANGUAGE_CODE)

        importer = JSONImporter(
            json_path=options["json_file"],
            dry_run=options["dry_run"],
        )
        stats = importer.run()

        self.stdout.write("Import stats:")
        for key, value in stats.items():
            self.stdout.write(f"  {key}: {value}")
