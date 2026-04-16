from django.conf import settings
from django.core.management.base import BaseCommand
from django.utils import translation

from froide_evidencecollection.sqlite_importer import SQLiteImporter


class Command(BaseCommand):
    help = "Import data from a partner-provided SQLite database"

    def add_arguments(self, parser):
        parser.add_argument("database", help="Path to the SQLite database file")
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Print what would be imported without making changes",
        )

    def handle(self, *args, **options):
        translation.activate(settings.LANGUAGE_CODE)

        importer = SQLiteImporter(
            db_path=options["database"],
            dry_run=options["dry_run"],
        )
        stats = importer.run()

        for table, table_stats in stats.items():
            self.stdout.write(f"\n{table}:")
            for key, value in table_stats.items():
                self.stdout.write(f"  {key}: {value}")
