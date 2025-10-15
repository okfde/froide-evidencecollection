import logging

from froide.celery import app as celery_app
from froide_evidencecollection.abgeordnetenwatch import AbgeordnetenwatchImporter
from froide_evidencecollection.exporter import NocoDBExporter
from froide_evidencecollection.importer import NocoDBImporter
from froide_evidencecollection.models import ImportExportRun

logger = logging.getLogger(__name__)


@celery_app.task(name="froide_evidencecollection.import_evidence_nocodb")
def import_evidence_nocodb(full=False):
    importer = NocoDBImporter(full_import=full)
    run = ImportExportRun.objects.create(
        operation=ImportExportRun.IMPORT,
        source=ImportExportRun.NOCODB,
        target=ImportExportRun.FROIDE_EVIDENCECOLLECTION,
    )

    success = False

    try:
        importer.run()
    except Exception as e:
        logger.exception(f"Failed to import data from NocoDB: {e}")
        run.complete(success, notes=str(e))
    else:
        success = True
        run.complete(success, changes=importer.log_stats())
    finally:
        logger.info(f"Import from NocoDB finished with success={success}")


@celery_app.task(name="froide_evidencecollection.export_evidence_nocodb")
def export_evidence_nocodb():
    exporter = NocoDBExporter()
    run = ImportExportRun.objects.create(
        operation=ImportExportRun.EXPORT,
        source=ImportExportRun.FROIDE_EVIDENCECOLLECTION,
        target=ImportExportRun.NOCODB,
    )

    success = False
    notes = ""

    try:
        exporter.run()
        success = True
    except Exception as e:
        logger.exception(f"Failed to export data to NocoDB: {e}")
        notes = str(e)
    finally:
        # Make sure we log changes up to this point, even on failure.
        stats = exporter.log_stats()
        run.complete(success, changes=stats, notes=notes)
        logger.info(f"Export to NocoDB finished with success={success}")


@celery_app.task(name="froide_evidencecollection.import_data_abgeordnetenwatch")
def import_data_abgeordnetenwatch(only_setup=False):
    importer = AbgeordnetenwatchImporter(only_setup=only_setup)
    run = ImportExportRun.objects.create(
        operation=ImportExportRun.IMPORT,
        source=ImportExportRun.ABGEORDNETENWATCH,
        target=ImportExportRun.FROIDE_EVIDENCECOLLECTION,
    )

    success = False

    try:
        importer.run()
    except Exception as e:
        logger.exception(f"Failed to import data from abgeordnetenwatch.de: {e}")
        run.complete(success, notes=str(e))
    else:
        success = True
        run.complete(success, changes=importer.log_stats())
    finally:
        logger.info(f"Import from abgeordnetenwatch.de finished with success={success}")
