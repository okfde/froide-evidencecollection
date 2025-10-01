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

    try:
        importer.run()
    except Exception as e:
        logger.exception(f"Failed to import data from NocoDB: {e}")
        run.complete(False, notes=str(e))
    else:
        run.complete(True, changes=importer.log_stats())


@celery_app.task(name="froide_evidencecollection.export_evidence_nocodb")
def export_evidence_nocodb():
    exporter = NocoDBExporter()
    run = ImportExportRun.objects.create(
        operation=ImportExportRun.EXPORT,
        source=ImportExportRun.FROIDE_EVIDENCECOLLECTION,
        target=ImportExportRun.NOCODB,
    )

    try:
        exporter.run()
    except Exception as e:
        logger.exception(f"Failed to export data to NocoDB: {e}")
        run.complete(False, notes=str(e))
    else:
        run.complete(True, changes=exporter.log_stats())


@celery_app.task(name="froide_evidencecollection.import_data_abgeordnetenwatch")
def import_data_abgeordnetenwatch():
    importer = AbgeordnetenwatchImporter()
    run = ImportExportRun.objects.create(
        operation=ImportExportRun.IMPORT,
        source=ImportExportRun.ABGEORDNETENWATCH,
        target=ImportExportRun.FROIDE_EVIDENCECOLLECTION,
    )

    try:
        importer.run()
    except Exception as e:
        logger.exception(f"Failed to import data from abgeordnetenwatch.de: {e}")
        run.complete(False, notes=str(e))
    else:
        run.complete(True, changes=importer.log_stats())
