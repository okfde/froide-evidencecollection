import logging

from froide.celery import app as celery_app
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
    finally:
        run.complete(True, changes=importer.log_stats())
