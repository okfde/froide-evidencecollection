import logging

from froide.celery import app as celery_app
from froide_evidencecollection.importer import NocoDBImporter

logger = logging.getLogger(__name__)


@celery_app.task(name="froide_evidencecollection.import_evidence_nocodb")
def import_evidence_nocodb():
    importer = NocoDBImporter()
    try:
        importer.run()
    except Exception as e:
        logger.error(f"Failed to import data from NocoDB: {e}")
        raise
