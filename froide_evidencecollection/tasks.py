import logging

from froide.celery import app as celery_app
from froide_evidencecollection.abgeordnetenwatch import AbgeordnetenwatchImporter
from froide_evidencecollection.documents import EvidenceDocument
from froide_evidencecollection.json_importer import JSONImporter
from froide_evidencecollection.models import ImportExportRun
from froide_evidencecollection.wikidata import WikidataImporter

logger = logging.getLogger(__name__)


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


@celery_app.task(name="froide_evidencecollection.import_evidence_json")
def import_evidence_json(json_path, dry_run=False):
    importer = JSONImporter(json_path=json_path, dry_run=dry_run)

    if dry_run:
        importer.run()
        logger.info("Dry-run import from JSON finished")
        return

    run = ImportExportRun.objects.create(
        operation=ImportExportRun.IMPORT,
        source=ImportExportRun.JSON,
        target=ImportExportRun.FROIDE_EVIDENCECOLLECTION,
    )

    success = False

    try:
        importer.run()
    except Exception as e:
        logger.exception(f"Failed to import data from JSON: {e}")
        run.complete(success, notes=str(e))
    else:
        success = True
        run.complete(success, changes=importer.log_stats())
    finally:
        logger.info(f"Import from JSON finished with success={success}")


@celery_app.task(name="froide_evidencecollection.reindex_evidence")
def reindex_evidence(pks=None):
    """Re-write the evidence search documents, all of them when `pks` is None.

    Queued when a redaction rule changes: `search_text` is redacted, so the
    already-written index would otherwise keep answering queries for a term the
    new rule masks. One bulk request, not one per evidence.
    """
    doc = EvidenceDocument()
    queryset = doc.get_queryset()
    if pks is not None:
        queryset = queryset.filter(pk__in=pks)

    try:
        doc.update(queryset)
    except Exception as e:
        logger.exception(f"Failed to re-index evidence: {e}")


@celery_app.task(name="froide_evidencecollection.import_data_wikidata")
def import_data_wikidata():
    importer = WikidataImporter()
    run = ImportExportRun.objects.create(
        operation=ImportExportRun.IMPORT,
        source=ImportExportRun.WIKIDATA,
        target=ImportExportRun.FROIDE_EVIDENCECOLLECTION,
    )

    success = False

    try:
        importer.run()
    except Exception as e:
        logger.exception(f"Failed to import data from Wikidata: {e}")
        run.complete(success, notes=str(e))
    else:
        success = True
        run.complete(success, changes=importer.log_stats())
    finally:
        logger.info(f"Import from Wikidata finished with success={success}")
