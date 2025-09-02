from unittest import mock

import pytest

from froide_evidencecollection.models import ImportExportRun
from froide_evidencecollection.tasks import (
    export_evidence_nocodb,
    import_evidence_nocodb,
)


class TestImportTask:
    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.tasks.NocoDBImporter")
    def test_import_evidence_nocodb_success(self, MockImporter):
        """
        Test that the import task successfully completes and logs stats when no exceptions occur.
        """
        mock_importer_instance = MockImporter.return_value
        mock_importer_instance.log_stats.return_value = {"imported": 5}

        import_evidence_nocodb()

        mock_importer_instance.run.assert_called_once()
        mock_importer_instance.log_stats.assert_called_once()

        run = ImportExportRun.objects.last()
        assert run.success is True
        assert run.changes == {"imported": 5}
        assert run.notes == ""

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.tasks.NocoDBImporter")
    def test_import_evidence_nocodb_exception(self, MockImporter):
        """
        Test that the import task handles exceptions properly and marks the run as failed.
        """
        mock_importer_instance = MockImporter.return_value
        mock_importer_instance.run.side_effect = Exception("Import failed")

        import_evidence_nocodb()

        mock_importer_instance.run.assert_called_once()
        mock_importer_instance.log_stats.assert_not_called()

        run = ImportExportRun.objects.last()
        assert run.success is False
        assert run.changes == {}
        assert "Import failed" in run.notes


class TestExportTask:
    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.tasks.NocoDBExporter")
    def test_export_evidence_nocodb_success(self, MockExporter):
        """
        Test that the export task successfully completes and logs stats when no exceptions occur.
        """
        mock_exporter_instance = MockExporter.return_value
        mock_exporter_instance.log_stats.return_value = {"exported": 10}

        export_evidence_nocodb()

        mock_exporter_instance.run.assert_called_once()
        mock_exporter_instance.log_stats.assert_called_once()

        run = ImportExportRun.objects.last()
        assert run.success is True
        assert run.changes == {"exported": 10}
        assert run.notes == ""

    @pytest.mark.django_db
    @mock.patch("froide_evidencecollection.tasks.NocoDBExporter")
    def test_export_evidence_nocodb_exception(self, MockExporter):
        """
        Test that the export task handles exceptions properly and marks the run as failed.
        """
        mock_exporter_instance = MockExporter.return_value
        mock_exporter_instance.run.side_effect = Exception("Export failed")

        export_evidence_nocodb()

        mock_exporter_instance.run.assert_called_once()
        mock_exporter_instance.log_stats.assert_not_called()

        run = ImportExportRun.objects.last()
        assert run.success is False
        assert run.changes == {}
        assert "Export failed" in run.notes
