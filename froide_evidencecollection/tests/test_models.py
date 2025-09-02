import pytest

from froide_evidencecollection.models import SyncableModel

from .factories import syncable_model_factories


@pytest.mark.django_db
class TestSyncableModel:
    @pytest.mark.parametrize("factory", syncable_model_factories)
    def test_syncable_model_creation(self, factory):
        instance = factory()

        assert isinstance(instance, SyncableModel)
        assert instance.sync_uuid is not None
        assert instance.synced_at is None
        assert instance.is_synced is False

    @pytest.mark.parametrize("factory", syncable_model_factories)
    def test_syncable_model_saving(self, factory):
        instance = factory()
        updated_at = instance.updated_at

        # Normal save does not change synced_at or is_synced.
        instance.save()
        assert instance.updated_at > updated_at
        assert instance.synced_at is None
        assert instance.is_synced is False

        # Save with sync=True updates synced_at and is_synced.
        updated_at = instance.updated_at
        instance.save(sync=True)
        assert instance.updated_at > updated_at
        assert instance.synced_at == instance.updated_at
        assert instance.is_synced is True

        # Another normal save does not change synced_at or is_synced.
        updated_at = instance.updated_at
        synced_at = instance.synced_at
        instance.save()
        assert instance.updated_at > updated_at
        assert instance.synced_at == synced_at
        assert instance.is_synced is True

    @pytest.mark.parametrize("factory", syncable_model_factories)
    def test_syncable_model_mark_synced(self, factory):
        instance = factory()
        updated_at = instance.updated_at

        instance.mark_synced()

        assert instance.synced_at is not None
        assert instance.updated_at == updated_at
        assert instance.is_synced is True

    @pytest.mark.parametrize("factory", syncable_model_factories)
    def test_syncable_model_update_without_sync(self, factory):
        instance = factory()
        sync_uuid = instance.sync_uuid
        updated_at = instance.updated_at

        instance.name = "Updated Name"
        instance.save()

        assert instance.sync_uuid == sync_uuid
        assert instance.updated_at > updated_at
        assert instance.synced_at is None
        assert instance.is_synced is False

    @pytest.mark.parametrize("factory", syncable_model_factories)
    def test_syncable_model_update_with_sync(self, factory):
        instance = factory()
        sync_uuid = instance.sync_uuid
        updated_at = instance.updated_at

        instance.name = "Updated Name"
        instance.save(sync=True)

        assert instance.sync_uuid == sync_uuid
        assert instance.updated_at > updated_at
        assert instance.synced_at == instance.updated_at
        assert instance.is_synced is True
