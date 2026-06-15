import pytest

from froide_evidencecollection.models import (
    Category,
    Evidence,
    EvidenceMention,
    Parliament,
    PostImage,
    PostVideo,
    SocialMediaAccount,
    SocialMediaPost,
    SyncableModel,
    VideoExcerpt,
)

from .factories import OrganizationFactory, RoleFactory, syncable_model_factories


def _make_post(**overrides):
    # Each call gets its own account (unique platform_user_id) so multiple
    # posts in one test don't collide on the (platform, platform_user_id)
    # constraint.
    ppid = str(overrides.get("platform_post_id", "1"))
    account = SocialMediaAccount.objects.create(
        platform=SocialMediaAccount.Platform.TELEGRAM,
        username=f"example_{ppid}",
        platform_user_id=ppid,
    )
    fields = {
        "account": account,
        "platform_post_id": ppid,
        "url": "https://t.me/example/1",
        "text": "post body",
        "raw": {},
    }
    fields.update(overrides)
    return SocialMediaPost.objects.create(**fields)


@pytest.mark.django_db
class TestPostMediaTextSegments:
    def test_media_text_flows_into_evidence_segments(self):
        post = _make_post()
        # An image's content_text surfaces as an `extracted_text` segment...
        PostImage.objects.create(
            post=post,
            source_path="./img/a.png",
            description="a protest sign",
            content_text="STOP",
        )
        # ...and a video excerpt's text as a `transcription` segment.
        video = PostVideo.objects.create(post=post, source_path="./video/b.mp4")
        VideoExcerpt.objects.create(video=video, order=0, text="spoken words")
        evidence = Evidence.objects.create(social_media_post=post, external_id=1)

        by_kind = {seg.kind: seg.text for seg in evidence.text_segments}
        assert by_kind["body"] == "post body"
        assert by_kind["description"] == "a protest sign"
        assert by_kind["extracted_text"] == "STOP"
        assert by_kind["transcription"] == "spoken words"

    def test_transcription_is_display_only(self):
        # A video excerpt's transcript is shown on the detail page but is kept
        # out of the search index and the topic input — the curated mention
        # citations carry that text instead.
        post = _make_post(text="")
        video = PostVideo.objects.create(post=post, source_path="./video/b.mp4")
        VideoExcerpt.objects.create(video=video, order=0, text="spoken words")
        evidence = Evidence.objects.create(social_media_post=post, external_id=1)

        seg = next(s for s in evidence.text_segments if s.kind == "transcription")
        assert seg.for_search is False
        assert seg.for_topics is False
        assert "spoken words" not in evidence.search_text
        assert "spoken words" not in evidence.topic_text

    def test_mention_citation_feeds_search_and_topics(self):
        # `EvidenceMention.citation` is a searched/topic-modelled segment, shown
        # after the source text and labelled with its category and footnote.
        post = _make_post(text="")
        evidence = Evidence.objects.create(social_media_post=post, external_id=1)
        category = Category.objects.create(name="Disinformation")
        EvidenceMention.objects.create(
            evidence=evidence,
            category=category,
            footnote="fn3",
            citation="the relevant quote",
        )

        seg = next(s for s in evidence.text_segments if s.kind == "citation")
        assert seg.text == "the relevant quote"
        assert seg.for_search is True
        assert seg.for_topics is True
        assert seg.attribution == "Disinformation · fn3"
        assert "the relevant quote" in evidence.search_text
        assert "the relevant quote" in evidence.topic_text

    def test_topic_text_orders_media_by_signal(self):
        post = _make_post(text="")
        # extracted_text leads (priority 0); transcription is display-only so it
        # drops out of the topic input; descriptions (priority 2) trail in
        # source order.
        PostImage.objects.create(
            post=post,
            source_path="./img/a.png",
            description="image scene",
            content_text="image words",
        )
        video = PostVideo.objects.create(
            post=post,
            source_path="./video/b.mp4",
            description="video scene",
        )
        VideoExcerpt.objects.create(video=video, order=0, text="video speech")
        evidence = Evidence.objects.create(social_media_post=post, external_id=1)

        assert evidence.topic_text == ("image words\n\nimage scene\n\nvideo scene")

    def test_redistributed_media_is_prefixed_and_attributed(self):
        inner = _make_post(platform_post_id="inner", url="https://t.me/x/2")
        inner_video = PostVideo.objects.create(
            post=inner, source_path="./video/inner.mp4"
        )
        VideoExcerpt.objects.create(video=inner_video, order=0, text="quoted speech")
        outer = _make_post(platform_post_id="outer", url="https://t.me/x/3")
        outer.redistributes = inner
        outer.save(update_fields=["redistributes"])
        evidence = Evidence.objects.create(social_media_post=outer, external_id=1)

        seg = next(
            s for s in evidence.text_segments if s.kind == "redistributed:transcription"
        )
        assert seg.text == "quoted speech"
        assert seg.attribution == str(inner.account)


@pytest.mark.django_db
class TestParliamentFindMatchingFraction:
    def test_matches_by_organization_name(self):
        org = OrganizationFactory(organization_name="Landtagsfraktion Sachsen")
        # `name` is the AW `label_external_long`, e.g. the full parliament name.
        assert Parliament(name="Sachsen").find_matching_fraction() == org

    def test_matches_renamed_org_via_alias(self):
        # After the align step the parliament wording survives only in the
        # alias; the new name no longer contains "Abgeordnetenhaus Berlin".
        org = OrganizationFactory(
            organization_name="Landtagsfraktion Berlin",
            also_known_as=["AfD-Fraktion im Abgeordnetenhaus Berlin"],
        )
        parliament = Parliament(name="Abgeordnetenhaus Berlin")
        assert parliament.find_matching_fraction() == org

    def test_raises_when_no_match(self):
        OrganizationFactory(organization_name="Landtagsfraktion Sachsen")
        with pytest.raises(ValueError, match="No matching fraction"):
            Parliament(name="Bayern").find_matching_fraction()


@pytest.mark.django_db
class TestSyncableModel:
    @pytest.mark.parametrize("factory", syncable_model_factories)
    def test_syncable_model_creation(self, factory):
        instance = factory()

        assert isinstance(instance, SyncableModel)
        assert instance.sync_uuid is not None
        assert instance.synced_at is None
        assert instance.is_synced is False
        assert instance.last_synced_state == {}

    @pytest.mark.parametrize("factory", syncable_model_factories)
    def test_syncable_model_saving(self, factory):
        instance = factory()
        updated_at = instance.updated_at

        # Normal save does not change synced_at or is_synced.
        instance.save()
        assert instance.updated_at > updated_at
        assert instance.synced_at is None
        assert instance.is_synced is False
        assert instance.last_synced_state == {}

        # Save with sync=True updates synced_at, is_synced, and last_synced_state.
        updated_at = instance.updated_at
        instance.save(sync=True)
        assert instance.updated_at > updated_at
        assert instance.synced_at == instance.updated_at
        assert instance.is_synced is True
        assert instance.last_synced_state != {}

        # Another normal save does not change synced_at or last_synced_state.
        # The instance is out of sync now.
        updated_at = instance.updated_at
        synced_at = instance.synced_at
        last_synced_state = instance.last_synced_state
        instance.save()
        assert instance.updated_at > updated_at
        assert instance.synced_at == synced_at
        assert instance.is_synced is False
        assert instance.last_synced_state == last_synced_state

    @pytest.mark.parametrize("factory", syncable_model_factories)
    def test_syncable_model_mark_synced(self, factory):
        instance = factory()
        updated_at = instance.updated_at

        instance.mark_synced()

        assert instance.synced_at is not None
        assert instance.updated_at == updated_at
        assert instance.is_synced is True

    @pytest.mark.parametrize("factory", [RoleFactory])
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
        assert instance.last_synced_state == {}

    @pytest.mark.parametrize("factory", [RoleFactory])
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
        assert instance.last_synced_state["name"] == "Updated Name"
