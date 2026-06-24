import pytest

from froide_evidencecollection.models import (
    Actor,
    Category,
    Evidence,
    EvidenceMention,
    Parliament,
    RedactionRule,
    SocialMediaAccount,
    SocialMediaPost,
    SyncableModel,
)

from .factories import (
    GeoRegionFactory,
    OrganizationFactory,
    RoleFactory,
    syncable_model_factories,
)


class TestVerbandLabel:
    @pytest.mark.django_db
    def test_country_region_reads_bund(self):
        org = OrganizationFactory(
            verband=GeoRegionFactory(name="Deutschland", kind="country")
        )
        assert org.verband_label == "Bund"

    @pytest.mark.django_db
    def test_state_region_reads_bare_name(self):
        org = OrganizationFactory(verband=GeoRegionFactory(name="Bayern", kind="state"))
        assert org.verband_label == "Bayern"

    @pytest.mark.django_db
    def test_no_verband_reads_empty(self):
        assert OrganizationFactory(verband=None).verband_label == ""


def _actor():
    """A throwaway actor for mentions whose originator is irrelevant to the test."""
    return Actor.objects.create(organization=OrganizationFactory())


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
    }
    fields.update(overrides)
    return SocialMediaPost.objects.create(**fields)


@pytest.mark.django_db
class TestPostTextSegments:
    def test_non_video_post_uses_its_whole_text(self):
        # A non-video post contributes its title, body, description and the
        # image's alt-text description — all searched and topic-modelled.
        post = _make_post(
            title="the title",
            description="a description",
            image_description="a protest sign",
        )
        evidence = Evidence.objects.create(social_media_post=post)

        by_kind = {}
        for seg in evidence.text_segments:
            by_kind.setdefault(seg.kind, []).append(seg.text)
        assert by_kind["title"] == ["the title"]
        assert by_kind["body"] == ["post body"]
        assert set(by_kind["description"]) == {"a description", "a protest sign"}
        assert all(s.for_search and s.for_topics for s in evidence.text_segments)

    def test_video_post_uses_mention_raw_transcript(self):
        # For a video post the per-mention raw_transcript is the searched /
        # topic text; the full transcription is the fallback and is not used
        # when a mention carries a transcript.
        post = _make_post(
            text="caption",
            video_source_path="./video/b.mp4",
            transcription="full transcript not used",
        )
        evidence = Evidence.objects.create(social_media_post=post)
        category = Category.objects.create(name="Disinformation")
        EvidenceMention.objects.create(
            evidence=evidence,
            category=category,
            footnote="fn3",
            raw_transcript="the spoken excerpt",
            originator=_actor(),
        )

        seg = next(s for s in evidence.text_segments if s.kind == "transcription")
        assert seg.text == "the spoken excerpt"
        assert seg.for_search is True
        assert seg.for_topics is True
        assert seg.attribution == "Disinformation · fn3"
        assert "the spoken excerpt" in evidence.search_text
        assert "the spoken excerpt" in evidence.topic_text
        # The caption rides along, but the full transcription does not.
        assert "caption" in evidence.search_text
        assert "full transcript not used" not in evidence.search_text

    def test_video_post_falls_back_to_full_transcription(self):
        # With no per-mention transcript, the post's full transcription is used
        # for display/search/topics.
        post = _make_post(
            text="",
            video_source_path="./video/b.mp4",
            transcription="the whole transcript",
        )
        evidence = Evidence.objects.create(social_media_post=post)

        seg = next(s for s in evidence.text_segments if s.kind == "transcription")
        assert seg.text == "the whole transcript"
        assert seg.for_search is True
        assert "the whole transcript" in evidence.topic_text

    def test_video_post_description_is_display_only(self):
        # A video post's (often promotional) description rides along as a
        # display-only segment — shown in the detail view but kept out of
        # search/topics, where the transcript carries the content.
        post = _make_post(
            text="caption",
            description="promo blurb",
            video_source_path="./video/b.mp4",
            transcription="speech",
        )
        evidence = Evidence.objects.create(social_media_post=post)

        seg = next(s for s in evidence.text_segments if s.kind == "video_description")
        assert seg.text == "promo blurb"
        assert seg.for_search is False
        assert seg.for_topics is False
        assert "promo blurb" not in evidence.search_text
        assert "promo blurb" not in evidence.topic_text
        assert "caption" in evidence.search_text

    def test_citation_is_not_wired_into_segments(self):
        # `EvidenceMention.citation` is kept on the model but unwired from
        # display/search/topics.
        post = _make_post(text="body")
        evidence = Evidence.objects.create(social_media_post=post)
        category = Category.objects.create(name="Disinformation")
        EvidenceMention.objects.create(
            evidence=evidence,
            category=category,
            footnote="fn3",
            citation="the curated quote",
            originator=_actor(),
        )

        assert "the curated quote" not in evidence.search_text
        assert "the curated quote" not in evidence.topic_text

    def test_redistributed_text_is_prefixed_and_attributed(self):
        inner = _make_post(
            platform_post_id="inner", url="https://t.me/x/2", text="quoted text"
        )
        outer = _make_post(platform_post_id="outer", url="https://t.me/x/3")
        outer.redistributes = inner
        outer.save(update_fields=["redistributes"])
        evidence = Evidence.objects.create(social_media_post=outer)

        seg = next(s for s in evidence.text_segments if s.kind == "redistributed:body")
        assert seg.text == "quoted text"
        assert seg.attribution == str(inner.account)


@pytest.mark.django_db
class TestGroupedTextSegments:
    def test_own_components_merged_into_one_post_block(self):
        # Title and body collapse into a single "Post text" block; the
        # description is left for the Visual material section.
        post = _make_post(title="the title", description="a description")
        evidence = Evidence.objects.create(social_media_post=post)

        groups = evidence.grouped_text_segments
        assert [g.kind for g in groups] == ["post"]
        post_group = groups[0]
        assert post_group.heading == "Post text"
        assert [s.base_kind for s in post_group.segments] == ["title", "body"]

    def test_video_post_block_is_headed_video_description(self):
        post = _make_post(
            text="caption", description="promo", video_source_path="./video/b.mp4"
        )
        evidence = Evidence.objects.create(social_media_post=post)

        post_group = next(g for g in evidence.grouped_text_segments if g.kind == "post")
        assert post_group.heading == "Video description"
        # The display-only description rides along inside the same block.
        assert "video_description" in [s.base_kind for s in post_group.segments]

    def test_transcript_is_a_standalone_block(self):
        post = _make_post(
            text="caption",
            video_source_path="./video/b.mp4",
            transcription="the whole transcript",
        )
        evidence = Evidence.objects.create(social_media_post=post)

        kinds = [g.kind for g in evidence.grouped_text_segments]
        assert "standalone" in kinds
        standalone = next(
            g for g in evidence.grouped_text_segments if g.kind == "standalone"
        )
        assert standalone.segments[0].base_kind == "transcription"

    def test_repost_is_its_own_attributed_block(self):
        inner = _make_post(
            platform_post_id="inner",
            url="https://t.me/x/2",
            title="inner title",
            text="quoted text",
        )
        outer = _make_post(
            platform_post_id="outer", url="https://t.me/x/3", text="my take"
        )
        outer.redistributes = inner
        outer.save(update_fields=["redistributes"])
        evidence = Evidence.objects.create(social_media_post=outer)

        groups = evidence.grouped_text_segments
        assert [g.kind for g in groups] == ["post"]
        # The repost is nested inside the post that shares it, not a sibling.
        post_group = groups[0]
        assert [s.base_kind for s in post_group.segments] == ["body"]
        assert len(post_group.reposts) == 1
        repost = post_group.reposts[0]
        # The reposted source's components are grouped together, and the block
        # keeps the reference to the account it was lifted from.
        assert repost.attribution == str(inner.account)
        assert [s.base_kind for s in repost.segments] == ["title", "body"]


@pytest.mark.django_db
class TestRedaction:
    def test_global_rule_masks_everywhere(self):
        post = _make_post(text="the Badword appears here")
        evidence = Evidence.objects.create(social_media_post=post)
        RedactionRule.objects.create(pattern="Badword", placeholder="[X]")

        assert "Badword" not in evidence.search_text
        assert "[X]" in evidence.search_text
        assert "Badword" not in evidence.topic_text
        # The raw imported field is untouched; only the assembled text is masked.
        post.refresh_from_db()
        assert "Badword" in post.text

    def test_disabled_rule_does_not_mask(self):
        post = _make_post(text="the Badword appears here")
        evidence = Evidence.objects.create(social_media_post=post)
        RedactionRule.objects.create(
            pattern="Badword", placeholder="[X]", enabled=False
        )

        assert "Badword" in evidence.search_text

    def test_scoped_rule_only_masks_its_posts(self):
        scoped = _make_post(platform_post_id="a", text="secret name here")
        other = _make_post(platform_post_id="b", text="secret name here")
        ev_scoped = Evidence.objects.create(social_media_post=scoped)
        ev_other = Evidence.objects.create(social_media_post=other)
        rule = RedactionRule.objects.create(pattern="secret", placeholder="[Name]")
        rule.posts.add(scoped)

        assert "secret" not in ev_scoped.search_text
        assert "[Name]" in ev_scoped.search_text
        # The other post is untouched (the rule is scoped, not global).
        assert "secret" in ev_other.search_text


@pytest.mark.django_db
class TestEvidenceSlug:
    def test_social_media_post_slug_is_frozen_contract(self):
        # Pins the public slug for a known (platform, post_id) seed. Partners
        # derive the same value to link into our data, so this must never change:
        # if it does, the seed format, hash, encoding or length has drifted.
        post = _make_post(platform_post_id="12345")
        assert post.compute_slug() == "u2iqeggxhv"

    def test_evidence_delegates_slug_to_source(self):
        post = _make_post(platform_post_id="12345")
        evidence = Evidence(social_media_post=post)
        assert evidence.compute_slug() == post.compute_slug()

    def test_save_sets_slug_from_source_once(self):
        post = _make_post(platform_post_id="12345")
        evidence = Evidence.objects.create(social_media_post=post)
        assert evidence.slug == "u2iqeggxhv"

        # Never recomputed: the slug is frozen after first save even if the
        # source's slug inputs were to change.
        evidence.save()
        assert evidence.slug == "u2iqeggxhv"


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
