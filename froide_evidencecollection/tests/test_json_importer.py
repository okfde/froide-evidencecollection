import json
from datetime import timedelta

import pytest

from froide_evidencecollection.json_importer import JSONImporter
from froide_evidencecollection.models import (
    Actor,
    Chapter,
    Evidence,
    EvidenceActorRelation,
    EvidenceMention,
    PostImage,
    PostScreenshot,
    PostVideo,
    SocialMediaAccount,
    SocialMediaPost,
    VideoExcerpt,
)

from .factories import OrganizationFactory, PersonFactory


def _make_account(**overrides):
    base = {
        "username": "example_user",
        "platform_user_id": "123",
        "display_name": "Example",
        "description": "An example account",
        "url": "https://t.me/example_user",
        "follower_count": 1000,
        "is_verified": False,
        "is_blue_verified": False,
    }
    base.update(overrides)
    return base


def _make_post(**overrides):
    base = {
        "url": "https://t.me/example/1",
        "platform_post_id": "1",
        "created_at": "2024-01-01T10:00:00+00:00",
        "edited_at": None,
        "collected_at": "2024-01-02T12:00:00+00:00",
        "text": "Hello",
        "title": "",
        "view_count": 100,
        "like_count": 5,
        "comment_count": 1,
        "share_count": 0,
        "reactions": None,
        "references": [],
        "account": _make_account(),
    }
    base.update(overrides)
    return base


def _write_dump(tmp_path, dump, name="import.json"):
    path = tmp_path / name
    path.write_text(json.dumps(dump))
    return str(path)


@pytest.fixture
def person(db):
    # `external_id` is required because `Actor.save()` copies it from its target,
    # and `Actor.external_id` is NOT NULL.
    return PersonFactory(first_name="Max", last_name="Mustermann", external_id=1)


class TestJSONImporter:
    @pytest.mark.django_db
    def test_import_creates_account_post_evidence_and_actor(self, person, tmp_path):
        path = _write_dump(
            tmp_path,
            {
                person.name_hash: {
                    "label": "Max Mustermann",
                    "Name": "Max Mustermann",
                    "social_media": {"telegram": [_make_post()]},
                }
            },
        )

        importer = JSONImporter(path)
        importer.run()

        account = SocialMediaAccount.objects.get()
        assert account.platform == SocialMediaAccount.Platform.TELEGRAM
        assert account.username == "example_user"
        assert account.actor.person == person
        # Full account profile is persisted on creation.
        assert account.platform_user_id == "123"
        assert account.display_name == "Example"
        assert account.description == "An example account"
        assert account.url == "https://t.me/example_user"
        assert account.follower_count == 1000
        assert account.is_verified is False

        sm_post = SocialMediaPost.objects.get()
        assert sm_post.account == account
        assert sm_post.platform_post_id == "1"
        assert sm_post.text == "Hello"
        assert sm_post.url == "https://t.me/example/1"

        evidence = Evidence.objects.get()
        assert evidence.social_media_post == sm_post
        assert evidence.external_id == 1
        assert str(evidence.documentation_date) == "2024-01-02"

        assert Actor.objects.count() == 1

        stats = importer.log_stats()
        assert set(stats.keys()) == {
            "Actor",
            "SocialMediaAccount",
            "SocialMediaPost",
            "Evidence",
        }
        for model_stats in stats.values():
            assert len(model_stats["created"]) == 1
            assert model_stats["updated"] == []
            assert model_stats["skipped"] == []
            assert model_stats["deleted"] == []

    @pytest.mark.django_db
    def test_import_creates_post_media(self, person, tmp_path, settings):
        settings.MEDIA_ROOT = str(tmp_path / "media")
        (tmp_path / "img").mkdir()
        (tmp_path / "video").mkdir()
        (tmp_path / "shot").mkdir()
        (tmp_path / "srt").mkdir()
        (tmp_path / "img" / "y.png").write_bytes(b"fake-image")
        (tmp_path / "video" / "x.mp4").write_bytes(b"fake-video")
        (tmp_path / "shot" / "s.png").write_bytes(b"fake-screenshot")
        (tmp_path / "srt" / "x.srt").write_bytes(b"fake-srt")

        post = _make_post(
            image_file="./img/y.png",
            # `image_alt_text` accompanies `image_file`: alt text plus a
            # German JA/NEIN flag for whether the image relates to the text.
            image_alt_text={"alt_text": "A protest sign", "text_bezug_zum_bild": "JA"},
            video_file="./video/x.mp4",
            # `srt_file` is the video's transcript sidecar (single path string).
            srt_file="./srt/x.srt",
            # `screenshot_file` is a single path string.
            screenshot_file="./shot/s.png",
            # `report_data` is currently required by the importer (temporary).
            # `video_timestamp` carries the video's excerpts.
            report_data={
                "footnote_url": ["https://t.me/example/1"],
                "video_timestamp": [
                    {"start": "00:00:01", "end": "00:00:05", "excerpt": "spoken words"},
                ],
            },
        )
        path = _write_dump(
            tmp_path,
            {
                person.name_hash: {
                    "label": "Max Mustermann",
                    "social_media": {"telegram": [post]},
                }
            },
        )

        JSONImporter(path).run()

        sm_post = SocialMediaPost.objects.get()
        image = sm_post.images.get()
        video = sm_post.videos.get()
        screenshot = sm_post.screenshots.get()
        # The video_timestamp entry becomes the video's (order 0) excerpt, with
        # "HH:MM:SS" parsed into start/end durations.
        excerpt = video.excerpts.get()
        assert excerpt.text == "spoken words"
        assert excerpt.start == timedelta(seconds=1)
        assert excerpt.end == timedelta(seconds=5)
        assert image.content_text == ""
        # Alt-text fields land on the image; JA -> True.
        assert image.description == "A protest sign"
        assert image.is_related_to_text is True
        assert video.source_path == "./video/x.mp4"
        assert screenshot.source_path == "./shot/s.png"
        # File bytes are copied into the FileField.
        assert video.file.read() == b"fake-video"
        assert image.file.read() == b"fake-image"
        assert screenshot.file.read() == b"fake-screenshot"
        # The SRT sidecar lands in the video's transcript_file.
        assert video.transcript_file.read() == b"fake-srt"

        # Idempotent: a second run neither duplicates rows nor reports changes.
        importer = JSONImporter(path)
        importer.run()
        assert PostImage.objects.count() == 1
        assert PostVideo.objects.count() == 1
        assert VideoExcerpt.objects.count() == 1
        assert PostScreenshot.objects.count() == 1
        stats = importer.log_stats()
        assert "PostImage" not in stats
        assert "PostVideo" not in stats
        assert "VideoExcerpt" not in stats
        assert "PostScreenshot" not in stats

    @pytest.mark.django_db
    def test_reimport_backfills_missing_media_file(self, person, tmp_path, settings):
        settings.MEDIA_ROOT = str(tmp_path / "media")
        (tmp_path / "video").mkdir()
        media_file = tmp_path / "video" / "x.mp4"

        post = _make_post(
            video_file="./video/x.mp4",
            report_data={"footnote_url": ["https://t.me/example/1"]},
        )
        path = _write_dump(
            tmp_path,
            {
                person.name_hash: {
                    "label": "Max Mustermann",
                    "social_media": {"telegram": [post]},
                }
            },
        )

        # First run: the file isn't on disk yet, so the row is created without
        # one (the historical situation that left 0 files attached).
        JSONImporter(path).run()
        m = PostVideo.objects.get()
        assert not m.file
        assert m.source_path == "./video/x.mp4"

        # The file becomes available; a re-import backfills it onto the
        # existing row rather than skipping it via the update branch.
        media_file.write_bytes(b"fake-video")
        JSONImporter(path).run()
        assert PostVideo.objects.count() == 1
        assert PostVideo.objects.get().file.read() == b"fake-video"

    @pytest.mark.django_db
    def test_video_timestamps_become_ordered_excerpts(self, person, tmp_path):
        post = _make_post(
            video_file="./video/x.mp4",
            report_data={
                "footnote_url": ["https://t.me/example/1"],
                "video_timestamp": [
                    {"start": "00:00:10", "end": "00:00:20", "excerpt": "first"},
                    # An entirely-empty entry is skipped, not stored as a row.
                    {"start": None, "end": None, "excerpt": None},
                    {"start": "01:02:03", "end": "01:02:30", "excerpt": "second"},
                ],
            },
        )
        path = _write_dump(
            tmp_path,
            {
                person.name_hash: {
                    "label": "Max Mustermann",
                    "social_media": {"telegram": [post]},
                }
            },
        )

        JSONImporter(path).run()

        video = PostVideo.objects.get()
        # The empty entry is dropped; the two real ones become ordered excerpts.
        excerpts = list(video.excerpts.all())
        assert [(e.order, e.text) for e in excerpts] == [(0, "first"), (1, "second")]
        assert excerpts[0].start == timedelta(seconds=10)
        assert excerpts[0].end == timedelta(seconds=20)
        assert excerpts[1].start == timedelta(hours=1, minutes=2, seconds=3)

        # Idempotent: re-running neither duplicates excerpts nor reports changes.
        importer = JSONImporter(path)
        importer.run()
        assert VideoExcerpt.objects.count() == 2
        assert "VideoExcerpt" not in importer.log_stats()

    @pytest.mark.django_db
    def test_reimport_same_data_produces_no_changes(self, person, tmp_path):
        path = _write_dump(
            tmp_path,
            {
                person.name_hash: {
                    "label": "Max Mustermann",
                    "social_media": {"telegram": [_make_post()]},
                }
            },
        )

        JSONImporter(path).run()
        importer = JSONImporter(path)
        importer.run()

        # Nothing changed → no tracked operations.
        assert importer.log_stats() == {}

        # And the data is still there exactly once.
        assert SocialMediaAccount.objects.count() == 1
        assert SocialMediaPost.objects.count() == 1
        assert Evidence.objects.count() == 1

    @pytest.mark.django_db
    def test_reimport_newer_collected_at_updates_account(self, person, tmp_path):
        path = _write_dump(
            tmp_path,
            {
                person.name_hash: {
                    "label": "Max Mustermann",
                    "social_media": {"telegram": [_make_post()]},
                }
            },
        )
        JSONImporter(path).run()

        updated_post = _make_post(
            collected_at="2024-03-01T12:00:00+00:00",
            account=_make_account(display_name="Renamed", follower_count=2000),
        )
        path2 = _write_dump(
            tmp_path,
            {
                person.name_hash: {
                    "label": "Max Mustermann",
                    "social_media": {"telegram": [updated_post]},
                }
            },
            name="import2.json",
        )
        importer = JSONImporter(path2)
        importer.run()

        account = SocialMediaAccount.objects.get()
        assert account.display_name == "Renamed"
        assert account.follower_count == 2000

        stats = importer.log_stats()
        assert "SocialMediaAccount" in stats
        diffs = stats["SocialMediaAccount"]["updated"][0]["diff"]
        assert diffs["display_name"] == {"old": "Example", "new": "Renamed"}
        assert diffs["follower_count"] == {"old": 1000, "new": 2000}
        # Post fields are unchanged, so no SocialMediaPost update.
        assert "SocialMediaPost" not in stats
        # Evidence picks up the new documentation_date from the newer collected_at.
        evidence_diff = stats["Evidence"]["updated"][0]["diff"]
        assert evidence_diff == {
            "documentation_date": {"old": "2024-01-02", "new": "2024-03-01"},
        }

    @pytest.mark.django_db
    def test_older_collected_at_does_not_update_profile(self, person, tmp_path):
        path = _write_dump(
            tmp_path,
            {
                person.name_hash: {
                    "label": "Max Mustermann",
                    "social_media": {"telegram": [_make_post()]},
                }
            },
        )
        JSONImporter(path).run()

        stale_post = _make_post(
            collected_at="2023-01-01T00:00:00+00:00",
            account=_make_account(display_name="Old Name", follower_count=1),
        )
        path2 = _write_dump(
            tmp_path,
            {
                person.name_hash: {
                    "label": "Max Mustermann",
                    "social_media": {"telegram": [stale_post]},
                }
            },
            name="import2.json",
        )
        importer = JSONImporter(path2)
        importer.run()

        account = SocialMediaAccount.objects.get()
        # Profile stays at the first import's values.
        assert account.display_name == "Example"
        assert account.follower_count == 1000

        assert "SocialMediaAccount" not in importer.log_stats()

    @pytest.mark.django_db
    def test_post_field_change_is_tracked(self, person, tmp_path):
        path = _write_dump(
            tmp_path,
            {
                person.name_hash: {
                    "label": "Max Mustermann",
                    "social_media": {"telegram": [_make_post()]},
                }
            },
        )
        JSONImporter(path).run()

        updated_post = _make_post(
            text="Edited text",
            like_count=99,
        )
        path2 = _write_dump(
            tmp_path,
            {
                person.name_hash: {
                    "label": "Max Mustermann",
                    "social_media": {"telegram": [updated_post]},
                }
            },
            name="import2.json",
        )
        importer = JSONImporter(path2)
        importer.run()

        sm_post = SocialMediaPost.objects.get()
        assert sm_post.text == "Edited text"
        assert sm_post.like_count == 99

        stats = importer.log_stats()
        assert "SocialMediaPost" in stats
        diff = stats["SocialMediaPost"]["updated"][0]["diff"]
        assert diff["text"] == {"old": "Hello", "new": "Edited text"}
        assert diff["like_count"] == {"old": 5, "new": 99}

    @pytest.mark.django_db
    def test_unknown_label_is_skipped(self, tmp_path):
        path = _write_dump(
            tmp_path,
            {
                "deadbeef": {
                    "label": "Nobody Here",
                    "social_media": {"telegram": [_make_post()]},
                }
            },
        )

        importer = JSONImporter(path)
        importer.run()

        assert SocialMediaAccount.objects.count() == 0
        assert SocialMediaPost.objects.count() == 0
        assert Evidence.objects.count() == 0

        stats = importer.log_stats()
        assert set(stats.keys()) == {"Actor"}
        assert stats["Actor"]["created"] == []
        assert stats["Actor"]["updated"] == []
        assert stats["Actor"]["deleted"] == []
        assert len(stats["Actor"]["skipped"]) == 1
        assert "Nobody Here" in stats["Actor"]["skipped"][0]

    @pytest.mark.django_db
    def test_resolves_organization_by_name(self, db, tmp_path):
        org = OrganizationFactory(organization_name="Landesverband Sachsen")
        path = _write_dump(
            tmp_path,
            {
                "anykey": {
                    "label": "Landesverband Sachsen",
                    "social_media": {"telegram": [_make_post()]},
                }
            },
        )

        JSONImporter(path).run()

        account = SocialMediaAccount.objects.get()
        assert account.actor.organization == org
        assert Evidence.objects.count() == 1

    @pytest.mark.django_db
    def test_resolves_organization_by_alias(self, db, tmp_path):
        # Dump uses the new name; the DB org still carries it as an alias after
        # the align step renamed it.
        org = OrganizationFactory(
            organization_name="Landtagsfraktion Sachsen",
            also_known_as=["AfD-Fraktion im Landtag Sachsen"],
        )
        path = _write_dump(
            tmp_path,
            {
                "anykey": {
                    "label": "AfD-Fraktion im Landtag Sachsen",
                    "social_media": {"telegram": [_make_post()]},
                }
            },
        )

        JSONImporter(path).run()

        assert SocialMediaPost.objects.get().account.actor.organization == org

    @pytest.mark.django_db
    def test_ambiguous_label_is_skipped(self, db, tmp_path):
        # A Person and an Organization normalize to the same name -> skip.
        PersonFactory(first_name="Junge", last_name="Alternative", external_id=2)
        OrganizationFactory(organization_name="Junge Alternative")
        path = _write_dump(
            tmp_path,
            {
                "anykey": {
                    "label": "Junge Alternative",
                    "social_media": {"telegram": [_make_post()]},
                }
            },
        )

        importer = JSONImporter(path)
        importer.run()

        assert SocialMediaAccount.objects.count() == 0
        stats = importer.log_stats()
        assert len(stats["Actor"]["skipped"]) == 1
        assert "multiple actors" in stats["Actor"]["skipped"][0]

    @pytest.mark.django_db
    def test_reply_resolution_within_same_batch(self, person, tmp_path):
        parent = _make_post(platform_post_id="100", url="https://t.me/example/100")
        reply = _make_post(
            platform_post_id="101",
            url="https://t.me/example/101",
            reply_to={"reply_to_msg_id": "100"},
        )
        path = _write_dump(
            tmp_path,
            {
                person.name_hash: {
                    "label": "Max Mustermann",
                    "social_media": {"telegram": [parent, reply]},
                }
            },
        )

        importer = JSONImporter(path)
        importer.run()

        parent_post = SocialMediaPost.objects.get(platform_post_id="100")
        reply_post = SocialMediaPost.objects.get(platform_post_id="101")
        assert reply_post.reply_to_id == parent_post.id

        stats = importer.log_stats()
        # The reply is first created with reply_to=None, then patched up — that
        # surfaces as one update on SocialMediaPost.
        post_updates = stats["SocialMediaPost"]["updated"]
        assert len(post_updates) == 1
        assert post_updates[0]["id"] == reply_post.id
        assert "reply_to" in post_updates[0]["diff"]

    @pytest.mark.django_db
    def test_quote_reference_creates_stub_account_and_post(self, person, tmp_path):
        post = _make_post(
            url="https://t.me/example/200",
            platform_post_id="200",
            references=[
                {
                    "platform_post_id": "999",
                    "url": "https://t.me/somebody/999",
                    "created_at": "2024-01-01T01:00:00+00:00",
                    "text": "Quoted text",
                    "account": {
                        "username": "somebody",
                        "platform_user_id": "987",
                        "display_name": "Somebody",
                        "description": "Some bio",
                        "url": "https://x.com/somebody",
                        "follower_count": 321,
                        "is_verified": False,
                        "is_blue_verified": True,
                    },
                }
            ],
        )
        path = _write_dump(
            tmp_path,
            {
                person.name_hash: {
                    "label": "Max Mustermann",
                    "social_media": {"telegram": [post]},
                }
            },
        )

        importer = JSONImporter(path)
        importer.run()

        assert SocialMediaAccount.objects.count() == 2
        stub_account = SocialMediaAccount.objects.get(username="somebody")
        # Stub accounts are not linked to an Actor.
        assert stub_account.actor is None
        # The full profile carried by the reference is copied onto the stub.
        assert stub_account.platform_user_id == "987"
        assert stub_account.display_name == "Somebody"
        assert stub_account.description == "Some bio"
        assert stub_account.url == "https://x.com/somebody"
        assert stub_account.follower_count == 321
        # is_verified is derived from is_verified OR is_blue_verified.
        assert stub_account.is_verified is True

        stub_post = SocialMediaPost.objects.get(platform_post_id="999")
        main_post = SocialMediaPost.objects.get(platform_post_id="200")
        assert main_post.redistributes_id == stub_post.id

        stats = importer.log_stats()
        # Main account + stub account were both created.
        assert len(stats["SocialMediaAccount"]["created"]) == 2
        # Main post + stub post were both created. The reference link is set
        # right after creation, which also produces one update.
        assert len(stats["SocialMediaPost"]["created"]) == 2
        assert len(stats["SocialMediaPost"]["updated"]) == 1

    @pytest.mark.django_db
    def test_stub_account_is_completed_when_seen_as_main_post(self, person, tmp_path):
        # Run 1: account "987" is only referenced -> created as an orphan stub
        # with no actor and no profile data.
        referencing_post = _make_post(
            url="https://t.me/example/200",
            platform_post_id="200",
            references=[
                {
                    "platform_post_id": "999",
                    "url": "https://t.me/somebody/999",
                    "created_at": "2024-01-01T01:00:00+00:00",
                    "text": "Quoted text",
                    "account": {"platform_user_id": "987"},
                }
            ],
        )
        path = _write_dump(
            tmp_path,
            {
                person.name_hash: {
                    "label": "Max Mustermann",
                    "social_media": {"telegram": [referencing_post]},
                }
            },
        )
        JSONImporter(path).run()

        stub = SocialMediaAccount.objects.get(platform_user_id="987")
        assert stub.actor is None
        assert stub.url == ""
        assert stub.display_name == ""

        # Run 2: the same account shows up as a real post. collected_at is None
        # (telegram carries no timestamp) which previously suppressed the
        # profile refresh entirely.
        main_post = _make_post(
            url="https://t.me/somebody/500",
            platform_post_id="500",
            collected_at=None,
            account=_make_account(
                username="somebody",
                platform_user_id="987",
                display_name="Somebody Real",
                url="https://t.me/somebody",
                follower_count=42,
            ),
        )
        path2 = _write_dump(
            tmp_path,
            {
                person.name_hash: {
                    "label": "Max Mustermann",
                    "social_media": {"telegram": [main_post]},
                }
            },
            name="import2.json",
        )
        JSONImporter(path2).run()

        # Same row is reused (keyed on platform_user_id), not duplicated.
        assert SocialMediaAccount.objects.filter(platform_user_id="987").count() == 1
        stub.refresh_from_db()
        assert stub.actor == person.actor
        # The full profile from the main post is backfilled onto the stub.
        assert stub.username == "somebody"
        assert stub.display_name == "Somebody Real"
        assert stub.description == "An example account"
        assert stub.url == "https://t.me/somebody"
        assert stub.follower_count == 42
        assert stub.is_verified is False

    @pytest.mark.django_db
    def test_mentions_are_added_and_removed_across_runs(self, person, tmp_path):
        path = _write_dump(
            tmp_path,
            {
                person.name_hash: {
                    "label": "Max Mustermann",
                    "social_media": {
                        "telegram": [
                            _make_post(
                                report_data={
                                    "footnote_url": ["https://t.me/example/1"],
                                    "topic": ["A", "B"],
                                    "footnote_id": ["1", "2"],
                                    "chapter_sturcrue": [["Ch 1"], ["Ch 2"]],
                                }
                            ),
                        ]
                    },
                }
            },
        )
        JSONImporter(path).run()

        evidence = Evidence.objects.get()
        assert evidence.mentions.count() == 2

        path2 = _write_dump(
            tmp_path,
            {
                person.name_hash: {
                    "label": "Max Mustermann",
                    "social_media": {
                        "telegram": [
                            _make_post(
                                report_data={
                                    "footnote_url": ["https://t.me/example/1"],
                                    "topic": ["B", "C"],
                                    "footnote_id": ["2", "3"],
                                    "chapter_sturcrue": [["Ch 2"], ["Ch 3"]],
                                }
                            ),
                        ]
                    },
                }
            },
            name="import2.json",
        )
        importer = JSONImporter(path2)
        importer.run()

        evidence.refresh_from_db()
        existing = {(m.category.name, m.footnote) for m in evidence.mentions.all()}
        assert existing == {("B", "2"), ("C", "3")}

        stats = importer.log_stats()
        assert len(stats["EvidenceMention"]["created"]) == 1
        assert len(stats["EvidenceMention"]["deleted"]) == 1
        # Mention (B, 2) is untouched, no spurious churn.
        assert EvidenceMention.objects.filter(category__name="B").count() == 1

    def test_chapter_tree_is_built_from_structure(self, person, tmp_path):
        path = _write_dump(
            tmp_path,
            {
                person.name_hash: {
                    "label": "Max Mustermann",
                    "social_media": {
                        "telegram": [
                            _make_post(
                                platform_post_id="1",
                                url="https://t.me/example/1",
                                report_data={
                                    "footnote_url": ["https://t.me/example/1"],
                                    "topic": ["T"],
                                    "footnote_id": ["1"],
                                    "chapter_sturcrue": [["Root", "T", "Leaf A"]],
                                },
                            ),
                            _make_post(
                                platform_post_id="2",
                                url="https://t.me/example/2",
                                report_data={
                                    "footnote_url": ["https://t.me/example/2"],
                                    "topic": ["T"],
                                    "footnote_id": ["2"],
                                    "chapter_sturcrue": [["Root", "T", "Leaf B"]],
                                },
                            ),
                        ]
                    },
                }
            },
        )
        JSONImporter(path).run()

        # Shared prefixes reuse nodes: Root -> T -> {Leaf A, Leaf B}.
        assert Chapter.objects.count() == 4
        root = Chapter.objects.get(custom_label="Root")
        topic = Chapter.objects.get(custom_label="T")
        leaf_a = Chapter.objects.get(custom_label="Leaf A")
        leaf_b = Chapter.objects.get(custom_label="Leaf B")

        assert topic.get_parent() == root
        assert set(topic.get_children()) == {leaf_a, leaf_b}

        # is_main_topic is set only on the node matching the `topic` field.
        assert topic.is_main_topic
        assert not root.is_main_topic
        assert not leaf_a.is_main_topic

        # The leaf chapter is linked from each mention.
        assert {m.chapter for m in EvidenceMention.objects.all()} == {leaf_a, leaf_b}

        # Subsumed counts include descendants.
        assert root.subsumed_evidences().count() == 2
        assert topic.subsumed_evidences().count() == 2
        assert leaf_a.subsumed_evidences().count() == 1
        assert leaf_b.subsumed_evidences().count() == 1

    @pytest.mark.django_db
    def test_seeds_posted_by_relation_to_account_actor(self, person, tmp_path):
        path = _write_dump(
            tmp_path,
            {
                person.name_hash: {
                    "label": "Max Mustermann",
                    "social_media": {"telegram": [_make_post()]},
                }
            },
        )
        JSONImporter(path).run()

        evidence = Evidence.objects.get()
        relations = EvidenceActorRelation.objects.filter(evidence=evidence)
        assert relations.count() == 1
        relation = relations.get()
        assert relation.role.name == "posted_by"
        assert relation.actor == person.actor

    @pytest.mark.django_db
    def test_links_reply_to_parent_post_within_batch(self, person, tmp_path):
        parent = _make_post(platform_post_id="100", url="https://t.me/example/100")
        reply = _make_post(
            platform_post_id="101",
            url="https://t.me/example/101",
            reply_to={"reply_to_msg_id": "100"},
        )
        path = _write_dump(
            tmp_path,
            {
                person.name_hash: {
                    "label": "Max Mustermann",
                    "social_media": {"telegram": [parent, reply]},
                }
            },
        )

        JSONImporter(path).run()

        parent_post = SocialMediaPost.objects.get(platform_post_id="100")
        reply_post = SocialMediaPost.objects.get(platform_post_id="101")

        assert reply_post.reply_to_id == parent_post.id

    @pytest.mark.django_db
    def test_links_redistributes_when_quoted_post_is_also_imported(
        self, person, tmp_path
    ):
        # Main post quotes another post by the same account that is fully
        # imported as a second item in the same dump.
        quoted_post_item = _make_post(
            platform_post_id="500",
            url="https://t.me/example/500",
            text="Original",
        )
        main_post_item = _make_post(
            platform_post_id="501",
            url="https://t.me/example/501",
            text="Quoting",
            references=[
                {
                    "platform_post_id": "500",
                    "url": "https://t.me/example/500",
                    "created_at": "2024-01-01T09:00:00+00:00",
                    "text": "Original",
                    "account": _make_account(),
                }
            ],
        )
        path = _write_dump(
            tmp_path,
            {
                person.name_hash: {
                    "label": "Max Mustermann",
                    "social_media": {
                        "telegram": [main_post_item, quoted_post_item],
                    },
                }
            },
        )

        JSONImporter(path).run()

        main_post = SocialMediaPost.objects.get(platform_post_id="501")
        quoted_post = SocialMediaPost.objects.get(platform_post_id="500")

        assert main_post.redistributes_id == quoted_post.id

    @pytest.mark.django_db
    def test_dry_run_makes_no_changes_to_posts_or_accounts(self, person, tmp_path):
        path = _write_dump(
            tmp_path,
            {
                person.name_hash: {
                    "label": "Max Mustermann",
                    "social_media": {"telegram": [_make_post()]},
                }
            },
        )

        importer = JSONImporter(path, dry_run=True)
        importer.run()

        assert SocialMediaAccount.objects.count() == 0
        assert SocialMediaPost.objects.count() == 0
        assert Evidence.objects.count() == 0
