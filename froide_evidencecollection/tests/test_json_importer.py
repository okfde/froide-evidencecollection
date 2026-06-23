import json
from datetime import timedelta

import pytest

from froide_evidencecollection.json_importer import JSONImporter
from froide_evidencecollection.models import (
    Actor,
    Chapter,
    Evidence,
    EvidenceMention,
    SocialMediaAccount,
    SocialMediaPost,
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
    return PersonFactory(first_name="Max", last_name="Mustermann", external_id=1)


class TestJSONImporter:
    @pytest.mark.django_db
    def test_import_creates_account_post_evidence_and_actor(self, person, tmp_path):
        path = _write_dump(
            tmp_path,
            {
                str(person.pk): {
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
        # Accounts are never linked to an Actor by the import.
        assert account.actor is None
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
        (tmp_path / "shot").mkdir()
        (tmp_path / "shot" / "s.png").write_bytes(b"fake-screenshot")

        post = _make_post(
            # Image/video are tracked by source path only (binaries not stored).
            image_file="./img/y.png",
            # `image_alt_text` accompanies `image_file`: the alt-text description.
            image_alt_text={"alt_text": "A protest sign"},
            video_file="./video/x.mp4",
            # The full video transcript, kept verbatim on the post.
            transcription="the full transcript",
            # `screenshot_file` is the one file-backed post media (single path).
            screenshot_file="./shot/s.png",
            # `video_timestamp` is row-parallel to topic/footnote/fliesstext and
            # folds onto the matching mention as start/end/raw_transcript.
            report_data={
                "footnote_url": ["https://t.me/example/1"],
                "topic": ["Disinformation"],
                "footnote_id": ["fn1"],
                "fliesstext": ["the curated quote"],
                "video_timestamp": [
                    {"start": "00:00:01", "end": "00:00:05", "excerpt": "spoken words"},
                ],
            },
        )
        path = _write_dump(
            tmp_path,
            {
                str(person.pk): {
                    "label": "Max Mustermann",
                    "social_media": {"telegram": [post]},
                }
            },
        )

        JSONImporter(path).run()

        sm_post = SocialMediaPost.objects.get()
        # Media tracked by source path on the post; only the screenshot is a file.
        assert sm_post.image_source_path == "./img/y.png"
        assert sm_post.image_description == "A protest sign"
        assert sm_post.video_source_path == "./video/x.mp4"
        assert sm_post.is_video is True
        assert sm_post.transcription == "the full transcript"
        assert sm_post.screenshot_source_path == "./shot/s.png"
        assert sm_post.screenshot.read() == b"fake-screenshot"

        # The video_timestamp folds onto the (single) mention, parsed into
        # start/end durations; the curated quote lands in `citation`.
        mention = EvidenceMention.objects.get()
        assert mention.citation == "the curated quote"
        assert mention.raw_transcript == "spoken words"
        assert mention.start == timedelta(seconds=1)
        assert mention.end == timedelta(seconds=5)

        # Idempotent: a second run neither duplicates rows nor reports changes.
        importer = JSONImporter(path)
        importer.run()
        assert SocialMediaPost.objects.count() == 1
        assert EvidenceMention.objects.count() == 1
        assert importer.log_stats() == {}

    @pytest.mark.django_db
    def test_reimport_backfills_missing_screenshot(self, person, tmp_path, settings):
        settings.MEDIA_ROOT = str(tmp_path / "media")
        (tmp_path / "shot").mkdir()
        screenshot = tmp_path / "shot" / "s.png"

        post = _make_post(
            screenshot_file="./shot/s.png",
            report_data={"footnote_url": ["https://t.me/example/1"]},
        )
        path = _write_dump(
            tmp_path,
            {
                str(person.pk): {
                    "label": "Max Mustermann",
                    "social_media": {"telegram": [post]},
                }
            },
        )

        # First run: the file isn't on disk yet, so the post is saved without a
        # screenshot (the source path is still recorded).
        JSONImporter(path).run()
        sm_post = SocialMediaPost.objects.get()
        assert not sm_post.screenshot
        assert sm_post.screenshot_source_path == "./shot/s.png"

        # The file becomes available; a re-import backfills it onto the existing
        # post rather than skipping it.
        screenshot.write_bytes(b"fake-screenshot")
        JSONImporter(path).run()
        sm_post.refresh_from_db()
        assert sm_post.screenshot.read() == b"fake-screenshot"

    @pytest.mark.django_db
    def test_video_timestamps_fold_into_mentions(self, person, tmp_path):
        post = _make_post(
            video_file="./video/x.mp4",
            report_data={
                "footnote_url": ["https://t.me/example/1"],
                "topic": ["A", "B"],
                "footnote_id": ["1", "2"],
                "fliesstext": ["quote a", "quote b"],
                "video_timestamp": [
                    {"start": "00:00:10", "end": "00:00:20", "excerpt": "first"},
                    {"start": "01:02:03", "end": "01:02:30", "excerpt": "second"},
                ],
            },
        )
        path = _write_dump(
            tmp_path,
            {
                str(person.pk): {
                    "label": "Max Mustermann",
                    "social_media": {"telegram": [post]},
                }
            },
        )

        JSONImporter(path).run()

        # Each video_timestamp lands on its row-parallel mention.
        mentions = {m.category.name: m for m in EvidenceMention.objects.all()}
        assert mentions["A"].raw_transcript == "first"
        assert mentions["A"].start == timedelta(seconds=10)
        assert mentions["A"].end == timedelta(seconds=20)
        assert mentions["B"].raw_transcript == "second"
        assert mentions["B"].start == timedelta(hours=1, minutes=2, seconds=3)

        # Idempotent: re-running reports no mention changes.
        importer = JSONImporter(path)
        importer.run()
        assert EvidenceMention.objects.count() == 2
        assert "EvidenceMention" not in importer.log_stats()

    @pytest.mark.django_db
    def test_same_post_under_multiple_people_unions_mentions(self, person, tmp_path):
        # The same post (same account + platform_post_id) is grouped under two
        # scrape targets, each carrying its own report_data. Both occurrences map
        # to one SocialMediaPost/Evidence, so the importer must keep the union of
        # mentions rather than letting the second target wipe the first's.
        other = PersonFactory(first_name="Erika", last_name="Musterfrau", external_id=2)
        post_for_max = _make_post(
            report_data={"topic": ["A"], "footnote_id": ["fn-max"]}
        )
        post_for_erika = _make_post(
            report_data={"topic": ["B"], "footnote_id": ["fn-erika"]}
        )
        path = _write_dump(
            tmp_path,
            {
                str(person.pk): {
                    "label": "Max Mustermann",
                    "social_media": {"telegram": [post_for_max]},
                },
                str(other.pk): {
                    "label": "Erika Musterfrau",
                    "social_media": {"telegram": [post_for_erika]},
                },
            },
        )

        JSONImporter(path).run()

        # One shared post/evidence, both footnotes preserved.
        assert SocialMediaPost.objects.count() == 1
        evidence = Evidence.objects.get()
        by_footnote = {m.footnote: m for m in EvidenceMention.objects.all()}
        assert set(by_footnote) == {"fn-max", "fn-erika"}

        # Both grouped people are recorded as originators of the shared evidence,
        # and each mention is attributed to the person it was grouped under.
        assert set(evidence.originators.all()) == {person.actor, other.actor}
        assert by_footnote["fn-max"].originator == person.actor
        assert by_footnote["fn-erika"].originator == other.actor

        # Idempotent: a second run keeps both mentions and reports no changes.
        importer = JSONImporter(path)
        importer.run()
        assert EvidenceMention.objects.count() == 2
        assert "EvidenceMention" not in importer.log_stats()

    @pytest.mark.django_db
    def test_reimport_same_data_produces_no_changes(self, person, tmp_path):
        path = _write_dump(
            tmp_path,
            {
                str(person.pk): {
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
                str(person.pk): {
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
                str(person.pk): {
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
                str(person.pk): {
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
                str(person.pk): {
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
    def test_account_upserted_once_from_freshest_snapshot(self, person, tmp_path):
        # Two posts of the same account in one run carry disagreeing snapshots:
        # a dateless one (listed first) and a dated one. The account must be
        # written once from the dated (freshest) snapshot, not flapped between
        # the two — and its collected_at must not be wiped by the dateless post.
        dateless = _make_post(
            platform_post_id="1",
            url="https://t.me/example/1",
            collected_at=None,
            account=_make_account(follower_count=999, display_name="Stale"),
        )
        dated = _make_post(
            platform_post_id="2",
            url="https://t.me/example/2",
            collected_at="2024-05-01T12:00:00+00:00",
            account=_make_account(follower_count=2000, display_name="Fresh"),
        )
        path = _write_dump(
            tmp_path,
            {
                str(person.pk): {
                    "label": "Max Mustermann",
                    "social_media": {"telegram": [dateless, dated]},
                }
            },
        )
        importer = JSONImporter(path)
        importer.run()

        account = SocialMediaAccount.objects.get()
        assert account.follower_count == 2000
        assert account.display_name == "Fresh"
        assert account.collected_at.isoformat() == "2024-05-01T12:00:00+00:00"

        stats = importer.log_stats()
        # Created exactly once, never updated-then-reverted within the run.
        assert len(stats["SocialMediaAccount"]["created"]) == 1
        assert stats["SocialMediaAccount"]["updated"] == []

    @pytest.mark.django_db
    def test_dateless_repost_does_not_wipe_stored_collected_at(self, person, tmp_path):
        # Regression: a later dateless dump used to overwrite the stored
        # collected_at (and profile) with None / its own values.
        path = _write_dump(
            tmp_path,
            {
                str(person.pk): {
                    "label": "Max Mustermann",
                    "social_media": {"telegram": [_make_post()]},
                }
            },
        )
        JSONImporter(path).run()

        dateless = _make_post(
            collected_at=None,
            account=_make_account(display_name="Should Not Win", follower_count=7),
        )
        path2 = _write_dump(
            tmp_path,
            {
                str(person.pk): {
                    "label": "Max Mustermann",
                    "social_media": {"telegram": [dateless]},
                }
            },
            name="import2.json",
        )
        importer = JSONImporter(path2)
        importer.run()

        account = SocialMediaAccount.objects.get()
        assert account.collected_at.isoformat() == "2024-01-02T12:00:00+00:00"
        assert account.display_name == "Example"
        assert account.follower_count == 1000
        assert "SocialMediaAccount" not in importer.log_stats()

    @pytest.mark.django_db
    def test_post_field_change_is_tracked(self, person, tmp_path):
        path = _write_dump(
            tmp_path,
            {
                str(person.pk): {
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
                str(person.pk): {
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
        # The label resolves to the org (its Actor is created), but the account
        # is not linked to it.
        assert account.actor is None
        assert Actor.objects.get().organization == org
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

        assert SocialMediaPost.objects.get().account.actor is None
        assert Actor.objects.get().organization == org

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
                str(person.pk): {
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
                str(person.pk): {
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
                str(person.pk): {
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
                str(person.pk): {
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
        # The account is still not linked to an actor (the import never links).
        assert stub.actor is None
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
                str(person.pk): {
                    "label": "Max Mustermann",
                    "social_media": {
                        "telegram": [
                            _make_post(
                                report_data={
                                    "footnote_url": ["https://t.me/example/1"],
                                    "topic": [["A"], ["B"]],
                                    "footnote_id": ["1", "2"],
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
                str(person.pk): {
                    "label": "Max Mustermann",
                    "social_media": {
                        "telegram": [
                            _make_post(
                                report_data={
                                    "footnote_url": ["https://t.me/example/1"],
                                    "topic": [["B"], ["C"]],
                                    "footnote_id": ["2", "3"],
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

    def test_chapter_tree_is_built_from_topic_paths(self, person, tmp_path):
        path = _write_dump(
            tmp_path,
            {
                str(person.pk): {
                    "label": "Max Mustermann",
                    "social_media": {
                        "telegram": [
                            _make_post(
                                platform_post_id="1",
                                url="https://t.me/example/1",
                                report_data={
                                    "footnote_url": ["https://t.me/example/1"],
                                    "topic": [["Root", "T", "Leaf A"]],
                                    "footnote_id": ["1"],
                                },
                            ),
                            _make_post(
                                platform_post_id="2",
                                url="https://t.me/example/2",
                                report_data={
                                    "footnote_url": ["https://t.me/example/2"],
                                    "topic": [["Root", "T", "Leaf B"]],
                                    "footnote_id": ["2"],
                                },
                            ),
                        ]
                    },
                }
            },
        )
        JSONImporter(path).run()

        # The chapter tree is built from the topic path; shared prefixes reuse
        # nodes: Root -> T -> {Leaf A, Leaf B}.
        assert Chapter.objects.count() == 4
        root = Chapter.objects.get(custom_label="Root")
        topic = Chapter.objects.get(custom_label="T")
        leaf_a = Chapter.objects.get(custom_label="Leaf A")
        leaf_b = Chapter.objects.get(custom_label="Leaf B")

        assert topic.get_parent() == root
        assert set(topic.get_children()) == {leaf_a, leaf_b}

        # The leaf of each topic path is the main topic; ancestors are not.
        assert leaf_a.is_main_topic
        assert leaf_b.is_main_topic
        assert not root.is_main_topic
        assert not topic.is_main_topic

        # The leaf names the mention's category and is linked as its chapter.
        assert {m.category.name for m in EvidenceMention.objects.all()} == {
            "Leaf A",
            "Leaf B",
        }
        assert {m.chapter for m in EvidenceMention.objects.all()} == {leaf_a, leaf_b}

        # Subsumed counts include descendants.
        assert root.subsumed_evidences().count() == 2

    def test_topic_path_collapses_adjacent_duplicate_labels(self, person, tmp_path):
        path = _write_dump(
            tmp_path,
            {
                str(person.pk): {
                    "label": "Max Mustermann",
                    "social_media": {
                        "telegram": [
                            _make_post(
                                report_data={
                                    "footnote_url": ["https://t.me/example/1"],
                                    "topic": [
                                        [
                                            "Menschenwürde",
                                            "Ausbürgerung",
                                            "Ausbürgerung",
                                        ]
                                    ],
                                    "footnote_id": ["1"],
                                },
                            ),
                        ]
                    },
                }
            },
        )
        JSONImporter(path).run()

        # The repeated leaf is collapsed: Menschenwürde -> Ausbürgerung (no
        # same-label child), and the leaf is the mention's category.
        assert Chapter.objects.count() == 2
        leaf = Chapter.objects.get(custom_label="Ausbürgerung")
        assert leaf.get_parent().custom_label == "Menschenwürde"
        assert leaf.is_main_topic
        mention = EvidenceMention.objects.get()
        assert mention.category.name == "Ausbürgerung"
        assert mention.chapter == leaf
        assert mention.chapter_structure == ["Menschenwürde", "Ausbürgerung"]

    @pytest.mark.django_db
    def test_originators_come_from_grouping_not_account_holder(self, person, tmp_path):
        path = _write_dump(
            tmp_path,
            {
                str(person.pk): {
                    "label": "Max Mustermann",
                    "social_media": {"telegram": [_make_post()]},
                }
            },
        )
        JSONImporter(path).run()

        # The post is grouped under Max, so he is recorded as an originator from
        # the dump grouping — even though the scraped account is not linked to him.
        evidence = Evidence.objects.get()
        account = SocialMediaAccount.objects.get()
        assert account.actor is None
        assert list(evidence.originators.all()) == [person.actor]

        # Linking the posting account to another actor must NOT make that actor
        # an originator: the account holder is never assumed to be the originator.
        other = PersonFactory(first_name="Erika", last_name="Musterfrau", external_id=2)
        other_actor = Actor.objects.create(person=other)
        account.actor = other_actor
        account.save()
        JSONImporter(path).run()

        evidence.refresh_from_db()
        assert list(evidence.originators.all()) == [person.actor]

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
                str(person.pk): {
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
                str(person.pk): {
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
    def test_redistributes_links_single_reference_without_flapping(
        self, person, tmp_path
    ):
        # A post that both quotes and retweets carries two linkable references.
        # `redistributes` is a single FK, so only one is linked (the last),
        # written once — not set to the first and then overwritten by the second.
        post = _make_post(
            url="https://t.me/example/200",
            platform_post_id="200",
            references=[
                {
                    "platform_post_id": "900",
                    "url": "https://t.me/somebody/900",
                    "created_at": "2024-01-01T01:00:00+00:00",
                    "text": "Quoted",
                    "account": {"platform_user_id": "987"},
                },
                {
                    "platform_post_id": "901",
                    "url": "https://t.me/somebody/901",
                    "created_at": "2024-01-01T02:00:00+00:00",
                    "text": "Retweeted",
                    "account": {"platform_user_id": "987"},
                },
            ],
        )
        path = _write_dump(
            tmp_path,
            {
                str(person.pk): {
                    "label": "Max Mustermann",
                    "social_media": {"telegram": [post]},
                }
            },
        )
        importer = JSONImporter(path)
        importer.run()

        main_post = SocialMediaPost.objects.get(platform_post_id="200")
        winner = SocialMediaPost.objects.get(platform_post_id="901")
        assert main_post.redistributes_id == winner.id
        # The unlinked reference is not materialized as an orphan stub.
        assert not SocialMediaPost.objects.filter(platform_post_id="900").exists()

        stats = importer.log_stats()
        # The main post is linked exactly once (one update), not twice.
        redistribute_updates = [
            u
            for u in stats["SocialMediaPost"].get("updated", [])
            if u["id"] == main_post.id
        ]
        assert len(redistribute_updates) == 1

    @pytest.mark.django_db
    def test_dry_run_makes_no_changes_to_posts_or_accounts(self, person, tmp_path):
        path = _write_dump(
            tmp_path,
            {
                str(person.pk): {
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
