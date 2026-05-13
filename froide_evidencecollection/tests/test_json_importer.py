import json

import pytest

from froide_evidencecollection.json_importer import JSONImporter
from froide_evidencecollection.models import (
    Actor,
    Evidence,
    EvidenceActorRelation,
    EvidenceMention,
    EvidenceRelation,
    SocialMediaAccount,
    SocialMediaPost,
)

from .factories import PersonFactory


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
        "transcription": "",
        "view_count": 100,
        "like_count": 5,
        "comment_count": 1,
        "share_count": 0,
        "reactions": None,
        "categories": [],
        "pages": [],
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
        assert account.display_name == "Example"
        assert account.follower_count == 1000

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
    def test_reimport_same_data_produces_no_changes(self, person, tmp_path):
        path = _write_dump(
            tmp_path,
            {person.name_hash: {"social_media": {"telegram": [_make_post()]}}},
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
            {person.name_hash: {"social_media": {"telegram": [_make_post()]}}},
        )
        JSONImporter(path).run()

        updated_post = _make_post(
            collected_at="2024-03-01T12:00:00+00:00",
            account=_make_account(display_name="Renamed", follower_count=2000),
        )
        path2 = _write_dump(
            tmp_path,
            {person.name_hash: {"social_media": {"telegram": [updated_post]}}},
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
            {person.name_hash: {"social_media": {"telegram": [_make_post()]}}},
        )
        JSONImporter(path).run()

        stale_post = _make_post(
            collected_at="2023-01-01T00:00:00+00:00",
            account=_make_account(display_name="Old Name", follower_count=1),
        )
        path2 = _write_dump(
            tmp_path,
            {person.name_hash: {"social_media": {"telegram": [stale_post]}}},
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
            {person.name_hash: {"social_media": {"telegram": [_make_post()]}}},
        )
        JSONImporter(path).run()

        updated_post = _make_post(
            text="Edited text",
            like_count=99,
        )
        path2 = _write_dump(
            tmp_path,
            {person.name_hash: {"social_media": {"telegram": [updated_post]}}},
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
    def test_missing_person_hash_is_skipped(self, tmp_path):
        path = _write_dump(
            tmp_path,
            {
                "deadbeef": {
                    "Name": "Nobody",
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
        assert set(stats.keys()) == {"Person"}
        assert stats["Person"]["created"] == []
        assert stats["Person"]["updated"] == []
        assert stats["Person"]["deleted"] == []
        assert len(stats["Person"]["skipped"]) == 1
        assert "deadbeef" in stats["Person"]["skipped"][0]

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
            {person.name_hash: {"social_media": {"telegram": [parent, reply]}}},
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
                    "kind": "quote",
                    "platform_post_id": "999",
                    "url": "https://t.me/somebody/999",
                    "created_at": "2024-01-01T01:00:00+00:00",
                    "text": "Quoted text",
                    "account": {
                        "username": "somebody",
                        "platform_user_id": "987",
                        "display_name": "Somebody",
                    },
                }
            ],
        )
        path = _write_dump(
            tmp_path,
            {person.name_hash: {"social_media": {"telegram": [post]}}},
        )

        importer = JSONImporter(path)
        importer.run()

        assert SocialMediaAccount.objects.count() == 2
        stub_account = SocialMediaAccount.objects.get(username="somebody")
        # Stub accounts are not linked to an Actor.
        assert stub_account.actor is None

        stub_post = SocialMediaPost.objects.get(platform_post_id="999")
        main_post = SocialMediaPost.objects.get(platform_post_id="200")
        assert main_post.references_id == stub_post.id
        assert main_post.reference_type == SocialMediaPost.ReferenceType.QUOTE

        stats = importer.log_stats()
        # Main account + stub account were both created.
        assert len(stats["SocialMediaAccount"]["created"]) == 2
        # Main post + stub post were both created. The reference link is set
        # right after creation, which also produces one update.
        assert len(stats["SocialMediaPost"]["created"]) == 2
        assert len(stats["SocialMediaPost"]["updated"]) == 1

    @pytest.mark.django_db
    def test_mentions_are_added_and_removed_across_runs(self, person, tmp_path):
        path = _write_dump(
            tmp_path,
            {
                person.name_hash: {
                    "social_media": {
                        "telegram": [
                            _make_post(categories=["A", "B"], pages=[1, 2]),
                        ]
                    }
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
                    "social_media": {
                        "telegram": [
                            _make_post(categories=["B", "C"], pages=[2, 3]),
                        ]
                    }
                }
            },
            name="import2.json",
        )
        importer = JSONImporter(path2)
        importer.run()

        evidence.refresh_from_db()
        existing = {(m.category.name, m.page) for m in evidence.mentions.all()}
        assert existing == {("B", 2), ("C", 3)}

        stats = importer.log_stats()
        assert len(stats["EvidenceMention"]["created"]) == 1
        assert len(stats["EvidenceMention"]["deleted"]) == 1
        # Mention (B, 2) is untouched, no spurious churn.
        assert EvidenceMention.objects.filter(category__name="B").count() == 1

    @pytest.mark.django_db
    def test_seeds_posted_by_relation_to_account_actor(self, person, tmp_path):
        path = _write_dump(
            tmp_path,
            {person.name_hash: {"social_media": {"telegram": [_make_post()]}}},
        )
        JSONImporter(path).run()

        evidence = Evidence.objects.get()
        relations = EvidenceActorRelation.objects.filter(evidence=evidence)
        assert relations.count() == 1
        relation = relations.get()
        assert relation.role.name == "posted_by"
        assert relation.actor == person.actor

    @pytest.mark.django_db
    def test_seeds_replies_to_relation_within_batch(self, person, tmp_path):
        parent = _make_post(platform_post_id="100", url="https://t.me/example/100")
        reply = _make_post(
            platform_post_id="101",
            url="https://t.me/example/101",
            reply_to={"reply_to_msg_id": "100"},
        )
        path = _write_dump(
            tmp_path,
            {person.name_hash: {"social_media": {"telegram": [parent, reply]}}},
        )

        JSONImporter(path).run()

        parent_evidence = Evidence.objects.get(
            social_media_post__platform_post_id="100"
        )
        reply_evidence = Evidence.objects.get(social_media_post__platform_post_id="101")

        relation = EvidenceRelation.objects.get(role__name="replies_to")
        assert relation.from_evidence == reply_evidence
        assert relation.to_evidence == parent_evidence

    @pytest.mark.django_db
    def test_seeds_quotes_relation_when_quoted_post_is_also_imported(
        self, person, tmp_path
    ):
        # Main post quotes another post by the same account that is fully
        # imported as a second item in the same dump. The end-of-run relation
        # sweep should pick up the cross-reference.
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
                    "kind": "quote",
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
                    "social_media": {
                        "telegram": [main_post_item, quoted_post_item],
                    }
                }
            },
        )

        JSONImporter(path).run()

        main_evidence = Evidence.objects.get(social_media_post__platform_post_id="501")
        quoted_evidence = Evidence.objects.get(
            social_media_post__platform_post_id="500"
        )

        relation = EvidenceRelation.objects.get(role__name="quotes")
        assert relation.from_evidence == main_evidence
        assert relation.to_evidence == quoted_evidence

    @pytest.mark.django_db
    def test_dry_run_makes_no_changes_to_posts_or_accounts(self, person, tmp_path):
        path = _write_dump(
            tmp_path,
            {person.name_hash: {"social_media": {"telegram": [_make_post()]}}},
        )

        importer = JSONImporter(path, dry_run=True)
        importer.run()

        assert SocialMediaAccount.objects.count() == 0
        assert SocialMediaPost.objects.count() == 0
        assert Evidence.objects.count() == 0
