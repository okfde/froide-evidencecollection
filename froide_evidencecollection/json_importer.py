import json
import logging
from datetime import datetime

from django.db import transaction

from froide_evidencecollection.models import (
    Actor,
    Category,
    Evidence,
    EvidenceMention,
    Person,
    SocialMediaAccount,
    SocialMediaPost,
)
from froide_evidencecollection.relation_seeding import seed_relations_from_source
from froide_evidencecollection.utils import ImportStatsCollection, equals, to_dict

logger = logging.getLogger(__name__)


PLATFORM_MAP = {
    "facebook": SocialMediaAccount.Platform.FACEBOOK,
    "instagram": SocialMediaAccount.Platform.INSTAGRAM,
    "telegram": SocialMediaAccount.Platform.TELEGRAM,
    "tiktok": SocialMediaAccount.Platform.TIKTOK,
    "twitter": SocialMediaAccount.Platform.X,
    "youtube": SocialMediaAccount.Platform.YOUTUBE,
}

REFERENCE_TYPES = {
    "quote": SocialMediaPost.ReferenceType.QUOTE,
    "repost": SocialMediaPost.ReferenceType.REPOST,
}


def _parse_dt(value):
    if not value:
        return None
    return datetime.fromisoformat(value)


class JSONImporter:
    """
    Import the social-media JSON dump into Evidence + SocialMediaPost rows.

    Expects input that has already been normalized by scripts/prepare_import.py
    (top-level fields named uniformly across platforms, an `account` dict per
    post, and a `references` list for quote/repost relationships).

    Each post becomes one SocialMediaPost and (if not yet curated) one
    Evidence linked via Evidence.social_media_post. Profile fields on
    SocialMediaAccount are updated only when the post's `collected_at` is
    newer than the account's `collected_at`.

    Quote/repost references are materialized inline from each post's
    `references` list (stub accounts/posts via get_or_create). Replies are
    resolved in a second pass over posts inserted during this run. A final
    sweep re-runs the relation seeding so cross-references between Evidence
    pairs created in the same run pick each other up.

    Tracks per-model create/update/delete/skip counts via
    ``ImportStatsCollection`` so the run can be persisted to
    ``ImportExportRun.changes``.
    """

    ACCOUNT_PROFILE_FIELDS = (
        "platform_user_id",
        "display_name",
        "description",
        "url",
        "follower_count",
    )

    def __init__(self, json_path, dry_run=False):
        self.json_path = json_path
        self.dry_run = dry_run
        self.stats = ImportStatsCollection()
        # (account_id, platform_post_id) -> SocialMediaPost.id
        self._post_index = {}
        # SocialMediaPost.id -> (reply_to_platform_post_id, account_id)
        self._pending_replies = {}

    def load(self):
        with open(self.json_path) as f:
            return json.load(f)

    @transaction.atomic
    def run(self):
        data = self.load()
        persons_by_hash = {p.name_hash: p for p in Person.objects.all() if p.name_hash}
        external_id = 1

        for person_id, entry in data.items():
            person = persons_by_hash.get(person_id)
            if person is None:
                msg = (
                    f"No Person for hash {person_id} "
                    f"(name={entry.get('Name')!r}); skipping"
                )
                logger.warning(msg)
                self.stats.track_skipped(Person, msg)
                continue
            actor = self._get_or_create_actor(person)
            for platform, items in (entry.get("social_media") or {}).items():
                if platform not in PLATFORM_MAP:
                    logger.warning("Unknown platform %r; skipping", platform)
                    continue

                for item in items:
                    self._import_item(actor, platform, item, external_id)
                    external_id += 1

        self._resolve_replies()
        self._seed_all_evidence_relations()

    def log_stats(self):
        """Return collected stats in the standard ``ImportExportRun.changes`` shape."""
        return self.stats.to_dict()

    def _get_or_create_actor(self, person):
        try:
            return person.actor
        except Actor.DoesNotExist:
            self.stats.reset_instance(Actor)
            actor = Actor.objects.create(person=person)
            self.stats.track_created(Actor, actor)
            return actor

    # ------------------------------------------------------------------
    # Per-item import
    # ------------------------------------------------------------------
    def _import_item(self, actor, platform, item, external_id):
        account_data = item["account"]
        platform_post_id = str(item["platform_post_id"])
        posted_at = _parse_dt(item.get("created_at"))
        edited_at = _parse_dt(item.get("edited_at"))
        collected_at = _parse_dt(item.get("collected_at"))

        account = self._upsert_account(actor, platform, account_data, collected_at)

        if self.dry_run:
            return

        post_fields = {
            "url": item["url"],
            "posted_at": posted_at,
            "edited_at": edited_at,
            "text": item.get("text") or "",
            "title": item.get("title") or "",
            "transcription": item.get("transcription") or "",
            "view_count": item.get("view_count"),
            "like_count": item.get("like_count"),
            "comment_count": item.get("comment_count"),
            "share_count": item.get("share_count"),
            "reactions": item.get("reactions"),
            "user_snapshot": account_data,
            "raw": item,
        }
        evidence_fields = {
            "documentation_date": collected_at.date() if collected_at else None,
        }

        post = self._upsert_post(account, platform_post_id, post_fields)
        evidence = self._upsert_evidence(post, external_id, evidence_fields)
        seed_relations_from_source(evidence)
        self._upsert_mentions(evidence, item)

        self._post_index[(account.id, post.platform_post_id)] = post.id

        # Quote/repost references (inline stub creation).
        for ref in item.get("references") or []:
            stub_post = self._upsert_stub_post(platform, ref)
            if not stub_post:
                continue
            self._link_reference(post, stub_post, ref)

        # Replies are resolved in a second pass (target may not yet exist).
        reply_id = self._extract_reply_id(platform, item)
        if reply_id is not None:
            self._pending_replies[post.id] = (str(reply_id), account.id)

    def _upsert_post(self, account, platform_post_id, post_fields):
        self.stats.reset_instance(SocialMediaPost)
        post = SocialMediaPost.objects.filter(
            account=account, platform_post_id=platform_post_id
        ).first()
        if post is None:
            post = SocialMediaPost.objects.create(
                account=account,
                platform_post_id=platform_post_id,
                **post_fields,
            )
            self.stats.track_created(SocialMediaPost, post)
            return post

        old_data = to_dict(post)
        update = False
        for field, value in post_fields.items():
            if not equals(getattr(post, field), value):
                setattr(post, field, value)
                update = True
        if update:
            post.save()
            self.stats.track_updated(SocialMediaPost, old_data, post)
        return post

    def _upsert_evidence(self, post, external_id, evidence_fields):
        self.stats.reset_instance(Evidence)
        evidence = Evidence.objects.filter(social_media_post=post).first()
        if evidence is None:
            evidence = Evidence.objects.create(
                external_id=external_id,
                social_media_post=post,
                **evidence_fields,
            )
            self.stats.track_created(Evidence, evidence)
            return evidence

        old_data = to_dict(evidence)
        update = False
        for field, value in evidence_fields.items():
            if not equals(getattr(evidence, field), value):
                setattr(evidence, field, value)
                update = True
        if update:
            evidence.save()
            self.stats.track_updated(Evidence, old_data, evidence)
        return evidence

    def _link_reference(self, post, stub_post, ref):
        self.stats.reset_instance(SocialMediaPost)
        new_reference_type = REFERENCE_TYPES.get(ref["kind"], "")
        if equals(post.references_id, stub_post.id) and equals(
            post.reference_type, new_reference_type
        ):
            return
        old_data = to_dict(post)
        post.references = stub_post
        post.reference_type = new_reference_type
        post.save(update_fields=["references", "reference_type"])
        self.stats.track_updated(SocialMediaPost, old_data, post)

    @staticmethod
    def _extract_reply_id(platform, item):
        if platform == "telegram":
            reply_to = item.get("reply_to") or {}
            return reply_to.get("reply_to_msg_id")
        if platform == "twitter":
            return item.get("in_reply_to_status_id")
        return None

    # ------------------------------------------------------------------
    # Stub account/post upsert (referenced but not directly scraped)
    # ------------------------------------------------------------------
    def _upsert_stub_post(self, platform, ref):
        self.stats.reset_instance(SocialMediaAccount)
        self.stats.reset_instance(SocialMediaPost)
        platform_value = PLATFORM_MAP[platform]
        acct_data = ref["account"]
        username = acct_data["username"]

        account, created_account = SocialMediaAccount.objects.get_or_create(
            platform=platform_value,
            username=username,
            defaults={
                "actor": None,
                "platform_user_id": acct_data.get("platform_user_id") or "",
                "display_name": acct_data.get("display_name") or "",
            },
        )
        if created_account:
            self.stats.track_created(SocialMediaAccount, account)

        post, created_post = SocialMediaPost.objects.get_or_create(
            account=account,
            platform_post_id=ref["platform_post_id"],
            defaults={
                "url": ref.get("url") or "",
                "posted_at": _parse_dt(ref.get("created_at")),
                "text": ref.get("text") or "",
                "raw": ref,
            },
        )
        if created_post:
            self.stats.track_created(SocialMediaPost, post)
            self._post_index[(account.id, post.platform_post_id)] = post.id
        return post

    # ------------------------------------------------------------------
    # Account upsert + profile freshness
    # ------------------------------------------------------------------
    def _upsert_account(self, actor, platform, account_data, collected_at):
        self.stats.reset_instance(SocialMediaAccount)
        platform_value = PLATFORM_MAP[platform]
        username = account_data["username"]
        account = SocialMediaAccount.objects.filter(
            platform=platform_value, username=username
        ).first()
        created = account is None

        if created:
            account = SocialMediaAccount(
                platform=platform_value,
                username=username,
                actor=actor,
            )
            old_data = {}
        else:
            old_data = to_dict(account)
            if account.actor_id != actor.id:
                logger.warning(
                    "Account %s/%s already linked to actor #%s, not #%s",
                    platform,
                    username,
                    account.actor_id,
                    actor.id,
                )

        # Profile fields are refreshed only when this dump is newer.
        should_refresh = collected_at is not None and (
            account.collected_at is None or collected_at > account.collected_at
        )

        update = False
        if should_refresh:
            for field in self.ACCOUNT_PROFILE_FIELDS:
                value = account_data.get(field)
                if value is None:
                    continue
                if not equals(getattr(account, field), value):
                    setattr(account, field, value)
                    update = True

            is_verified = account_data.get("is_verified")
            is_blue_verified = account_data.get("is_blue_verified")
            if is_verified is not None or is_blue_verified is not None:
                new_value = bool(is_verified) or bool(is_blue_verified)
                if not equals(account.is_verified, new_value):
                    account.is_verified = new_value
                    update = True

            if not equals(account.collected_at, collected_at):
                account.collected_at = collected_at
                update = True

        if self.dry_run:
            return account

        if created:
            account.save()
            self.stats.track_created(SocialMediaAccount, account)
        elif update:
            account.save()
            self.stats.track_updated(SocialMediaAccount, old_data, account)

        return account

    # ------------------------------------------------------------------
    # Evidence mentions (category/page tuples)
    # ------------------------------------------------------------------
    def _upsert_mentions(self, evidence, item):
        categories = item.get("categories") or []
        pages = item.get("pages") or []
        if not (categories or pages):
            return

        existing = {(m.category_id, m.page): m for m in evidence.mentions.all()}
        wanted = set()

        for category_name, page in zip(categories, pages, strict=False):
            category_name = category_name.strip()
            if not category_name:
                continue
            category, _ = Category.objects.get_or_create(name=category_name)
            key = (category.id, int(page))
            wanted.add(key)
            if key in existing:
                continue
            self.stats.reset_instance(EvidenceMention)
            mention = EvidenceMention.objects.create(
                evidence=evidence,
                category=category,
                page=key[1],
            )
            self.stats.track_created(EvidenceMention, mention)

        for key, mention in existing.items():
            if key in wanted:
                continue
            self.stats.reset_instance(EvidenceMention)
            mention_id = mention.id
            mention.delete()
            self.stats.track_deleted(EvidenceMention, mention_id)

    # ------------------------------------------------------------------
    # Second-pass reply resolution
    # ------------------------------------------------------------------
    def _resolve_replies(self):
        if self.dry_run or not self._pending_replies:
            return
        for post_id, (reply_id, account_id) in self._pending_replies.items():
            target = self._post_index.get((account_id, reply_id))
            if target is None:
                continue
            self.stats.reset_instance(SocialMediaPost)
            post = SocialMediaPost.objects.get(pk=post_id)
            if post.reply_to_id == target:
                continue
            old_data = to_dict(post)
            post.reply_to_id = target
            post.save(update_fields=["reply_to"])
            self.stats.track_updated(SocialMediaPost, old_data, post)

    # ------------------------------------------------------------------
    # End-of-run relation sweep
    # ------------------------------------------------------------------
    def _seed_all_evidence_relations(self):
        """Re-seed every Evidence touched this run so that cross-references
        between Evidence rows created in the same run pick each other up.

        seed_relations_from_source is idempotent — get_or_create on every row —
        so this is safe to call after the inline per-item seeding."""
        if self.dry_run or not self._post_index:
            return
        post_ids = list(self._post_index.values())
        qs = Evidence.objects.filter(social_media_post_id__in=post_ids).iterator()
        for evidence in qs:
            seed_relations_from_source(evidence)
