import json
import logging
from collections import defaultdict
from datetime import datetime

from django.db import transaction

from froide_evidencecollection.models import (
    Actor,
    Category,
    Evidence,
    EvidenceMention,
    EvidenceSource,
    Person,
    SocialMediaAccount,
    SocialMediaPost,
)

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


def compose_citation(item):
    """Compose the citation text from the post's textual fields."""
    parts = []
    for key in ("title", "text"):
        val = item.get(key)
        if val:
            parts.append(val.strip())
    if item.get("transcription"):
        parts.append("[Transkript]\n" + item["transcription"].strip())
    return "\n\n".join(parts)


def get_or_create_actor(person):
    try:
        return person.actor
    except Actor.DoesNotExist:
        return Actor.objects.create(person=person)


class JSONImporter:
    """
    Import the social-media JSON dump into Evidence + SocialMediaPost rows.

    Expects input that has already been normalized by scripts/prepare_import.py
    (top-level fields named uniformly across platforms, an `account` dict per
    post, and a `references` list for quote/repost relationships).

    Each post becomes one Evidence + one SocialMediaPost (linked via
    EvidenceSource). Profile fields on SocialMediaAccount are updated only when
    the post's `collected_at` is newer than the account's `collected_at`.

    Quote/repost references are materialized inline from each post's
    `references` list (stub accounts/posts via get_or_create). Replies are
    resolved in a second pass over posts inserted during this run.
    """

    def __init__(self, json_path, dry_run=False):
        self.json_path = json_path
        self.dry_run = dry_run
        self.stats = defaultdict(int)
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
                logger.warning(
                    "No Person for hash %s (name=%r); skipping",
                    person_id,
                    entry.get("Name"),
                )
                self.stats["persons_missing"] += 1
                continue
            actor = get_or_create_actor(person)
            for platform, items in (entry.get("social_media") or {}).items():
                if platform not in PLATFORM_MAP:
                    logger.warning("Unknown platform %r; skipping", platform)
                    continue

                for item in items:
                    self._import_item(actor, platform, item, external_id)
                    external_id += 1

        self._resolve_replies()
        return dict(self.stats)

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
            self.stats[f"{platform}_would_import"] += 1
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
            "citation": compose_citation(item),
            "reference_url": item["url"],
            "event_date": posted_at.date() if posted_at else None,
            "publishing_date": posted_at.date() if posted_at else None,
            "documentation_date": collected_at.date() if collected_at else None,
            "posted_by": account,
        }

        existing_post = SocialMediaPost.objects.filter(
            account=account, platform_post_id=platform_post_id
        ).first()
        if existing_post:
            for field, value in post_fields.items():
                setattr(existing_post, field, value)
            existing_post.save()
            post = existing_post
            source = EvidenceSource.objects.filter(social_media_post=post).first()
            evidence = (
                Evidence.objects.filter(source=source).first() if source else None
            )
            if evidence is None:
                evidence = Evidence.objects.create(
                    external_id=external_id, **evidence_fields
                )
                if source is None:
                    source = EvidenceSource.objects.create(social_media_post=post)
                evidence.source = source
                evidence.save(update_fields=["source"])
            else:
                for field, value in evidence_fields.items():
                    setattr(evidence, field, value)
                evidence.save()
            evidence.originators.add(actor)
            self.stats[f"{platform}_updated"] += 1
        else:
            evidence = Evidence.objects.create(
                external_id=external_id, **evidence_fields
            )
            evidence.originators.add(actor)
            post = SocialMediaPost.objects.create(
                account=account,
                platform_post_id=platform_post_id,
                **post_fields,
            )
            source = EvidenceSource.objects.create(social_media_post=post)
            evidence.source = source
            evidence.save(update_fields=["source"])
            self.stats[f"{platform}_imported"] += 1

        self._upsert_mentions(evidence, item)

        self._post_index[(account.id, post.platform_post_id)] = post.id

        # Quote/repost references (inline stub creation).
        for ref in item.get("references") or []:
            stub_post = self._upsert_stub_post(platform, ref)
            if not stub_post:
                continue
            post.references = stub_post
            post.reference_type = REFERENCE_TYPES.get(ref["kind"], "")
            post.save(update_fields=["references", "reference_type"])

        # Replies are resolved in a second pass (target may not yet exist).
        reply_id = self._extract_reply_id(platform, item)
        if reply_id is not None:
            self._pending_replies[post.id] = (str(reply_id), account.id)

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
            self.stats[f"{platform}_stub_accounts_created"] += 1

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
            self.stats[f"{platform}_stub_posts_created"] += 1
            self._post_index[(account.id, post.platform_post_id)] = post.id
        return post

    # ------------------------------------------------------------------
    # Account upsert + profile freshness
    # ------------------------------------------------------------------
    def _upsert_account(self, actor, platform, account_data, collected_at):
        platform_value = PLATFORM_MAP[platform]
        username = account_data["username"]
        account, _ = SocialMediaAccount.objects.get_or_create(
            platform=platform_value,
            username=username,
            defaults={"actor": actor},
        )
        if account.actor_id != actor.id:
            logger.warning(
                "Account %s/%s already linked to actor #%s, not #%s",
                platform,
                username,
                account.actor_id,
                actor.id,
            )

        if not collected_at:
            return account
        if account.collected_at is not None and collected_at <= account.collected_at:
            return account

        for field in (
            "platform_user_id",
            "display_name",
            "description",
            "url",
            "follower_count",
        ):
            value = account_data.get(field)
            if value is not None:
                setattr(account, field, value)

        is_verified = account_data.get("is_verified")
        is_blue_verified = account_data.get("is_blue_verified")
        if is_verified is not None or is_blue_verified is not None:
            account.is_verified = bool(is_verified) or bool(is_blue_verified)

        account.collected_at = collected_at
        if not self.dry_run:
            account.save()
        return account

    # ------------------------------------------------------------------
    # Evidence mentions (category/page triples)
    # ------------------------------------------------------------------
    def _upsert_mentions(self, evidence, item):
        categories = item.get("categories") or []
        pages = item.get("pages") or []
        if not (categories or pages):
            return
        evidence.mentions.all().delete()
        for category_name, page in zip(categories, pages, strict=False):
            category_name = category_name.strip()
            if not category_name:
                continue
            category, _ = Category.objects.get_or_create(name=category_name)
            EvidenceMention.objects.create(
                evidence=evidence,
                category=category,
                page=int(page),
            )
            self.stats["mentions_created"] += 1

    # ------------------------------------------------------------------
    # Second-pass reply resolution
    # ------------------------------------------------------------------
    def _resolve_replies(self):
        if self.dry_run or not self._pending_replies:
            return
        for post_id, (reply_id, account_id) in self._pending_replies.items():
            target = self._post_index.get((account_id, reply_id))
            if target:
                SocialMediaPost.objects.filter(pk=post_id).update(reply_to_id=target)
                self.stats["replies_resolved"] += 1
