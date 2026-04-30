import json
import logging
from collections import defaultdict
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

from django.db import transaction

from froide_evidencecollection.models import (
    Actor,
    Evidence,
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


# Per-platform keys to drop from the raw payload before storing.
# Anything not listed here stays in `SocialMediaPost.raw` for forensic access.
RAW_DROP_KEYS = {
    "telegram": {
        "_",
        "out",
        "mentioned",
        "media_unread",
        "silent",
        "post",
        "from_scheduled",
        "legacy",
        "edit_hide",
        "pinned",
        "noforwards",
        "invert_media",
        "offline",
        "video_processing_pending",
        "from_boosts_applied",
        "via_bot_id",
        "via_business_bot_id",
        "reply_markup",
        "entities",
        "restriction_reason",
        "ttl_period",
        "quick_reply_shortcut_id",
        "effect",
        "factcheck",
        "report_delivery_until_date",
        "paid_message_stars",
        "file_creation_time",
        "action",
        "reactions_are_possible",
    },
    "instagram": {
        "ad_id",
        "boosted_status",
        "boost_unavailable_identifier",
        "boost_unavailable_reason",
        "feed_demotion_control",
        "feed_recs_demotion_control",
        "inventory_source",
        "video_versions",
        "is_dash_eligible",
        "number_of_qualities",
        "video_dash_manifest",
        "image_versions2",
        "sharing_friction_info",
        "sponsor_tags",
        "affiliate_info",
        "organic_tracking_token",
        "story_cta",
        "follow_hashtag_info",
        "comments_disabled",
        "commenting_disabled_for_viewer",
        "like_and_view_counts_disabled",
        "has_liked",
        "top_likers",
        "facepile_top_likers",
        "preview",
        "can_see_insights_as_brand",
        "social_context",
        "can_reshare",
        "can_viewer_reshare",
        "ig_media_sharing_disabled",
        "photo_of_you",
        "media_overlay_info",
        "carousel_parent_id",
        "clips_metadata",
        "clips_attribution_info",
        "audience",
        "media_cropping_info",
        "profile_grid_thumbnail_fitting_style",
        "thumbnails",
        "timeline_pinned_user_ids",
        "upcoming_event",
        "logging_info_token",
        "explore",
        "main_feed_carousel_starting_media_id",
        "is_seen",
        "open_carousel_submission_state",
        "previous_submitter",
        "all_previous_submitters",
        "saved_collection_ids",
        "has_viewer_saved",
        "media_level_comment_controls",
        "__typename",
    },
    "tiktok": {
        "AIGCDescription",
        "CategoryType",
        "authorStats",
        "authorStatsV2",
        "backendSourceEventTracking",
        "collected",
        "contents",
        "digged",
        "diversificationId",
        "duetDisplay",
        "duetEnabled",
        "forFriend",
        "isAd",
        "isReviewing",
        "itemCommentStatus",
        "item_control",
        "officalItem",
        "originalItem",
        "privateItem",
        "secret",
        "shareEnabled",
        "stitchDisplay",
        "stitchEnabled",
        "textTranslatable",
        "challenges",
        "effectStickers",
        "stickersOnItem",
        "imagePost",
        "titleLanguage",
        "titleTranslatable",
        "anchors",
        "poi",
        "itemMute",
        "warnInfo",
        "maskType",
        "aigcLabelType",
        "isPinnedItem",
        "duetInfo",
        "playlistId",
        "BAInfo",
        "adAuthorization",
        "adLabelVersion",
        "brandOrganicType",
        "videoSuggestWordsList",
        "moderationAigcLabelType",
        "isECVideo",
        "HasPromoteEntry",
        "event",
        "IsHDBitrate",
        "creatorAIComment",
    },
    "facebook": {
        "video_files",
    },
    "youtube": set(),
    "twitter": {
        "binding_values",
        "extended_entities",
        "source",
    },
}


# User/author blob field name per platform.
USER_BLOB_FIELD = {
    "facebook": "author",
    "instagram": "user",
    "tiktok": "author",
    "twitter": "user",
    "telegram": None,
    "youtube": None,
}


def _parse_iso(value):
    if not value:
        return None
    try:
        s = value.replace("Z", "+00:00") if isinstance(value, str) else value
        return datetime.fromisoformat(s)
    except (ValueError, TypeError):
        logger.warning("Could not parse ISO datetime: %r", value)
        return None


def _parse_epoch(value):
    if value in (None, ""):
        return None
    try:
        return datetime.fromtimestamp(int(float(value)), tz=timezone.utc)
    except (ValueError, TypeError, OSError):
        logger.warning("Could not parse epoch: %r", value)
        return None


def _parse_twitter_creation(value):
    if not value:
        return None
    try:
        return parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None


def _coerce_int(value):
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _strip(d, drop_keys):
    return {k: v for k, v in d.items() if k not in drop_keys}


# ---------------------------------------------------------------------------
# Per-platform extractors. Each returns a dict of normalized fields:
#   platform_post_id, posted_at, edited_at, scraped_at,
#   text, title, description, caption, transcription,
#   view_count, like_count, comment_count, share_count, reactions,
#   foreign_reply_to, foreign_reference, foreign_reference_type,
#   user_blob (raw dict or None), profile (normalized account-profile dict or None)
# ---------------------------------------------------------------------------


def _telegram_ref(blob):
    """Stub spec from a Telegram fwd_from blob, or None.

    Telegram forwards usually carry only a numeric channel id, not a username,
    so we can only materialize a stub when the scraper preserved a username.
    """
    if not isinstance(blob, dict):
        return None
    channel_post = blob.get("channel_post")
    if not channel_post:
        return None
    username = (
        blob.get("from_username")
        or blob.get("channel_username")
        or blob.get("username")
        or "<UNKOWN>"
    )
    if not username:
        return None
    from_id = blob.get("from_id")
    if isinstance(from_id, dict):
        channel_id = from_id.get("channel_id") or from_id.get("user_id") or ""
    else:
        channel_id = from_id or ""
    return {
        "kind": "repost",
        "platform_post_id": str(channel_post),
        "url": f"https://t.me/{username}/{channel_post}",
        "posted_at": _parse_iso(blob.get("date")),
        "text": "",
        "account": {
            "username": str(username),
            "platform_user_id": str(channel_id),
            "display_name": blob.get("from_name") or blob.get("post_author") or "",
        },
        "raw": blob,
    }


def _extract_telegram(item):
    replies = item.get("replies") or {}
    fwd = item.get("fwd_from") or {}
    references = []
    ref = _telegram_ref(fwd)
    if ref:
        references.append(ref)
    return {
        "platform_post_id": str(item.get("message_id") or item.get("id") or ""),
        "posted_at": _parse_iso(item.get("date")),
        "edited_at": _parse_iso(item.get("edit_date")),
        "scraped_at": None,
        "text": item.get("message") or "",
        "title": "",
        "description": "",
        "caption": "",
        "transcription": "",
        "view_count": _coerce_int(item.get("views")),
        "like_count": None,
        "comment_count": _coerce_int(replies.get("replies")),
        "share_count": _coerce_int(item.get("forwards")),
        "reactions": item.get("reactions"),
        "foreign_reply_to": (item.get("reply_to") or {}).get("reply_to_msg_id"),
        "foreign_reference": (item.get("fwd_from") or {}).get("from_id")
        if item.get("fwd_from")
        else None,
        "foreign_reference_type": SocialMediaPost.ReferenceType.REPOST
        if item.get("fwd_from")
        else "",
        "user_blob": None,
        "profile": None,
        "references": references,
    }


def _extract_instagram(item):
    caption = item.get("caption")
    caption_text = caption.get("text") if isinstance(caption, dict) else (caption or "")
    user = item.get("user") or {}
    profile = None
    if user:
        profile = {
            "platform_user_id": str(user.get("pk") or user.get("id") or ""),
            "display_name": user.get("full_name") or "",
            "bio": "",
            "profile_url": (
                f"https://www.instagram.com/{user['username']}/"
                if user.get("username")
                else ""
            ),
            "is_verified": user.get("is_verified"),
            "follower_count": None,
        }
    return {
        "platform_post_id": str(item.get("pk") or item.get("code") or ""),
        "posted_at": _parse_epoch(item.get("taken_at")),
        "edited_at": None,
        "scraped_at": _parse_epoch(item.get("scraped_date")),
        "text": "",
        "title": item.get("title") or "",
        "description": "",
        "caption": caption_text or "",
        "transcription": item.get("video_transcription") or "",
        "view_count": _coerce_int(item.get("view_count")),
        "like_count": _coerce_int(item.get("like_count")),
        "comment_count": _coerce_int(item.get("comment_count")),
        "share_count": None,
        "reactions": None,
        "foreign_reply_to": None,
        "foreign_reference": None,
        "foreign_reference_type": "",
        "user_blob": user or None,
        "profile": profile,
    }


def _extract_tiktok(item):
    stats = item.get("stats") or {}
    author = item.get("author") or {}
    profile = None
    if author:
        profile = {
            "platform_user_id": str(author.get("id") or author.get("secUid") or ""),
            "display_name": author.get("nickname") or "",
            "bio": author.get("signature") or "",
            "profile_url": (
                f"https://www.tiktok.com/@{author['uniqueId']}"
                if author.get("uniqueId")
                else ""
            ),
            "is_verified": author.get("verified"),
            "follower_count": None,
        }
    return {
        "platform_post_id": str(item.get("vid_id") or item.get("id") or ""),
        "posted_at": _parse_epoch(item.get("createTime")),
        "edited_at": None,
        "scraped_at": None,
        "text": item.get("desc") or "",
        "title": "",
        "description": "",
        "caption": "",
        "transcription": item.get("text") or "",
        "view_count": _coerce_int(stats.get("playCount")),
        "like_count": _coerce_int(stats.get("diggCount")),
        "comment_count": _coerce_int(stats.get("commentCount")),
        "share_count": _coerce_int(stats.get("shareCount")),
        "reactions": None,
        "foreign_reply_to": None,
        "foreign_reference": None,
        "foreign_reference_type": "",
        "user_blob": author or None,
        "profile": profile,
    }


def _facebook_ref(blob):
    """Stub spec from a Facebook attached_post blob, or None."""
    if not isinstance(blob, dict):
        return None
    post_id = blob.get("post_id")
    if not post_id:
        return None
    author = blob.get("author") or {}
    username = author.get("name") or ""
    if not username and author.get("url"):
        url = author["url"].rstrip("/")
        tail = url.rsplit("/", 1)[-1] if "/" in url else ""
        # Skip numeric "profile.php?id=..." tails — not stable usernames.
        if tail and "?" not in tail and not tail.isdigit():
            username = tail
    if not username:
        return None
    return {
        "kind": "repost",
        "platform_post_id": str(post_id),
        "url": blob.get("url") or "",
        "posted_at": _parse_epoch(blob.get("timestamp")),
        "text": blob.get("message") or "",
        "account": {
            "username": str(username),
            "platform_user_id": str(author.get("id") or ""),
            "display_name": author.get("name") or "",
        },
        "raw": blob,
    }


def _extract_facebook(item):
    reactions = item.get("reactions") or {}
    like = reactions.get("like") if isinstance(reactions, dict) else None
    author = item.get("author") or {}
    profile = None
    if author:
        profile = {
            "platform_user_id": str(author.get("id") or ""),
            "display_name": author.get("name") or "",
            "bio": "",
            "profile_url": author.get("url") or "",
            "is_verified": None,
            "follower_count": None,
        }
    scraped = item.get("scraped_date") or item.get("date_collected")
    attached = item.get("attached_post") or {}
    references = []
    ref = _facebook_ref(attached)
    if ref:
        references.append(ref)
    return {
        "platform_post_id": str(item.get("post_id") or ""),
        "posted_at": _parse_epoch(item.get("timestamp")),
        "edited_at": None,
        "scraped_at": _parse_epoch(scraped) if scraped else None,
        "text": item.get("message") or "",
        "title": "",
        "description": "",
        "caption": "",
        "transcription": "",
        "view_count": None,
        "like_count": _coerce_int(like),
        "comment_count": None,
        "share_count": None,
        "reactions": reactions or None,
        "foreign_reply_to": None,
        "foreign_reference": attached.get("post_id") if attached else None,
        "foreign_reference_type": SocialMediaPost.ReferenceType.REPOST
        if attached.get("post_id")
        else "",
        "user_blob": author or None,
        "profile": profile,
        "references": references,
    }


def _extract_youtube(item):
    return {
        "platform_post_id": str(item.get("video_id") or ""),
        "posted_at": _parse_iso(item.get("published_at")),
        "edited_at": None,
        "scraped_at": None,
        "text": "",
        "title": item.get("title") or "",
        "description": item.get("description") or "",
        "caption": "",
        "transcription": item.get("transcription") or "",
        "view_count": _coerce_int(item.get("view_count")),
        "like_count": _coerce_int(item.get("like_count")),
        "comment_count": None,
        "share_count": None,
        "reactions": None,
        "foreign_reply_to": None,
        "foreign_reference": None,
        "foreign_reference_type": "",
        "user_blob": None,
        "profile": None,
    }


def _twitter_ref(kind, blob):
    """Turn a nested twitter status blob into a reference spec, or None."""
    if not isinstance(blob, dict):
        return None
    tweet_id = blob.get("tweet_id") or blob.get("id_str") or blob.get("id")
    if not tweet_id:
        return None
    user = blob.get("user") or {}
    username = user.get("username") or user.get("screen_name") or ""
    posted = _parse_twitter_creation(blob.get("creation_date")) or _parse_epoch(
        blob.get("timestamp")
    )
    return {
        "kind": kind,
        "platform_post_id": str(tweet_id),
        "url": (f"https://x.com/{username}/status/{tweet_id}" if username else ""),
        "posted_at": posted,
        "text": blob.get("text") or "",
        "account": {
            "username": username,
            "platform_user_id": str(user.get("user_id") or ""),
            "display_name": user.get("name") or "",
        }
        if username
        else None,
        "raw": blob,
    }


def _extract_twitter(item):
    user = item.get("user") or {}
    profile = None
    if user:
        profile = {
            "platform_user_id": str(user.get("user_id") or ""),
            "display_name": user.get("name") or "",
            "bio": user.get("description") or "",
            "profile_url": (
                f"https://x.com/{user['username']}" if user.get("username") else ""
            ),
            "is_verified": user.get("is_blue_verified") or user.get("is_verified"),
            "follower_count": _coerce_int(user.get("follower_count")),
        }
    posted = _parse_twitter_creation(item.get("creation_date")) or _parse_epoch(
        item.get("timestamp")
    )
    retweet = _coerce_int(item.get("retweet_count")) or 0
    quote = _coerce_int(item.get("quote_count")) or 0
    share = (
        retweet + quote
        if (item.get("retweet_count") or item.get("quote_count"))
        else None
    )
    references = []
    for kind, key in (
        ("quote", "quoted_status"),
        ("repost", "retweeted_status"),
    ):
        ref = _twitter_ref(kind, item.get(key))
        if ref:
            references.append(ref)
    if item.get("quoted_status_id"):
        foreign_reference = item.get("quoted_status_id")
        foreign_reference_type = SocialMediaPost.ReferenceType.QUOTE
    elif item.get("retweet_tweet_id"):
        foreign_reference = item.get("retweet_tweet_id")
        foreign_reference_type = SocialMediaPost.ReferenceType.REPOST
    else:
        foreign_reference = None
        foreign_reference_type = ""
    return {
        "platform_post_id": str(item.get("tweet_id") or ""),
        "posted_at": posted,
        "edited_at": None,
        "scraped_at": _parse_epoch(item.get("scraped_date")),
        "text": item.get("text") or "",
        "title": "",
        "description": "",
        "caption": "",
        "transcription": "",
        "view_count": _coerce_int(item.get("views")),
        "like_count": _coerce_int(item.get("favorite_count")),
        "comment_count": _coerce_int(item.get("reply_count")),
        "share_count": share,
        "reactions": None,
        "foreign_reply_to": item.get("in_reply_to_status_id"),
        "foreign_reference": foreign_reference,
        "foreign_reference_type": foreign_reference_type,
        "user_blob": user or None,
        "profile": profile,
        "references": references,
    }


EXTRACTORS = {
    "telegram": _extract_telegram,
    "instagram": _extract_instagram,
    "tiktok": _extract_tiktok,
    "facebook": _extract_facebook,
    "youtube": _extract_youtube,
    "twitter": _extract_twitter,
}


def compose_citation(extracted):
    """Compose the citation text from the extracted textual fields."""
    parts = []
    for key in ("title", "caption", "text", "description"):
        val = extracted.get(key)
        if val:
            parts.append(val.strip())
    if extracted.get("transcription"):
        parts.append("[Transkript]\n" + extracted["transcription"].strip())
    return "\n\n".join(parts)


def get_or_create_actor(person):
    try:
        return person.actor
    except Actor.DoesNotExist:
        return Actor.objects.create(person=person)


class JSONImporter:
    """
    Import the AfD social-media JSON dump into Evidence + SocialMediaPost rows.

    Each post becomes one Evidence + one SocialMediaPost (linked via
    EvidenceSource). Profile fields on SocialMediaAccount are updated only when
    the post's scrape date is newer than the account's `profile_retrieved_at`.

    Reply and reference (quote/repost) relationships are resolved in a second
    pass over posts inserted during this run (cross-import resolution would be
    additive).
    """

    def __init__(self, json_path, dry_run=False):
        self.json_path = json_path
        self.dry_run = dry_run
        self.stats = defaultdict(int)
        # (account_id, platform_post_id) -> SocialMediaPost.id
        self._post_index = {}
        # SocialMediaPost.id -> (foreign_reply_to, foreign_reference, foreign_reference_type, account_id)
        self._pending_links = {}

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
                    "No Person for hash %s (label=%r); skipping",
                    person_id,
                    entry.get("label"),
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
                    external_id = external_id + 1

        self._resolve_links()
        return dict(self.stats)

    # ------------------------------------------------------------------
    # Per-item import
    # ------------------------------------------------------------------
    def _import_item(self, actor, platform, item, external_id):
        # Warn on unexpected non-null Kommentar values.
        kommentar = item.get("Kommentar")
        if kommentar:
            logger.warning(
                "Non-null Kommentar for actor=%s platform=%s url=%s: %r",
                actor,
                platform,
                item.get("url_corrected"),
                kommentar,
            )

        username = (
            item.get("username")
            or item.get("username_y")
            or item.get("username_x")
            or ""
        ).strip()
        if not username:
            logger.warning("No username for %s item; skipping", platform)
            self.stats["items_missing_username"] += 1
            return

        extracted = EXTRACTORS[platform](item)
        if not extracted["platform_post_id"]:
            logger.warning("No post id for %s/%s; skipping", platform, username)
            self.stats["items_missing_post_id"] += 1
            return

        account = self._upsert_account(actor, platform, username, extracted)
        post_url = (
            item.get("url_corrected") or item.get("url_y") or item.get("url_x") or ""
        )

        if self.dry_run:
            self.stats[f"{platform}_would_import"] += 1
            return

        raw_payload = _strip(item, RAW_DROP_KEYS.get(platform, set()))
        post_fields = {
            "url": post_url,
            "posted_at": extracted["posted_at"],
            "edited_at": extracted["edited_at"],
            "text": extracted["text"],
            "title": extracted["title"],
            "description": extracted["description"],
            "caption": extracted["caption"],
            "transcription": extracted["transcription"],
            "view_count": extracted["view_count"],
            "like_count": extracted["like_count"],
            "comment_count": extracted["comment_count"],
            "share_count": extracted["share_count"],
            "reactions": extracted["reactions"],
            "user_snapshot": extracted["user_blob"],
            "raw": raw_payload,
        }
        evidence_fields = {
            "citation": compose_citation(extracted),
            "reference_url": post_url,
            "event_date": extracted["posted_at"].date()
            if extracted["posted_at"]
            else None,
            "publishing_date": extracted["posted_at"].date()
            if extracted["posted_at"]
            else None,
            "documentation_date": extracted["scraped_at"].date()
            if extracted["scraped_at"]
            else None,
            "posted_by": account,
        }

        existing_post = SocialMediaPost.objects.filter(
            account=account, platform_post_id=extracted["platform_post_id"]
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
                platform_post_id=extracted["platform_post_id"],
                **post_fields,
            )
            source = EvidenceSource.objects.create(social_media_post=post)
            evidence.source = source
            evidence.save(update_fields=["source"])
            self.stats[f"{platform}_imported"] += 1

        self._post_index[(account.id, post.platform_post_id)] = post.id
        self._pending_links[post.id] = (
            extracted["foreign_reply_to"],
            extracted["foreign_reference"],
            extracted["foreign_reference_type"],
            account.id,
        )

        link_updates = {}
        for ref in extracted.get("references") or []:
            stub = self._upsert_stub_post(platform, ref)
            if not stub:
                continue
            if ref["kind"] == "reply":
                link_updates["reply_to"] = stub
            elif ref["kind"] in (
                SocialMediaPost.ReferenceType.QUOTE,
                SocialMediaPost.ReferenceType.REPOST,
            ):
                link_updates["references"] = stub
                link_updates["reference_type"] = ref["kind"]
        if link_updates:
            for field, value in link_updates.items():
                setattr(post, field, value)
            post.save(update_fields=list(link_updates.keys()))

    # ------------------------------------------------------------------
    # Stub account/post upsert (referenced but not directly scraped)
    # ------------------------------------------------------------------
    def _upsert_stub_post(self, platform, ref):
        platform_value = PLATFORM_MAP[platform]
        acct_data = ref.get("account") or {}
        username = (acct_data.get("username") or "").strip()
        if not username or not ref.get("platform_post_id"):
            return None

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

        raw_blob = ref.get("raw")
        raw_payload = (
            _strip(raw_blob, RAW_DROP_KEYS.get(platform, set()))
            if isinstance(raw_blob, dict)
            else {}
        )
        post, created_post = SocialMediaPost.objects.get_or_create(
            account=account,
            platform_post_id=ref["platform_post_id"],
            defaults={
                "url": ref.get("url") or "",
                "posted_at": ref.get("posted_at"),
                "text": ref.get("text") or "",
                "raw": raw_payload,
            },
        )
        if created_post:
            self.stats[f"{platform}_stub_posts_created"] += 1
            self._post_index[(account.id, post.platform_post_id)] = post.id
        return post

    # ------------------------------------------------------------------
    # Account upsert + profile freshness
    # ------------------------------------------------------------------
    def _upsert_account(self, actor, platform, username, extracted):
        platform_value = PLATFORM_MAP[platform]
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

        profile = extracted.get("profile")
        scraped_at = extracted.get("scraped_at")
        if (
            profile
            and scraped_at
            and (
                account.profile_retrieved_at is None
                or scraped_at > account.profile_retrieved_at
            )
        ):
            for field in (
                "platform_user_id",
                "display_name",
                "bio",
                "profile_url",
            ):
                value = profile.get(field)
                if value is not None:
                    setattr(account, field, value)
            if profile.get("is_verified") is not None:
                account.is_verified = profile["is_verified"]
            if profile.get("follower_count") is not None:
                account.follower_count = profile["follower_count"]
            account.profile_retrieved_at = scraped_at
            if not self.dry_run:
                account.save()
        return account

    # ------------------------------------------------------------------
    # Second-pass link resolution
    # ------------------------------------------------------------------
    def _resolve_links(self):
        if self.dry_run or not self._pending_links:
            return
        for post_id, (
            reply_id,
            reference_id,
            reference_type,
            account_id,
        ) in self._pending_links.items():
            updates = {}
            if reply_id:
                target = self._post_index.get((account_id, str(reply_id)))
                if target:
                    updates["reply_to_id"] = target
            if reference_id:
                target = self._lookup_post(str(reference_id))
                if target:
                    updates["references_id"] = target
                    updates["reference_type"] = reference_type
            if updates:
                SocialMediaPost.objects.filter(pk=post_id).update(**updates)
                self.stats["links_resolved"] += 1

    def _lookup_post(self, platform_post_id):
        # Cross-account lookup (quotes/reposts can reference any account).
        for (_, ppid), pid in self._post_index.items():
            if ppid == platform_post_id:
                return pid
        return None
