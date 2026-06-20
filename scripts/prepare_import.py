#!/usr/bin/env python3
import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path


def load(path: Path) -> dict:
    with path.open() as f:
        return json.load(f)


def transform_telegram_post(post: dict) -> dict:
    # media
    media = post.get("media")

    if isinstance(media, dict):
        document = media.get("document")
        if isinstance(document, dict):
            media["document"].pop("file_reference")
            media["document"].pop("thumbs")

        photo = media.get("photo")
        if isinstance(photo, dict):
            media["photo"].pop("file_reference")
            media["photo"].pop("sizes")

        if "webpage" in media:
            photo = media["webpage"].get("photo")
            if isinstance(photo, dict):
                media["webpage"]["photo"].pop("file_reference")
                media["webpage"]["photo"].pop("sizes")

    post["media"] = media

    # reactions
    reactions = post.get("reactions")
    if isinstance(reactions, dict):
        post["reactions"] = {
            r["reaction"]["emoticon"]: r["count"] for r in reactions["results"]
        }

    # counts
    post["view_count"] = _coerce_int(post.pop("views", None))
    post["share_count"] = _coerce_int(post.pop("forwards", None))
    # Keep replies dict around - may be interesting to inspect.
    replies = post.get("replies", None)
    post["comment_count"] = (
        _coerce_int(replies.get("replies")) if isinstance(replies, dict) else None
    )

    # account
    username = post.pop("username")
    post["account"] = {
        "username": username,
        "platform_user_id": post.pop("channel_id"),
        "display_name": post.pop("post_author"),
        "url": f"https://t.me/{username}",
    }

    ## references
    references = []
    blob = post.pop("fwd_from")
    if isinstance(blob, dict):
        if isinstance(blob["from_id"], dict):
            if blob["from_id"].get("channel_id"):
                channel_id = blob["from_id"]["channel_id"]
                ref = {
                    "platform_post_id": blob["channel_post"],
                    "url": f"https://t.me/c/{channel_id}/{blob["channel_post"]}",
                    "created_at": blob["date"],
                    "account": {
                        "platform_user_id": channel_id,
                    },
                }
                references.append(ref)
            elif blob["from_id"].get("user_id"):
                user_id = blob["from_id"]["user_id"]
                ref = {
                    "created_at": blob["date"],
                    "account": {
                        "platform_user_id": user_id,
                    },
                }
                references.append(ref)
        else:
            username = blob["from_name"]
            ref = {
                "created_at": blob["date"],
                "account": {
                    "username": username,
                },
            }
            references.append(ref)

    post["references"] = references

    return post


def transform_instagram_post(post: dict) -> dict:
    # text
    caption = post.pop("caption")
    if isinstance(caption, dict):
        post["text"] = caption["text"]

    # created_at / collected_at
    post["created_at"] = _parse_epoch(post.pop("taken_at"))
    post["collected_at"] = _parse_epoch(post.pop("scraped_date"))

    # account
    user = post.pop("user")
    post["account"] = {
        "username": user["username"],
        "platform_user_id": user["pk"],
        "display_name": user["full_name"],
        "url": f"https://www.instagram.com/{user["username"]}",
        "is_verified": user["is_verified"],
    }

    return post


def transform_facebook_post(post: dict) -> dict:
    # created_at / collected_at
    post["created_at"] = _parse_epoch(post.pop("timestamp"))
    post["collected_at"] = _parse_epoch(post.pop("scraped_date"))

    # counts
    post["comment_count"] = _coerce_int(post.pop("comments_count"))
    post["share_count"] = _coerce_int(post.pop("reshare_count"))
    # like_count from reactions
    reactions = post.get("reactions")
    if isinstance(reactions, dict):
        post["like_count"] = _coerce_int(reactions.get("like"))

    # account
    author = post.pop("author")
    url = author["url"]
    username = None
    if "people/" not in url:
        username = url.replace("https://www.facebook.com/", "")

    post["account"] = {
        "username": username,
        "platform_user_id": author["id"],
        "display_name": author["name"],
        "url": url,
    }

    ## references
    references = []
    blob = post.pop("attached_post")
    if isinstance(blob, dict):
        ref_author = blob["author"]
        ref = {
            "platform_post_id": blob["post_id"],
            "url": blob["url"],
            "created_at": _parse_epoch(blob["timestamp"]),
            "text": blob["message"],
            "account": {
                "platform_user_id": ref_author["id"],
                "display_name": ref_author["name"],
            },
        }
        references.append(ref)

    post["references"] = references

    return post


def transform_tiktok_post(post: dict) -> dict:
    # created_at
    post["created_at"] = _parse_epoch(post.pop("createTime"))

    # counts (from statsV2)
    stats = post.pop("statsV2") or {}
    post["view_count"] = _coerce_int(stats.get("playCount"))
    post["like_count"] = _coerce_int(stats.get("diggCount"))
    post["comment_count"] = _coerce_int(stats.get("commentCount"))
    post["share_count"] = _coerce_int(stats.get("shareCount"))

    # account
    author = post.pop("author")
    author_stats = post.get("authorStats") or {}
    post["account"] = {
        "username": author["uniqueId"],
        "platform_user_id": author["id"],
        "display_name": author["nickname"],
        "url": f"https://www.tiktok.com/@{author["uniqueId"]}",
        "description": author["signature"],
        "is_verified": author["verified"],
        "follower_count": _coerce_int(author_stats.get("followerCount")),
    }

    return post


def transform_youtube_post(post: dict) -> dict:
    # counts
    post["view_count"] = _coerce_int(post.pop("viewCount"))
    post["like_count"] = _coerce_int(post.pop("likeCount"))
    post["comment_count"] = _parse_count_text(post.pop("commentCountText"))
    post["is_comment_disabled"] = post.pop("isCommentDisabled")

    # account
    channel = post.pop("channel")
    post["account"] = {
        "username": channel["handle"],
        "platform_user_id": channel["id"],
        "display_name": channel["name"],
        "url": f"https://www.youtube.com/channel/{channel["id"]}",
        # `isVerified` is the general verified badge (matches other platforms);
        # `isVerifiedArtist` is YouTube's separate music-artist badge, which the
        # account model has no field for, so it is dropped.
        "is_verified": channel.get("isVerified"),
        "follower_count": _parse_count_text(channel.get("subscriberCountText")),
    }

    return post


def _twitter_account(user: dict) -> dict:
    return {
        "username": user["username"],
        "platform_user_id": user["user_id"],
        "display_name": user["name"],
        "url": f"https://x.com/{user["username"].lower()}",
        "description": user["description"],
        "is_verified": user["is_verified"],
        "is_blue_verified": user["is_blue_verified"],
        "follower_count": _coerce_int(user.get("follower_count")),
        "created_at": _parse_epoch(user["timestamp"]),
        "location": user["location"],
        "external_url": user["external_url"],
    }


def transform_twitter_post(post: dict) -> dict:
    # created_at /collected_at
    post["created_at"] = _parse_epoch(post.pop("timestamp"))
    post["collected_at"] = _parse_epoch(post.pop("scraped_date", None))

    # counts
    post["view_count"] = _coerce_int(post.pop("views", None))
    post["like_count"] = _coerce_int(post.pop("favorite_count", None))
    post["comment_count"] = _coerce_int(post.pop("reply_count", None))
    retweet = _coerce_int(post.pop("retweet_count", None))
    quote = _coerce_int(post.pop("quote_count", None))
    post["share_count"] = (retweet or 0) + (quote or 0) if (retweet or quote) else None

    # account
    post["account"] = _twitter_account(post.pop("user"))

    ## references
    references = []
    for key in ["quoted_status", "retweet_status"]:
        blob = post.pop(key, None)
        if isinstance(blob, dict):
            user = blob["user"]
            ref = {
                "platform_post_id": blob["tweet_id"],
                "url": f"https://x.com/{user["username"]}/status/{blob["tweet_id"]}",
                "created_at": _parse_epoch(blob["timestamp"]),
                "text": blob["text"],
                "account": _twitter_account(user),
            }
            references.append(ref)

    post["references"] = references

    return post


PLATFORM_CONFIG: dict[str, dict] = {
    "all": {
        "keep": {
            "report_data",
            "image_file",
            "video_file",
            "srt_file",
            "screenshot_file",
            "url",
        },
        "rename": {
            "url_corrected": "url",
            "image_alternativ": "image_alt_text",
        },
        "discard": set(),
    },
    # https://core.telegram.org/constructor/message
    # Worth considering: silent (false/true), edit_hide, invert_media (for display)
    "telegram": {
        "keep": {
            "username",
            "fwd_from",
            "reply_to",
            "media",
            "reply_markup",
            "entities",
            "views",
            "forwards",
            "replies",
            "post_author",
            "grouped_id",
            "reactions",
            "channel_id",
        },
        "rename": {
            "date": "created_at",
            "message": "text",
            "edit_date": "edited_at",
            "message_id": "platform_post_id",
        },
        "discard": {
            "_",
            "id",  # use message_id
            "peer_id",  # duplicates info about channel_id
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
            "from_id",
            "from_boosts_applied",
            "saved_peer_id",
            "via_bot_id",
            "via_business_bot_id",
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
            "username_y",
        },
        "transform": transform_telegram_post,
    },
    "instagram": {
        "keep": {
            "caption",
            "taken_at",
            "original_height",
            "original_width",
            "user",
            "coauthor_producers",
            "comment_count",
            "like_count",
            "product_type",
            "media_type",
            "usertags",
            "carousel_media",
            "location",
            "has_audio",
            "scraped_date",
        },
        "rename": {
            "code": "platform_post_id",
            "video_transcription": "transcription",
        },
        "discard": {
            "username",
            "pk",
            "id",
            "ad_id",
            "boosted_status",
            "boost_unavailable_identifier",
            "boost_unavailable_reason",
            "caption_is_edited",
            "feed_demotion_control",
            "feed_recs_demotion_control",
            "inventory_source",
            "video_versions",
            "is_dash_eligible",
            "number_of_qualities",
            "video_dash_manifest",
            "image_versions2",
            "sharing_friction_info",
            "is_paid_partnership",
            "sponsor_tags",
            "affiliate_info",
            "organic_tracking_token",
            "link",
            "story_cta",
            "group",
            "owner",
            "invited_coauthor_producers",
            "follow_hashtag_info",
            "title",  # is always null
            "comments_disabled",
            "commenting_disabled_for_viewer",
            "like_and_view_counts_disabled",
            "has_liked",
            "top_likers",
            "facepile_top_likers",
            "preview",
            "can_see_insights_as_brand",
            "social_context",
            "view_count",  # is always null
            "can_reshare",
            "can_viewer_reshare",
            "ig_media_sharing_disabled",
            "photo_of_you",
            "media_overlay_info",
            "carousel_parent_id",
            "carousel_media_count",
            "clips_metadata",
            "clips_attribution_info",
            "accessibility_caption",
            "audience",
            "display_uri",
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
            "headline",
            "comments",
            "saved_collection_ids",
            "has_viewer_saved",
            "media_level_comment_controls",
            "__typename",
            "fb_like_count",
        },
        "transform": transform_instagram_post,
    },
    # Worth considering: type (srt/vtt), video (height/width/duration etc.), challenges, anchors?, videoSuggestWordsList?
    "tiktok": {
        "keep": {
            "author",
            "authorStats",
            "contents",
            "createTime",
            "statsV2",
            "effectStickers",
            "stickersOnItem",
            "creatorAIComment",
        },
        "rename": {
            "text": "transcription",
            "vid_id": "platform_post_id",
            "desc": "text",
            "textLanguage": "language",
            "poi": "location",
        },
        "discard": {
            "username",
            "type",
            "AIGCDescription",
            "CategoryType",
            "authorStatsV2",
            "backendSourceEventTracking",
            "collected",
            "contents",
            "digged",
            "diversificationId",
            "duetDisplay",
            "duetEnabled",
            "forFriend",
            "id",  # same as vid_id
            "isAd",
            "isReviewing",
            "itemCommentStatus",
            "item_control",
            "music",
            "officalItem",
            "originalItem",
            "privateItem",
            "secret",
            "shareEnabled",
            "stats",
            "stitchDisplay",
            "stitchEnabled",
            "textTranslatable",
            "challenges",
            "effectStickers",
            "stickersOnItem",
            "imagePost",
            "titleLanguage",
            "titleTranslatable",
            "video",
            "textExtra",  # also in contents
            "anchors",
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
            "username_y",
            "music_id",
            "commentCount",  # also in stats
            "HasPromoteEntry",
            "event",
            "IsHDBitrate",
        },
        "transform": transform_tiktok_post,
    },
    "facebook": {
        "keep": {
            "attached_post",
            "timestamp",
            "author",
            "video",
            "image",
            "reactions",
            "scraped_date",
            "external_url",
            "comments_count",
            "reshare_count",
        },
        "rename": {
            "post_id": "platform_post_id",
            "message": "text",
        },
        "discard": {
            "username",
            "video_files",
            "attached_post_url",
            "date",
            "date_collected",
            "type",
            "message_rich",
            "author_title",
            "album_preview",
            "video_thumbnail",
            "attached_event",
            "text_format_metadata",
            "comments_id",
            "shares_id",
            "author_id_param",
            "reactions_count",
        },
        "transform": transform_facebook_post,
    },
    "youtube": {
        "keep": {
            "channel",
            "title",
            "description",
            "transcription",
            "viewCount",
            "likeCount",
            "commentCountText",
            "isCommentDisabled",
        },
        "rename": {
            "id": "platform_post_id",
            "publishedTime": "created_at",
            # The YouTube video description is often promotional boilerplate /
            # links and is rarely about the video's content, so it stays in
            # `description` (low topic priority) rather than the primary `text`
            # body — the title + transcript carry the content.
        },
        "discard": {
            "errorId",
            "type",
            "lengthSeconds",
            "publishedTimeText",
            "isLiveStream",
            "isLiveNow",
            "isRegionRestricted",
            "isUnlisted",
            "thumbnails",
            "musicCredits",  # interesting in a way ...
            "videos",
            "audios",
            "subtitles",
            "related",  # could be interesting
            "reason",
        },
        "transform": transform_youtube_post,
    },
    "twitter": {
        "keep": {
            "text",
            "media_url",
            "video_url",
            "user",
            "language",
            "favorite_count",
            "retweet_count",
            "reply_count",
            "quote_count",
            "retweet",
            "views",
            "timestamp",
            "in_reply_to_status_id",
            "quoted_status_id",
            "binding_values",
            "expanded_url",
            "retweet_tweet_id",
            "extended_entities",
            "conversation_id",
            "retweet_status",
            "quoted_status",
            "bookmark_count",
            "source",
            "scraped_date",
        },
        "rename": {
            "tweet_id": "platform_post_id",
        },
        "discard": {
            "username",
            "creation_date",
            "video_view_count",
            "community_note",
            "detail",
        },
        "transform": transform_twitter_post,
    },
}

_warned_unknown: set[tuple[str, str]] = set()


def _parse_epoch(value):
    if value in (None, ""):
        return None
    try:
        return datetime.fromtimestamp(int(float(value)), tz=timezone.utc).isoformat()
    except (ValueError, TypeError, OSError):
        # logger.warning("Could not parse epoch: %r", value)
        return None


def _coerce_int(value):
    if value in (None, ""):
        return None
    try:
        # via float() so float-like strings ("568.0") and floats (568.0) coerce too
        return int(float(value))
    except (ValueError, TypeError):
        return None


_COUNT_SUFFIXES = {"": 1, "K": 1_000, "M": 1_000_000, "B": 1_000_000_000}


def _parse_count_text(value):
    """Parse human-readable counts like "14.8K subscribers" -> 14800."""
    if not isinstance(value, str):
        return None
    match = re.search(r"([\d.,]+)\s*([KMB]?)", value, re.IGNORECASE)
    if not match:
        return None
    number, suffix = match.groups()
    try:
        number = float(number.replace(",", ""))
    except ValueError:
        return None
    return int(number * _COUNT_SUFFIXES[suffix.upper()])


def filter_fields(post: dict, platform: str, config: dict) -> dict:
    all_config = PLATFORM_CONFIG.get(
        "all", {"keep": set(), "rename": {}, "discard": set()}
    )

    keep = config["keep"] | all_config["keep"]
    rename = {**all_config["rename"], **config["rename"]}
    discard = config["discard"] | all_config["discard"]

    result = {}
    for key, value in post.items():
        if key in keep:
            result[key] = value
        elif key in rename:
            result[rename[key]] = value
        elif key in discard:
            continue
        else:
            if (platform, key) not in _warned_unknown:
                _warned_unknown.add((platform, key))
                print(
                    f"warning: unknown {platform} field {key!r} "
                    f"(add to keep, rename, or discard)",
                    file=sys.stderr,
                )
    return result


def diff_posts(kept: dict, dup: dict) -> list[str]:
    diffs = []
    for key in sorted(set(kept) | set(dup)):
        if key not in kept:
            diffs.append(f"  + {key}: {dup[key]!r}")
        elif key not in dup:
            diffs.append(f"  - {key}: {kept[key]!r}")
        elif kept[key] != dup[key]:
            diffs.append(f"  ~ {key}: {kept[key]!r} != {dup[key]!r}")
    return diffs


def dedupe_posts(posts: list) -> list:
    by_url = {}
    unique = []
    for post in posts:
        url = post["url_corrected"]
        if url in by_url:
            diffs = diff_posts(by_url[url], post)
            if diffs:
                print(
                    f"warning: duplicate url_corrected {url!r} differs:",
                    file=sys.stderr,
                )
                for line in diffs:
                    print(line, file=sys.stderr)
            continue
        by_url[url] = post
        unique.append(post)
    return unique


# These per-post media fields arrive as an empty list when absent but as a
# bare value when present (a path string, or for `image_alt_text` an alt-text
# dict). A post never carries more than one of each, so collapse the empty
# list to None (and unwrap a populated list to its single element), giving the
# importer one shape to consume: the value, or None.
SINGLE_VALUE_FIELDS = (
    "image_file",
    "image_alt_text",
    "video_file",
    "srt_file",
    "screenshot_file",
)


def normalize_single_value_fields(post: dict) -> dict:
    for field in SINGLE_VALUE_FIELDS:
        value = post.get(field)
        if isinstance(value, list):
            post[field] = value[0] if value else None
    return post


def clean_social_media(social_media: dict) -> dict:
    cleaned = {}
    for platform, posts in social_media.items():
        config = PLATFORM_CONFIG.get(platform)
        if config is None:
            continue
        transform = config.get("transform", lambda p: p)
        cleaned[platform] = [
            normalize_single_value_fields(
                transform(filter_fields(post, platform, config))
            )
            for post in dedupe_posts(posts)
        ]
    return cleaned


# Misspellings in the source's `functions` text, given as the shortest
# distinctive substring -> fix. Keeping them minimal makes each entry reusable:
# "Bundestagsg" -> "Bundestags" repairs the typo in any label built from it,
# not just one full phrase. Applied to both the `Funktion` and `verband` fields
# (a fix that can't occur in a field is simply a no-op there). A few carry one
# word of context where the bare token would be ambiguous ("das" is a real word;
# only "Vorsitzender das" is wrong). Extend as new typos surface (use `--survey`).
CORRECTIONS = {
    "Miglied": "Mitglied",
    "Bundestagsg": "Bundestags",
    "Bundestagts": "Bundestags",
    "Westafalen": "Westfalen",
    "Vorstans": "Vorstands",
    "Stellvertreternder": "Stellvertretender",
    "Stellverretender": "Stellvertretender",
    "Stellvertretetender": "Stellvertretender",
    "Vorsitzend des": "Vorsitzende des",
    "Vorsitzender das": "Vorsitzender des",
    "Berllin": "Berlin",
}


def _normalize_function_text(value: str) -> str:
    # Collapse internal whitespace and strip surrounding whitespace and a single
    # trailing period (cheap, generic fixes for spacing/punctuation variants),
    # then apply the substring typo corrections.
    value = re.sub(r"\s+", " ", value or "").strip()
    value = value.rstrip(".").strip()
    for wrong, right in CORRECTIONS.items():
        value = value.replace(wrong, right)
    return value


def clean_functions(functions: list) -> list:
    # Normalise and typo-correct each function entry's `Funktion` and `verband`
    # in place; other fields pass through untouched. Linking these clean labels
    # to roles / institutional levels happens later, in the JSON importer.
    cleaned = []
    for func in functions:
        func = dict(func)
        func["Funktion"] = _normalize_function_text(func.get("Funktion", ""))
        func["verband"] = _normalize_function_text(func.get("verband", ""))
        cleaned.append(func)
    return cleaned


def survey_social_media(items: dict) -> dict[str, set[str]]:
    fields_by_platform: dict[str, set[str]] = {}
    for item in items.values():
        for platform, posts in item.get("social_media", {}).items():
            bucket = fields_by_platform.setdefault(platform, set())
            for post in posts:
                bucket.update(post.keys())
    return fields_by_platform


def survey_field_values(items: dict, field: str) -> dict[str, set]:
    values_by_platform: dict[str, set] = {}
    for item in items.values():
        for platform, posts in item.get("social_media", {}).items():
            bucket = values_by_platform.setdefault(platform, set())
            for post in posts:
                if field not in post:
                    continue
                value = post[field]
                try:
                    bucket.add(value)
                except TypeError:
                    bucket.add(json.dumps(value, sort_keys=True, ensure_ascii=False))
    return values_by_platform


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Combine and normalize source JSON files into one import-ready JSON."
    )
    parser.add_argument("primary", type=Path, help="Path to the primary source JSON.")
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("scripts/data/import.json"),
        help="Output path (default: scripts/data/import.json).",
    )
    parser.add_argument(
        "--survey",
        nargs="?",
        const=True,
        default=None,
        metavar="FIELD",
        help=(
            "Without argument: print the union of post field names per platform. "
            "With a field name: print the unique values seen for that field. "
            "Exits after printing."
        ),
    )
    args = parser.parse_args()

    items = load(args.primary)

    if args.survey is True:
        for platform, fields in sorted(survey_social_media(items).items()):
            print(f"{platform}:")
            for field in sorted(fields):
                print(f"  {field}")
        return

    if args.survey is not None:
        field = args.survey
        for platform, values in sorted(survey_field_values(items, field).items()):
            print(f"{platform} ({len(values)} unique):")
            for value in sorted(values, key=repr):
                print(f"  {value!r}")
        return

    result = {}
    for item_id, item in items.items():
        if "social_media" in item:
            item = {**item, "social_media": clean_social_media(item["social_media"])}
        if "functions" in item:
            item = {**item, "functions": clean_functions(item["functions"])}
        result[item_id] = item

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    posts_by_platform: dict[str, int] = {}
    for item in result.values():
        for platform, posts in item.get("social_media", {}).items():
            posts_by_platform[platform] = posts_by_platform.get(platform, 0) + len(
                posts
            )

    print(f"people: {len(result)}")
    print(f"posts: {sum(posts_by_platform.values())}")
    for platform, count in sorted(posts_by_platform.items()):
        print(f"  {platform}: {count}")


if __name__ == "__main__":
    main()
