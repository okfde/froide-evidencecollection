"""Shared helpers for the image-description review scripts.

The review pipeline runs *before* the database import, entirely on files:

  prepare_import.py ─▶ import.json ─┬─▶ export_image_reviews.py ─▶ reviews.xlsx
                                    │                                   │ (humans
                                    │                                   ▼  edit)
       import.curated.json ◀── apply_image_reviews.py ◀── image_reviews.json
                                    ▲          (the ledger) ◀── import_image_reviews.py
                                    │
                              (fed to the DB importer)

`image_reviews.json` (the *ledger*) is the one artefact that can't be
regenerated — it records the human-checked alt text per post — so it is meant to
be committed. `import.json` and the spreadsheets are regenerable/transient.

A post is identified by `platform:platform_user_id:platform_post_id`, which is
stable across re-scrapes and mirrors the DB's `(account, platform_post_id)`
uniqueness. The key is treated as opaque (never split back apart), so a
separator that happens to occur inside an id is harmless.
"""

import json
import os
from pathlib import Path

# Platforms the DB importer understands; others are skipped (mirrors the
# PLATFORM_MAP gate in json_importer.py).
KNOWN_PLATFORMS = {
    "facebook",
    "instagram",
    "telegram",
    "tiktok",
    "twitter",
    "youtube",
}


def generated_alt(item):
    """The machine-generated alt text on a post, stripped, or "" if none."""
    alt = item.get("image_alt_text") or {}
    return (alt.get("alt_text") or "").strip()


def post_key(platform, item):
    """Stable cross-scrape identity for a post."""
    account = item.get("account") or {}
    return (
        f"{platform}:{account.get('platform_user_id')}:{item.get('platform_post_id')}"
    )


def is_reviewable(item):
    """A post belongs in review if it has a content image to describe — or
    already has a generated description (which can exist even when no image file
    was stored). Images without a description are included so editors can write
    one by hand."""
    return bool(item.get("image_file")) or bool(generated_alt(item))


def iter_review_posts(data):
    """Yield ``(key, platform, item)`` for every reviewable post (one with an
    image, or an existing description), in document order."""
    for entry in data.values():
        for platform, items in (entry.get("social_media") or {}).items():
            if platform not in KNOWN_PLATFORMS:
                continue
            for item in items or []:
                if is_reviewable(item):
                    yield post_key(platform, item), platform, item


def _resolve(rel, images_root):
    """Absolute Path for a bundle-relative file ref, or None if absent/missing."""
    if not rel:
        return None
    path = Path(os.path.normpath(os.path.join(images_root, rel)))
    return path if path.is_file() else None


def image_path(item, images_root):
    """The post's content image as an absolute Path, or None."""
    return _resolve(item.get("image_file"), images_root)


def screenshot_path(item, images_root):
    """The post's archival full-page screenshot as an absolute Path, or None."""
    return _resolve(item.get("screenshot_file"), images_root)


# Generated files default here (relative to the repo root); see .gitignore.
DATA_DIR = "scripts/data"


def ensure_parent(path):
    """Create the parent directory of `path` if needed; return the path."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    return path


def load_json(path):
    return json.loads(Path(path).read_text())


def load_ledger(path):
    """Load the review ledger, or {} if it doesn't exist yet."""
    path = Path(path)
    if not path.exists():
        return {}
    return json.loads(path.read_text())


def save_ledger(path, ledger):
    """Write the ledger pretty-printed with sorted keys, for clean diffs."""
    text = json.dumps(ledger, ensure_ascii=False, indent=2, sort_keys=True)
    ensure_parent(path).write_text(text + "\n")
