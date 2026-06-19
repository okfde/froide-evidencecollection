import os

from django.core.files.storage import FileSystemStorage

from froide.helper.text_utils import slugify


class OverwriteStorage(FileSystemStorage):
    """FileSystemStorage that overwrites an existing file at the same path.

    Django's default storage appends a random suffix on a name collision; here
    the name is returned unchanged so re-saving replaces the file in place.
    Paired with a deterministic ``upload_to`` (see ``post_screenshot_path``)
    keyed on the ``screenshot_source_path`` natural key the importer upserts on,
    so the same source file always lands at the same path and a re-import
    overwrites it instead of accumulating copies.

    Unlike froide's content-addressed ``HashedFilenameStorage`` this keeps
    stable, human-readable paths and does *not* deduplicate identical bytes —
    the same image referenced by several posts is stored once per row.
    """

    def get_available_name(self, name, max_length=None):
        if self.exists(name):
            self.delete(name)
        return name


# Single top-level app directory for post media. The screenshot is now the only
# file-backed post media (image/video are tracked by source path only), so it
# lives directly under ``post_media/screenshots/``.
POST_MEDIA_DIR = "post_media"
SCREENSHOT_SUBDIR = "screenshots"


def post_media_path(instance, filename):
    """Legacy media path, retained only for historical migrations.

    Older migrations reference this callable as a field default (on the removed
    ``PostImage`` / ``PostScreenshot`` models), so the symbol must keep existing
    for the migration graph to import — it is never called at runtime now. New
    screenshots use ``post_screenshot_path``.
    """
    source = getattr(instance, "source_path", "") or filename
    ext = os.path.splitext(source)[1].lower()
    stem = source[: len(source) - len(ext)] if ext else source
    slug = slugify(stem.strip("./").replace("/", "-")) or "file"
    subdir = getattr(instance, "media_subdir", None) or SCREENSHOT_SUBDIR
    return f"{POST_MEDIA_DIR}/{subdir}/{slug}{ext}"


def post_screenshot_path(instance, filename):
    """Deterministic storage path for a ``SocialMediaPost`` screenshot file.

    Files live under ``post_media/screenshots/``. The filename is derived from
    the import ``screenshot_source_path`` (the natural key) so re-importing the
    same source overwrites in place; the full relative source path — not just
    the basename — is folded in to keep the name stable and readable. Falls back
    to the uploaded filename for a manually added file with no source path.
    """
    source = getattr(instance, "screenshot_source_path", "") or filename
    ext = os.path.splitext(source)[1].lower()
    stem = source[: len(source) - len(ext)] if ext else source
    slug = slugify(stem.strip("./").replace("/", "-")) or "file"
    return f"{POST_MEDIA_DIR}/{SCREENSHOT_SUBDIR}/{slug}{ext}"
