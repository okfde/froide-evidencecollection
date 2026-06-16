import os

from django.core.files.storage import FileSystemStorage

from froide.helper.text_utils import slugify


class OverwriteStorage(FileSystemStorage):
    """FileSystemStorage that overwrites an existing file at the same path.

    Django's default storage appends a random suffix on a name collision; here
    the name is returned unchanged so re-saving replaces the file in place.
    Paired with a deterministic ``upload_to`` (see ``post_media_path``) keyed on
    the same ``(post, source_path)`` natural key the importer upserts on, so the
    same source file always lands at the same path and a re-import overwrites it
    instead of accumulating copies.

    Unlike froide's content-addressed ``HashedFilenameStorage`` this keeps
    stable, human-readable paths and does *not* deduplicate identical bytes —
    the same image referenced by several posts is stored once per row.
    """

    def get_available_name(self, name, max_length=None):
        if self.exists(name):
            self.delete(name)
        return name


# Single top-level app directory; each media kind gets a child dir under it (see
# each model's `media_subdir`). Keeps all post media under one tree.
POST_MEDIA_DIR = "post_media"


def post_media_path(instance, filename):
    """Deterministic storage path for a ``SocialMediaPost`` media file.

    Files live under ``post_media/<media_subdir>/`` (one app dir, one child per
    media kind). The filename is derived from the import ``source_path`` (the
    per-row natural key) so re-importing the same source overwrites in place;
    source filenames are unique within a media subdir, so no per-post prefix is
    needed. The full relative source path — not just the basename — is folded in
    to keep the name stable and readable. Falls back to the uploaded filename for
    a manually added file with no ``source_path``.
    """
    source = instance.source_path or filename
    ext = os.path.splitext(source)[1].lower()
    stem = source[: len(source) - len(ext)] if ext else source
    slug = slugify(stem.strip("./").replace("/", "-")) or "file"
    return f"{POST_MEDIA_DIR}/{instance.media_subdir}/{slug}{ext}"
