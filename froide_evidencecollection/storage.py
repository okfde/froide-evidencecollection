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


def post_media_path(instance, filename):
    """Deterministic storage path for a ``SocialMediaPost`` media file.

    The top-level directory comes from the model's ``media_subdir`` so each
    media kind (images, screenshots, videos) lives in its own tree. The rest is
    derived from the post and the import ``source_path`` (the per-row natural
    key) so re-importing the same source overwrites in place. The full relative
    source path — not just the basename — is folded into the name so two sources
    that share a basename within one post don't collide. Falls back to the
    uploaded filename for a manually added file with no ``source_path``.
    """
    source = instance.source_path or filename
    ext = os.path.splitext(source)[1].lower()
    stem = source[: len(source) - len(ext)] if ext else source
    slug = slugify(stem.strip("./").replace("/", "-")) or "file"
    return f"{instance.media_subdir}/{instance.post_id}/{slug}{ext}"
