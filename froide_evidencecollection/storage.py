import os

from django.core.files.storage import FileSystemStorage


class OverwriteStorage(FileSystemStorage):
    """FileSystemStorage that overwrites an existing file at the same path.

    Django's default storage appends a random suffix on a name collision; here
    the name is returned unchanged so re-saving replaces the file in place.
    Paired with ``post_screenshot_path`` (which keeps the source filename
    unchanged), so re-importing the same source file lands at the same path and
    overwrites it instead of accumulating copies.

    Unlike froide's content-addressed ``HashedFilenameStorage`` this keeps
    stable, human-readable paths and does *not* deduplicate identical bytes —
    the same image referenced by several posts is stored once per row.
    """

    def get_available_name(self, name, max_length=None):
        if self.exists(name):
            self.delete(name)
        return name


# All app media live under one top-level directory named after the app, split
# into per-kind subdirs. Files keep their original filename unchanged (no
# prefix, slug, or path folding).
MEDIA_PATH = "froide_evidencecollection"
SCREENSHOT_SUBDIR = "screenshots"


def post_media_path(instance, filename):
    """Legacy media path, retained only for historical migrations.

    Older migrations reference this callable as a field default (on the removed
    ``PostImage`` / ``PostScreenshot`` models), so the symbol must keep existing
    for the migration graph to import — it is never called at runtime now. New
    screenshots use ``post_screenshot_path``.
    """
    return f"{MEDIA_PATH}/{SCREENSHOT_SUBDIR}/{os.path.basename(filename)}"


def post_screenshot_path(instance, filename):
    """Storage path for a ``SocialMediaPost`` screenshot file.

    The file is stored under ``<app dir>/screenshots/`` with its original
    filename unchanged. The importer saves the screenshot under the source
    file's basename, so re-importing the same source lands at the same path and
    (with ``OverwriteStorage``) overwrites in place.
    """
    return f"{MEDIA_PATH}/{SCREENSHOT_SUBDIR}/{os.path.basename(filename)}"
