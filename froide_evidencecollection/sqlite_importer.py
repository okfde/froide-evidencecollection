import logging
import os
import sqlite3
from datetime import date
from pathlib import Path

from django.core.files import File
from django.db import transaction

from froide_evidencecollection.models import (
    Actor,
    Attachment,
    Evidence,
    Person,
)
from froide_evidencecollection.utils import compute_hash

logger = logging.getLogger(__name__)


def parse_date(value):
    """Parse a date string in YYYY-MM-DD format."""
    if not value:
        return None
    try:
        return date.fromisoformat(value)
    except (ValueError, TypeError):
        logger.warning("Could not parse date: %s", value)
        return None


def get_or_create_actor(person):
    """Get or create an Actor for the given Person."""
    try:
        return person.actor
    except Actor.DoesNotExist:
        return Actor.objects.create(person=person)


MEDIA_EXTENSIONS = [".mp4", ".jpg"]
MIMETYPES = {".mp4": "video/mp4", ".jpg": "image/jpeg"}


def find_media_file(media_dir, url_hash):
    """Look for a media file matching the url_hash, preferring mp4 over jpg."""
    for ext in MEDIA_EXTENSIONS:
        path = media_dir / f"{url_hash}{ext}"
        if path.exists():
            return path
    return None


class SQLiteImporter:
    def __init__(self, db_path, dry_run=False):
        self.db_path = db_path
        self.media_dir = Path(db_path).parent / "media"
        self.dry_run = dry_run
        self.stats = {}

    def read_table(self, table_name):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_name}")  # noqa: S608
        rows = cursor.fetchall()
        conn.close()
        logger.info("Found %d rows in '%s' table", len(rows), table_name)
        return rows

    @transaction.atomic
    def run(self):
        self.import_evidence()
        self.import_attachments()
        return self.stats

    def import_evidence(self):
        rows = self.read_table("belege")

        # Build lookup for person_id (name_hash) -> Person.
        persons_by_hash = {p.name_hash: p for p in Person.objects.all() if p.name_hash}

        # Determine the next external_id to use.
        max_external_id = (
            Evidence.objects.order_by("-external_id")
            .values_list("external_id", flat=True)
            .first()
        ) or 0
        next_external_id = max_external_id + 1

        # Pre-fetch existing evidence by url_hash for update-or-create.
        existing_by_url_hash = {
            e.url_hash: e for e in Evidence.objects.exclude(url_hash="")
        }

        evidence_stats = {"created": 0, "updated": 0, "skipped_no_url": 0}

        for row in rows:
            url = (row["url"] or "").strip()
            if not url:
                evidence_stats["skipped_no_url"] += 1
                continue

            url_hash = compute_hash(url)
            citation = (row["fullText"] or "").strip()
            publishing_date = parse_date(row["date"])
            documentation_date = parse_date(row["date_collected"])

            existing = existing_by_url_hash.get(url_hash)

            if self.dry_run:
                person_id_hash = (row["person_id"] or "").strip()
                person_match = person_id_hash in persons_by_hash
                action = "update" if existing else "create"
                logger.info(
                    "Would %s: url=%s, person match=%s, date=%s",
                    action,
                    url,
                    person_match,
                    row["date"],
                )
                evidence_stats["updated" if existing else "created"] += 1
                continue

            if existing:
                evidence = existing
                evidence.citation = citation
                evidence.publishing_date = publishing_date
                evidence.documentation_date = documentation_date
                evidence.save()
                evidence_stats["updated"] += 1
            else:
                evidence = Evidence(
                    external_id=next_external_id,
                    reference_url=url,
                    citation=citation,
                    publishing_date=publishing_date,
                    documentation_date=documentation_date,
                )
                evidence.save()
                next_external_id += 1
                existing_by_url_hash[url_hash] = evidence
                evidence_stats["created"] += 1

            # Link originator via person_id (name_hash).
            person_id_hash = (row["person_id"] or "").strip()
            if person_id_hash:
                person = persons_by_hash.get(person_id_hash)
                if person:
                    actor = get_or_create_actor(person)
                    evidence.originators.add(actor)
                else:
                    logger.warning(
                        "No person found for name_hash=%s (url=%s)",
                        person_id_hash,
                        url,
                    )

        self.stats["evidence"] = evidence_stats
        logger.info("Evidence import: %s", evidence_stats)

    def import_attachments(self):
        existing_attachment_ids = set(
            Attachment.objects.values_list("external_id", flat=True)
        )

        attachment_stats = {"created": 0, "skipped_exists": 0, "skipped_no_file": 0}

        for evidence in Evidence.objects.exclude(url_hash=""):
            attachment_ext_id = evidence.url_hash[:20]

            if attachment_ext_id in existing_attachment_ids:
                attachment_stats["skipped_exists"] += 1
                continue

            media_path = find_media_file(self.media_dir, evidence.url_hash)
            if not media_path:
                attachment_stats["skipped_no_file"] += 1
                continue

            if self.dry_run:
                logger.info(
                    "Would attach: %s to evidence %s",
                    media_path.name,
                    evidence.external_id,
                )
                attachment_stats["created"] += 1
                continue

            ext = media_path.suffix
            with open(media_path, "rb") as f:
                attachment = Attachment(
                    external_id=attachment_ext_id,
                    evidence=evidence,
                    title=media_path.name,
                    mimetype=MIMETYPES.get(ext, ""),
                    size=os.path.getsize(media_path),
                )
                attachment.file.save(media_path.name, File(f), save=False)
                attachment.save()
            existing_attachment_ids.add(attachment_ext_id)
            attachment_stats["created"] += 1

        self.stats["attachments"] = attachment_stats
        logger.info("Attachment import: %s", attachment_stats)
