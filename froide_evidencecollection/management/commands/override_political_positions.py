import csv
from collections import defaultdict
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from froide_evidencecollection.json_importer import (
    parse_level,
    parse_role,
    segment_positions,
)
from froide_evidencecollection.models import (
    InstitutionalLevel,
    Person,
    PoliticalPosition,
    Role,
)
from froide_evidencecollection.utils import normalize_name

DATA_DIR = Path(__file__).resolve().parents[2] / "data"


class Command(BaseCommand):
    help = (
        "Override each listed person's political position label with the blob "
        "from a CSV (columns: name, position). The verbatim blob is stored on the "
        "person; it is segmented on commas / 'und' and each segment's parsed "
        "(role, level) is materialized as a tag. Persons are matched by "
        "normalized full name. Dry-run unless --apply."
    )

    def add_arguments(self, parser):
        parser.add_argument(
            "csv_file",
            nargs="?",
            default=str(DATA_DIR / "political_position_overrides.csv"),
            help="CSV with 'name' and 'position' columns",
        )
        parser.add_argument(
            "--apply",
            action="store_true",
            help="Write changes. Without this the command only reports.",
        )

    def handle(self, *args, **options):
        rows = self.load_overrides(options["csv_file"])

        persons = list(Person.objects.all())
        by_norm = defaultdict(list)
        for p in persons:
            by_norm[normalize_name(f"{p.first_name} {p.last_name}")].append(p)

        changes = []  # (person, old_label, new_label) — blob set or relabelled
        unmatched = []  # (name, reason)
        collisions = []  # (name, [persons])

        for name, label in rows:
            key = normalize_name(name)
            candidates = by_norm.get(key, [])
            if not candidates:
                unmatched.append((name, "no person matches this name"))
                continue
            if len(candidates) > 1:
                collisions.append((name, candidates))
                continue

            person = candidates[0]
            if person.political_position_label != label:
                changes.append((person, person.political_position_label, label))
            # else: already set to this blob (idempotent re-run) — nothing to do.

        self.report(changes, unmatched, collisions)

        if collisions:
            raise CommandError(
                "Several persons share a name in the CSV (see report above); "
                "resolve them before applying."
            )

        if not options["apply"]:
            self.stdout.write(
                self.style.WARNING("\nDry run — re-run with --apply to write changes.")
            )
            return

        with transaction.atomic():
            for person, _old_label, label in changes:
                person.political_position_label = label
                person.save(update_fields=["political_position_label", "updated_at"])
                for segment in segment_positions(label):
                    role = self.resolve_role(segment)
                    level = self.resolve_level(segment)
                    if role is None and level is None:
                        continue
                    PoliticalPosition.objects.get_or_create(
                        person=person,
                        role=role,
                        institutional_level=level,
                    )

        self.stdout.write(self.style.SUCCESS(f"\nApplied: {len(changes)} labels set."))

    # ------------------------------------------------------------------
    # Role / level resolution (per segment of the blob)
    # ------------------------------------------------------------------
    def resolve_role(self, segment):
        name = parse_role(segment)
        if not name:
            return None
        role, _ = Role.objects.get_or_create(name=name)
        return role

    def resolve_level(self, segment):
        name = parse_level(segment)
        if not name:
            return None
        level = InstitutionalLevel.objects.filter(name=name).first()
        if level is None:
            self.stdout.write(
                self.style.WARNING(
                    f"  No InstitutionalLevel {name!r} for segment {segment!r}"
                )
            )
        return level

    # ------------------------------------------------------------------
    # Loader
    # ------------------------------------------------------------------
    def load_overrides(self, path):
        with open(path, encoding="utf-8") as f:
            lines = [line for line in f if not line.lstrip().startswith("#")]
        rows = []
        for row in csv.DictReader(lines):
            name = (row.get("name") or "").strip()
            position = (row.get("position") or "").strip()
            if name and position:
                rows.append((name, position))
        return rows

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------
    def report(self, changes, unmatched, collisions):
        w = self.stdout.write

        w(self.style.MIGRATE_HEADING(f"\nSet label ({len(changes)}):"))
        for person, old, new in sorted(changes, key=lambda r: str(r[0])):
            w(f"  {person}: {old!r} -> {new!r}")

        if unmatched:
            w(self.style.WARNING(f"\nUnmatched ({len(unmatched)}):"))
            for name, reason in unmatched:
                w(f"  {name!r}  ({reason})")

        if collisions:
            w(
                self.style.ERROR(
                    f"\nCollisions — name matches several ({len(collisions)}):"
                )
            )
            for name, persons in collisions:
                w(f"  {name!r} matches:")
                for p in persons:
                    w(f"    {p}  sync_uuid={p.sync_uuid}")
