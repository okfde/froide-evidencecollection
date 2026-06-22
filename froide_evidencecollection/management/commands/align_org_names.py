import csv
import json
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from froide_evidencecollection.models import Actor, InstitutionalLevel, Organization
from froide_evidencecollection.utils import (
    apply_org_label_replacement,
    load_org_label_replacements,
    normalize_name,
)

DATA_DIR = Path(__file__).resolve().parents[2] / "data"
REVIEW = "REVIEW"


class Command(BaseCommand):
    help = (
        "Align existing Organization names with the JSON dump's naming scheme: "
        "rename matched orgs (keeping the old name as an alias) and create the "
        "orgs that exist only in the dump. Dry-run unless --apply is given."
    )

    def add_arguments(self, parser):
        parser.add_argument("json_file", help="Path to the JSON dump")
        parser.add_argument(
            "--overrides",
            default=str(DATA_DIR / "org_name_overrides.csv"),
            help="CSV of manual current_name -> canonical_name mappings",
        )
        parser.add_argument(
            "--levels",
            default=str(DATA_DIR / "org_type_levels.csv"),
            help="CSV mapping org type/label to InstitutionalLevel",
        )
        parser.add_argument(
            "--replacements",
            default=str(DATA_DIR / "org_label_replacements.csv"),
            help="CSV of dump-label corrections (wrong_label -> correct_label)",
        )
        parser.add_argument(
            "--apply",
            action="store_true",
            help="Write changes. Without this the command only reports.",
        )

    def handle(self, *args, **options):
        replacements = load_org_label_replacements(options["replacements"])
        dump_labels = self.load_org_labels(options["json_file"], replacements)
        overrides = self.load_overrides(options["overrides"])
        exact_levels, prefix_levels = self.load_level_rules(options["levels"])

        # Normalized form -> dump label (first wins on collision).
        norm_to_label = {}
        for label in dump_labels:
            norm_to_label.setdefault(normalize_name(label), label)

        orgs = list(Organization.objects.all())
        existing_names = {o.organization_name for o in orgs}

        renames = []  # (org, old_name, canonical)
        orphans = []  # orgs with no dump counterpart
        by_canonical = {}  # canonical -> [orgs] (collision detection)

        for org in orgs:
            name = org.organization_name
            if name in overrides:
                canonical = overrides[name]
            else:
                canonical = norm_to_label.get(normalize_name(name))

            if not canonical:
                orphans.append(org)
                continue
            if canonical == name:
                continue  # already aligned (idempotent re-run)

            renames.append((org, name, canonical))
            by_canonical.setdefault(canonical, []).append(org)

        collisions = {c: o for c, o in by_canonical.items() if len(o) > 1}

        claimed = {canonical for _, _, canonical in renames} | existing_names
        to_create = sorted(label for label in dump_labels if label not in claimed)

        creations = []  # (label, level_name)
        needs_level = []  # (label, reason)
        for label in to_create:
            level_name = self.resolve_level(label, exact_levels, prefix_levels)
            if level_name is None:
                needs_level.append((label, "no matching type rule"))
            elif level_name == REVIEW:
                needs_level.append((label, "type marked REVIEW"))
            else:
                creations.append((label, level_name))

        self.report(renames, orphans, collisions, creations, needs_level)

        if collisions:
            raise CommandError(
                "Multiple orgs map to the same canonical name; resolve the "
                "overrides before applying (see report above)."
            )

        if not options["apply"]:
            self.stdout.write(
                self.style.WARNING("\nDry run — re-run with --apply to write changes.")
            )
            return

        with transaction.atomic():
            for org, old_name, canonical in renames:
                if old_name not in org.also_known_as:
                    org.also_known_as.append(old_name)
                org.organization_name = canonical
                org.save()
                # Actor keeps a denormalized copy of the name, synced only in
                # Actor.save(); re-save it so it doesn't keep the old name.
                try:
                    org.actor.save()
                except Actor.DoesNotExist:
                    pass
            for label, level_name in creations:
                level, _ = InstitutionalLevel.objects.get_or_create(name=level_name)
                Organization.objects.create(
                    organization_name=label, institutional_level=level
                )

        self.stdout.write(
            self.style.SUCCESS(
                f"\nApplied: {len(renames)} renamed, {len(creations)} created."
            )
        )

    # ------------------------------------------------------------------
    # Loaders
    # ------------------------------------------------------------------
    def load_org_labels(self, json_file, replacements):
        with open(json_file) as f:
            data = json.load(f)
        # Org entries are top-level entries that carry a label and are typed "o".
        # Apply dump-label corrections so the dump's shorthand (abbreviated type
        # words, typos) never becomes an org name.
        return {
            apply_org_label_replacement(v["label"], replacements)
            for v in data.values()
            if v.get("label") and v.get("ent_type") == "o"
        }

    def _read_csv(self, path):
        with open(path, encoding="utf-8") as f:
            rows = [line for line in f if not line.lstrip().startswith("#")]
        return list(csv.DictReader(rows))

    def load_overrides(self, path):
        return {
            row["current_name"]: row["canonical_name"]
            for row in self._read_csv(path)
            if row.get("current_name") and row.get("canonical_name")
        }

    def load_level_rules(self, path):
        exact, prefix = {}, []
        for row in self._read_csv(path):
            kind, key, level = row["kind"], row["key"], row["level"]
            if kind == "exact":
                exact[key] = level
            elif kind == "prefix":
                prefix.append((key, level))
        # Longest prefixes first so the most specific rule wins.
        prefix.sort(key=lambda kv: len(kv[0]), reverse=True)
        return exact, prefix

    def resolve_level(self, label, exact_levels, prefix_levels):
        if label in exact_levels:
            return exact_levels[label]
        for key, level in prefix_levels:
            if label == key or label.startswith(key + " "):
                return level
        return None

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------
    def report(self, renames, orphans, collisions, creations, needs_level):
        w = self.stdout.write
        w(self.style.MIGRATE_HEADING(f"\nRename ({len(renames)}):"))
        for _, old, canonical in sorted(renames, key=lambda r: r[1]):
            w(f"  {old!r} -> {canonical!r}")

        w(self.style.MIGRATE_HEADING(f"\nCreate ({len(creations)}):"))
        for label, level in creations:
            w(f"  {label!r}  [{level}]")

        # w(self.style.MIGRATE_HEADING(f"\nKept as-is — no dump match ({len(orphans)}):"))
        # for org in sorted(orphans, key=lambda o: o.organization_name):
        #    w(f"  {org.organization_name!r}")

        if needs_level:
            w(self.style.WARNING(f"\nSkipped — level undecided ({len(needs_level)}):"))
            for label, reason in needs_level:
                w(f"  {label!r}  ({reason})")

        if collisions:
            w(self.style.ERROR(f"\nCollisions ({len(collisions)}):"))
            for canonical, orgs in collisions.items():
                names = ", ".join(repr(o.organization_name) for o in orgs)
                w(f"  {canonical!r} <- {names}")
