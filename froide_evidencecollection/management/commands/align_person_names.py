import csv
import json
from collections import defaultdict
from pathlib import Path

from django.core.management.base import BaseCommand, CommandError
from django.db import transaction

from froide_evidencecollection.models import Actor, Person
from froide_evidencecollection.utils import normalize_name

DATA_DIR = Path(__file__).resolve().parents[2] / "data"


class Command(BaseCommand):
    help = (
        "Align existing Person names with the JSON dump's naming scheme: re-split "
        "matched persons to the dump's label (keeping the old name as an alias) and "
        "create the persons that exist only in the dump. Dry-run unless --apply is "
        "given."
    )

    def add_arguments(self, parser):
        parser.add_argument("json_file", help="Path to the JSON dump")
        parser.add_argument(
            "--overrides",
            default=str(DATA_DIR / "person_name_overrides.csv"),
            help="CSV of manual label -> first_name/last_name splits",
        )
        parser.add_argument(
            "--apply",
            action="store_true",
            help="Write changes. Without this the command only reports.",
        )

    def handle(self, *args, **options):
        dump_labels = self.load_person_labels(options["json_file"])
        overrides = self.load_overrides(options["overrides"])

        persons = list(Person.objects.all())
        # Pre-tokenize once: normalized first-name token set and last-name tokens.
        tokenized = {
            p: (
                set(normalize_name(p.first_name).split()),
                normalize_name(p.last_name).split(),
            )
            for p in persons
        }

        renames = []  # (person, old_first, old_last, new_first, new_last)
        creations = []  # (label, first, last)
        needs_review = []  # (label, reason)
        collisions = []  # (label, [persons]) — one label, several persons
        matched_by = defaultdict(list)  # person -> [labels] (reverse collisions)

        for label in sorted(dump_labels):
            ltokens = normalize_name(label).split()
            if not ltokens:
                needs_review.append((label, "empty after normalization"))
                continue

            candidates = self.match_persons(ltokens, tokenized)
            override = overrides.get(label)

            if len(candidates) > 1:
                # Several DB persons match (e.g. the label "Jane Doe" matches
                # both "Jane Doe" and "Jane Mary Doe"). This is always a
                # collision unless an override names which of them the label
                # means.
                pinned = (
                    [p for p in candidates if str(p.sync_uuid) == override["sync_uuid"]]
                    if override and override["sync_uuid"]
                    else []
                )
                if len(pinned) == 1:
                    candidates = pinned
                else:
                    collisions.append((label, candidates))
                    continue

            if candidates:
                person = candidates[0]
                if override:
                    new_first, new_last = override["canonical"]
                else:
                    split = self.split_label(label, person.last_name)
                    if split is None:
                        # Surname boundary unclear (e.g. hyphenation mismatch).
                        needs_review.append(
                            (label, "could not split off last name; add to overrides")
                        )
                        continue
                    new_first, new_last = split

                matched_by[person].append(label)
                if (person.first_name, person.last_name) == (new_first, new_last):
                    continue  # already aligned (idempotent re-run)
                renames.append(
                    (person, person.first_name, person.last_name, new_first, new_last)
                )
            else:
                self.plan_creation(label, override, creations, needs_review)

        # A single DB person claimed by more than one dump label is ambiguous too.
        reverse = {p: labels for p, labels in matched_by.items() if len(labels) > 1}

        orphans = [p for p in persons if p not in matched_by]

        self.report(renames, creations, orphans, collisions, reverse, needs_review)

        if collisions or reverse:
            raise CommandError(
                "Ambiguous matches found (see report above); resolve them via "
                "overrides before applying."
            )

        if not options["apply"]:
            self.stdout.write(
                self.style.WARNING("\nDry run — re-run with --apply to write changes.")
            )
            return

        with transaction.atomic():
            for person, old_first, old_last, new_first, new_last in renames:
                old_full = f"{old_first} {old_last}".strip()
                if old_full and old_full not in person.also_known_as:
                    person.also_known_as.append(old_full)
                person.first_name = new_first
                person.last_name = new_last
                person.save()
                # Actor keeps a denormalized copy of the name, synced only in
                # Actor.save(); re-save it so it doesn't keep the old name.
                try:
                    person.actor.save()
                except Actor.DoesNotExist:
                    pass
            for _label, first, last in creations:
                Person.objects.create(first_name=first, last_name=last)

        self.stdout.write(
            self.style.SUCCESS(
                f"\nApplied: {len(renames)} re-split, {len(creations)} created."
            )
        )

    # ------------------------------------------------------------------
    # Matching / splitting
    # ------------------------------------------------------------------
    def match_persons(self, ltokens, tokenized):
        """Persons whose surname ends the label and whose first names overlap.

        A label matches a person when the label ends with the person's
        (normalized) last-name tokens and the label's leading tokens and the
        person's first-name tokens contain one another — so both an extra first
        name in the DB ("Alice Maria" vs dump "Alice") and an extra one in the
        dump ("Hans Peter" vs DB "Peter") still match.
        """
        matches = []
        for person, (first_tokens, last_tokens) in tokenized.items():
            n = len(last_tokens)
            if not n or ltokens[-n:] != last_tokens:
                continue
            label_first = set(ltokens[:-n])
            if not label_first:
                continue  # label is bare surname; not enough to identify a person
            if label_first <= first_tokens or first_tokens <= label_first:
                matches.append(person)
        return matches

    def split_label(self, label, last_name):
        """Split the raw label into (first_name, last_name) in the dump's casing.

        The surname is the same as the DB's, so we peel the smallest suffix of
        the label that normalizes to the person's normalized last name. Returns
        ``None`` if no such suffix exists.
        """
        raw = label.split()
        norm_last = normalize_name(last_name).split()
        for k in range(1, len(raw) + 1):
            if normalize_name(" ".join(raw[-k:])).split() == norm_last:
                first = " ".join(raw[:-k])
                if not first:
                    return None
                return first, " ".join(raw[-k:])
        return None

    def plan_creation(self, label, override, creations, needs_review):
        if override:
            creations.append((label, *override["canonical"]))
            return
        tokens = label.split()
        if len(tokens) == 2:
            creations.append((label, tokens[0], tokens[1]))
        else:
            needs_review.append(
                (label, f"{len(tokens)} tokens — split ambiguous; add to overrides")
            )

    # ------------------------------------------------------------------
    # Loaders
    # ------------------------------------------------------------------
    def load_person_labels(self, json_file):
        with open(json_file) as f:
            data = json.load(f)
        # Person entries are the ones that carry a label and are typed "p".
        return {
            v["label"]
            for v in data.values()
            if v.get("label") and v.get("ent_type") == "p"
        }

    def _read_csv(self, path):
        with open(path, encoding="utf-8") as f:
            rows = [line for line in f if not line.lstrip().startswith("#")]
        return list(csv.DictReader(rows))

    def load_overrides(self, path):
        if not Path(path).exists():
            return {}
        overrides = {}
        for row in self._read_csv(path):
            label = (row.get("label") or "").strip()
            first = (row.get("first_name") or "").strip()
            last = (row.get("last_name") or "").strip()
            if not (label and first and last):
                continue
            overrides[label] = {
                "canonical": (first, last),
                # Optional: sync_uuid of the DB person to disambiguate a
                # collision (names alone are not unique).
                "sync_uuid": (row.get("sync_uuid") or "").strip() or None,
            }
        return overrides

    # ------------------------------------------------------------------
    # Reporting
    # ------------------------------------------------------------------
    def report(self, renames, creations, orphans, collisions, reverse, needs_review):
        w = self.stdout.write
        w(self.style.MIGRATE_HEADING(f"\nRe-split ({len(renames)}):"))
        for _person, of, ol, nf, nl in sorted(renames, key=lambda r: (r[2], r[1])):
            w(f"  {of + ' ' + ol!r} -> {nf + ' ' + nl!r}")

        w(self.style.MIGRATE_HEADING(f"\nCreate ({len(creations)}):"))
        for label, first, last in sorted(creations):
            w(f"  {label!r}  ->  first={first!r} last={last!r}")

        # w(self.style.MIGRATE_HEADING(f"\nKept as-is — no dump match ({len(orphans)}):"))
        # for person in sorted(orphans, key=lambda p: (p.last_name, p.first_name)):
        #    w(f"  {person.first_name + ' ' + person.last_name!r}")

        if needs_review:
            w(self.style.WARNING(f"\nSkipped — split undecided ({len(needs_review)}):"))
            for label, reason in needs_review:
                w(f"  {label!r}  ({reason})")

        if collisions:
            w(
                self.style.ERROR(
                    f"\nCollisions — label matches several ({len(collisions)}):"
                )
            )
            for label, persons in collisions:
                w(f"  {label!r} matches:")
                for p in persons:
                    w(
                        f"    {p.first_name + ' ' + p.last_name!r}  sync_uuid={p.sync_uuid}"
                    )

        if reverse:
            w(
                self.style.ERROR(
                    f"\nCollisions — person matched by several ({len(reverse)}):"
                )
            )
            for person, labels in reverse.items():
                joined = ", ".join(repr(label) for label in labels)
                w(f"  {person.first_name + ' ' + person.last_name!r} <- {joined}")
