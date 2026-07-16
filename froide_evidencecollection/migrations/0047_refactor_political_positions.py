import datetime
import uuid

from django.db import migrations, models

from froide_evidencecollection.json_importer import (
    parse_level,
    parse_role,
    segment_positions,
)

# The blob's per-row label predates a checked-on date, so the backfill stamps the
# one date that was hardcoded in the old "(As of: …)" rendering.
CHECKED_AT = datetime.date(2026, 6, 24)


def backfill_labels(apps, schema_editor):
    Person = apps.get_model("froide_evidencecollection", "Person")
    PoliticalPosition = apps.get_model("froide_evidencecollection", "PoliticalPosition")
    Role = apps.get_model("froide_evidencecollection", "Role")
    InstitutionalLevel = apps.get_model("froide_evidencecollection", "InstitutionalLevel")

    for person in Person.objects.filter(political_positions__isnull=False).distinct():
        # Each person had a single position whose `label` was the whole blob; move
        # it onto the person and stamp the checked-on date.
        position = person.political_positions.order_by("pk").first()
        label = position.label if position else ""
        if not label:
            continue
        person.political_position_label = label
        person.political_position_checked_at = CHECKED_AT
        person.save(
            update_fields=[
                "political_position_label",
                "political_position_checked_at",
            ]
        )

        # Re-parse the moved blob the way the new importer does: segment on commas
        # / "und" and materialize one (role, level) tag per segment. The old rows
        # were parsed from the whole blob, so drop them and rebuild from segments.
        person.political_positions.all().delete()
        for segment in segment_positions(label):
            role_name = parse_role(segment)
            level_name = parse_level(segment)
            role = None
            if role_name:
                # Historical Role has no custom save(), so mint the sync_uuid here.
                role, _ = Role.objects.get_or_create(
                    name=role_name, defaults={"sync_uuid": uuid.uuid4()}
                )
            level = (
                InstitutionalLevel.objects.filter(name=level_name).first()
                if level_name
                else None
            )
            if role is None and level is None:
                continue
            PoliticalPosition.objects.get_or_create(
                person=person, role=role, institutional_level=level
            )


def restore_labels(apps, schema_editor):
    Person = apps.get_model("froide_evidencecollection", "Person")
    PoliticalPosition = apps.get_model("froide_evidencecollection", "PoliticalPosition")

    # Reverse of the split: collapse the per-segment (role, level) rows back into
    # the single blob-bearing row the old schema kept, so the label survives a
    # downgrade instead of being dropped with the Person field.
    for person in Person.objects.exclude(political_position_label="").iterator():
        label = person.political_position_label
        person.political_positions.all().delete()
        PoliticalPosition.objects.create(person=person, label=label)


class Migration(migrations.Migration):

    dependencies = [
        ("froide_evidencecollection", "0046_remove_evidence_related_actors"),
    ]

    operations = [
        migrations.AddField(
            model_name="person",
            name="political_position_checked_at",
            field=models.DateField(
                blank=True, null=True, verbose_name="political position checked at"
            ),
        ),
        migrations.AddField(
            model_name="person",
            name="political_position_label",
            field=models.CharField(
                blank=True,
                default="",
                max_length=255,
                verbose_name="political position label",
            ),
        ),
        # Kept in its own migration, separate from the label/comment drop below, so
        # this data migration's writes to the political-position table commit before
        # the next migration's ALTER TABLE (Postgres refuses to ALTER a table with
        # pending trigger events queued in the same transaction).
        migrations.RunPython(backfill_labels, restore_labels),
    ]
