from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("froide_evidencecollection", "0047_refactor_political_positions"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="politicalposition",
            name="comment",
        ),
        # Give `label` a DB default before dropping it so the auto-generated
        # reverse re-adds the column with that default instead of a bare NOT NULL,
        # which would fail against the segmented rows 0047 leaves behind.
        migrations.AlterField(
            model_name="politicalposition",
            name="label",
            field=models.CharField(
                default="", max_length=255, verbose_name="label"
            ),
        ),
        migrations.RemoveField(
            model_name="politicalposition",
            name="label",
        ),
        migrations.AddConstraint(
            model_name="politicalposition",
            constraint=models.UniqueConstraint(
                fields=("person", "role", "institutional_level"),
                name="unique_political_position",
            ),
        ),
    ]
