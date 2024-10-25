# Generated by Django 4.2.14 on 2024-09-11 17:20

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ("publicbody", "0049_alter_publicbody_email"),
        ("georegion", "0011_georegion_invalid_on"),
    ]

    operations = [
        migrations.CreateModel(
            name="EvidenceArea",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("name", models.TextField(unique=True)),
            ],
        ),
        migrations.CreateModel(
            name="EvidenceType",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("name", models.TextField(unique=True)),
            ],
        ),
        migrations.CreateModel(
            name="Institution",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("name", models.TextField(unique=True)),
            ],
        ),
        migrations.CreateModel(
            name="Position",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("name", models.TextField(unique=True)),
                ("comment", models.TextField()),
            ],
        ),
        migrations.CreateModel(
            name="Quality",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("name", models.TextField(unique=True)),
            ],
        ),
        migrations.CreateModel(
            name="Status",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("name", models.TextField(unique=True)),
            ],
        ),
        migrations.CreateModel(
            name="Source",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("note", models.TextField()),
                ("url", models.URLField(unique=True)),
                ("document_number", models.TextField(blank=True)),
                (
                    "public_body",
                    models.ForeignKey(
                        null=True,
                        on_delete=django.db.models.deletion.PROTECT,
                        to="publicbody.publicbody",
                    ),
                ),
            ],
        ),
        migrations.CreateModel(
            name="Person",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("name", models.TextField(unique=True)),
                ("note", models.TextField()),
                (
                    "georegion",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.PROTECT,
                        to="georegion.georegion",
                    ),
                ),
                (
                    "highest_position",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.PROTECT,
                        to="froide_evidencecollection.position",
                    ),
                ),
                (
                    "institution",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.PROTECT,
                        to="froide_evidencecollection.institution",
                    ),
                ),
                (
                    "status",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.PROTECT,
                        to="froide_evidencecollection.status",
                    ),
                ),
            ],
        ),
        migrations.CreateModel(
            name="Evidence",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("date", models.DateField()),
                ("description", models.TextField()),
                ("note", models.TextField()),
                ("checked_on", models.DateTimeField(null=True)),
                ("published_on", models.DateTimeField(null=True)),
                (
                    "area",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.PROTECT,
                        to="froide_evidencecollection.evidencearea",
                    ),
                ),
                (
                    "person",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.CASCADE,
                        to="froide_evidencecollection.person",
                    ),
                ),
                (
                    "quality",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.PROTECT,
                        to="froide_evidencecollection.quality",
                    ),
                ),
                (
                    "source",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.PROTECT,
                        to="froide_evidencecollection.source",
                    ),
                ),
                (
                    "type",
                    models.ForeignKey(
                        on_delete=django.db.models.deletion.PROTECT,
                        to="froide_evidencecollection.evidencetype",
                    ),
                ),
            ],
        ),
    ]
