# Generated by Django 5.2.1 on 2025-06-18 09:41

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('froide_evidencecollection', '0007_alter_evidence_options'),
    ]

    operations = [
        migrations.RenameModel(
            old_name='FdgoFeature',
            new_name='EvidenceCategory',
        ),
        migrations.AlterModelOptions(
            name='evidencecategory',
            options={'verbose_name': 'evidence category', 'verbose_name_plural': 'evidence categories'},
        ),
        migrations.AlterModelOptions(
            name='institution',
            options={'verbose_name': 'institution', 'verbose_name_plural': 'institutions'},
        ),
        migrations.RemoveField(
            model_name='evidence',
            name='fdgo_features',
        ),
        migrations.RemoveField(
            model_name='evidence',
            name='review_comment',
        ),
        migrations.RemoveField(
            model_name='evidence',
            name='submission_comment',
        ),
        migrations.RemoveField(
            model_name='personororganization',
            name='review_comment',
        ),
        migrations.RemoveField(
            model_name='source',
            name='review_comment',
        ),
        migrations.AddField(
            model_name='evidence',
            name='categories',
            field=models.ManyToManyField(blank=True, to='froide_evidencecollection.evidencecategory', verbose_name='evidence categories'),
        ),
        migrations.AlterField(
            model_name='affiliation',
            name='institution',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='froide_evidencecollection.institution', verbose_name='institution'),
        ),
    ]
