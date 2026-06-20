import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('froide_evidencecollection', '0030_alter_importexportrun_source_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='evidencemention',
            name='originator',
            field=models.ForeignKey(
                on_delete=django.db.models.deletion.PROTECT,
                related_name='originated_mentions',
                to='froide_evidencecollection.actor',
                verbose_name='originator',
            ),
        ),
    ]
