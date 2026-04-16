from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('froide_evidencecollection', '0022_remove_legislativeperiod_reference_url_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='organization',
            name='name_hash',
            field=models.CharField(blank=True, default='', max_length=64, verbose_name='name hash'),
        ),
        migrations.AddField(
            model_name='person',
            name='name_hash',
            field=models.CharField(blank=True, default='', max_length=64, verbose_name='name hash'),
        ),
        migrations.AddField(
            model_name='evidence',
            name='url_hash',
            field=models.CharField(blank=True, default='', max_length=64, verbose_name='URL hash'),
        ),
    ]