from django.db import migrations

from froide_evidencecollection.utils import compute_hash


def populate_hashes(apps, schema_editor):
    Person = apps.get_model('froide_evidencecollection', 'Person')
    Organization = apps.get_model('froide_evidencecollection', 'Organization')
    Evidence = apps.get_model('froide_evidencecollection', 'Evidence')

    for person in Person.objects.all():
        name = f"{person.first_name} {person.last_name}"
        person.name_hash = compute_hash(name)
        person.save(update_fields=["name_hash"])

    for org in Organization.objects.all():
        org.name_hash = compute_hash(org.organization_name)
        org.save(update_fields=["name_hash"])

    for evidence in Evidence.objects.all():
        evidence.url_hash = compute_hash(evidence.reference_url)
        evidence.save(update_fields=["url_hash"])


class Migration(migrations.Migration):

    dependencies = [
        ('froide_evidencecollection', '0023_organization_name_hash_person_name_hash'),
    ]

    operations = [
        migrations.RunPython(populate_hashes, migrations.RunPython.noop),
    ]