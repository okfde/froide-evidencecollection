from django.db import migrations


ACTOR_ROLE_NAMES = [
    "posted_by",
    "mentions",
    "depicts",
    "target_of",
    "endorses",
    "opposes",
    "attributed_to",
]


EVIDENCE_ROLE_NAMES = [
    "quotes",
    "reposts",
    "replies_to",
    "refers_to",
    "contradicts",
    "supports",
    "corrects",
    "duplicates",
]


def seed_roles(apps, schema_editor):
    ActorRole = apps.get_model("froide_evidencecollection", "EvidenceActorRelationRole")
    EvidenceRole = apps.get_model(
        "froide_evidencecollection", "EvidenceRelationRole"
    )
    for name in ACTOR_ROLE_NAMES:
        ActorRole.objects.get_or_create(name=name)
    for name in EVIDENCE_ROLE_NAMES:
        EvidenceRole.objects.get_or_create(name=name)


def drop_roles(apps, schema_editor):
    ActorRole = apps.get_model("froide_evidencecollection", "EvidenceActorRelationRole")
    EvidenceRole = apps.get_model(
        "froide_evidencecollection", "EvidenceRelationRole"
    )
    ActorRole.objects.filter(name__in=ACTOR_ROLE_NAMES).delete()
    EvidenceRole.objects.filter(name__in=EVIDENCE_ROLE_NAMES).delete()


class Migration(migrations.Migration):

    dependencies = [
        ("froide_evidencecollection", "0031_evidenceactorrelationrole_evidencerelationrole_and_more"),
    ]

    operations = [
        migrations.RunPython(seed_roles, drop_roles),
    ]