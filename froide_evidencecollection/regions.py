from django.db.models import Q

from froide.georegion.models import GeoRegion


def selectable_regions():
    return GeoRegion.objects.filter(Q(kind="state") | Q(name="Deutschland")).order_by(
        "kind", "name"
    )
