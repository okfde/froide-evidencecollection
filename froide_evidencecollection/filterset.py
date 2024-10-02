from django.db import models

import django_filters
from elasticsearch_dsl.query import Q as ESQ

from froide.helper.search.filters import BaseSearchFilterSet
from froide.helper.widgets import DateRangeWidget

from .models import Evidence


def override_field_default(cls, field, overrides=None, extra=None):
    res = {**cls.FILTER_DEFAULTS[field]}

    if overrides is not None:
        res.update(overrides)
    if extra is not None:
        old_extra = res.get("extra", lambda f: {})
        res["extra"] = lambda f: {**old_extra(f), **extra(f)}
    return res


class EvidenceFilterSet(BaseSearchFilterSet):
    query_fields = ["description", "note", "person_name"]

    def filter_foreignkey(self, qs, name, value):
        return self.apply_filter(qs, name, **{name: value.id})

    def filter_date_range(self, qs, name, value):
        range_kwargs = {}
        if value.start is not None:
            range_kwargs["gte"] = value.start
        if value.stop is not None:
            range_kwargs["lte"] = value.stop

        return self.apply_filter(qs, name, ESQ("range", **{name: range_kwargs}))

    class Meta:
        model = Evidence
        fields = [
            "q",
            "date",
            "type",
            "area",
            "person",
            "quality",
        ]

        filter_overrides = {
            models.DateField: override_field_default(
                BaseSearchFilterSet,
                models.DateField,
                overrides={
                    "filter_class": django_filters.DateFromToRangeFilter,
                },
                extra=lambda f: {
                    "method": "filter_date_range",
                    "widget": DateRangeWidget,
                },
            ),
            models.ForeignKey: override_field_default(
                BaseSearchFilterSet,
                models.ForeignKey,
                extra=lambda f: {
                    "widget": django_filters.widgets.LinkWidget,
                    "method": "filter_foreignkey",
                },
            ),
        }
