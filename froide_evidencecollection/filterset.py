from django.db import models

import django_filters
from elasticsearch_dsl.query import Q as ESQ

from froide.helper.search.filters import BaseSearchFilterSet

from .models import Evidence


def override_field_default(cls, field, overrides=None, extra=None):
    res = {**cls.FILTER_DEFAULTS[field]}

    if overrides is not None:
        res.update(overrides)
    if extra is not None:
        old_extra = res.get("extra", lambda f: {})
        res["extra"] = lambda f: {**old_extra(f), **extra(f)}
    print(res)
    return res


class BrowserDateRangeWidget(django_filters.widgets.DateRangeWidget):
    def __init__(self, attrs=None):
        if attrs is None:
            attrs = {}
        attrs = {**attrs, "type": "date"}
        super().__init__(attrs)


class EvidenceFilterSet(BaseSearchFilterSet):
    query_fields = ["description", "note"]

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
                    "widget": BrowserDateRangeWidget,
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
