from django import forms
from django.db import models
from django.utils.translation import gettext_lazy as _

import django_filters
from elasticsearch_dsl.query import Q as ESQ

from froide.helper.search.filters import BaseSearchFilterSet
from froide.helper.widgets import DateRangeWidget

from .models import (
    Category,
    Evidence,
    InstitutionalLevel,
    Organization,
    Role,
    SocialMediaAccount,
)


def override_field_default(cls, field, overrides=None, extra=None):
    res = {**cls.FILTER_DEFAULTS[field]}

    if overrides is not None:
        res.update(overrides)
    if extra is not None:
        old_extra = res.get("extra", lambda f: {})
        res["extra"] = lambda f: {**old_extra(f), **extra(f)}
    return res


class EvidenceFilterSet(BaseSearchFilterSet):
    query_fields = ["citation", "description"]

    originator = django_filters.CharFilter(
        field_name="originator_names",
        method="filter_originator",
        label="Originator",
        widget=forms.TextInput(
            attrs={"placeholder": _("Name"), "class": "form-control"}
        ),
    )
    category = django_filters.ModelChoiceFilter(
        field_name="categories",
        queryset=Category.objects.all(),
        method="filter_id_list",
        label="Category",
    )
    platform = django_filters.ChoiceFilter(
        choices=SocialMediaAccount.Platform.choices,
        method="filter_keyword",
        label="Platform",
    )
    organization = django_filters.ModelChoiceFilter(
        field_name="originator_organizations",
        queryset=Organization.objects.all(),
        method="filter_id_list",
        label="Organization",
    )
    role = django_filters.ModelChoiceFilter(
        field_name="originator_roles",
        queryset=Role.objects.all(),
        method="filter_id_list",
        label="Role",
    )
    institutional_level = django_filters.ModelChoiceFilter(
        field_name="originator_institutional_levels",
        queryset=InstitutionalLevel.objects.all(),
        method="filter_id_list",
        label="Institutional level",
    )

    def filter_foreignkey(self, qs, name, value):
        return self.apply_filter(qs, name, **{name: value.id})

    def filter_id_list(self, qs, name, value):
        return self.apply_filter(qs, name, **{name: value.id})

    def filter_keyword(self, qs, name, value):
        return self.apply_filter(qs, name, **{name: value})

    def filter_originator(self, qs, name, value):
        if value:
            return qs.filter(
                ESQ("match", originator_names={"query": value, "operator": "and"})
            )
        return qs

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
            "originator",
            "documentation_date",
            "evidence_type",
            "category",
            "platform",
            "organization",
            "role",
            "institutional_level",
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
