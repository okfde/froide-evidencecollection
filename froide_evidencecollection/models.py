from urllib.parse import urlparse

from django.core.exceptions import ValidationError
from django.db import models
from django.urls import reverse
from django.utils.translation import gettext_lazy as _

from froide.georegion.models import GeoRegion
from froide.publicbody.models import PublicBody


class Source(models.Model):
    note = models.TextField()
    url = models.URLField(unique=True)
    document_number = models.TextField(blank=True)
    public_body = models.ForeignKey(PublicBody, on_delete=models.PROTECT, null=True)

    def clean(self):
        if (self.public_body and not self.document_number) or (
            not self.public_body and self.document_number
        ):
            raise ValidationError(
                "Either both or neither of document_number and public_body must be set"
            )
        super().clean()

    def __str__(self):
        if self.document_number:
            return f"{self.url} {self.document_number} ({self.public_body})"
        return self.url

    @property
    def domain(self) -> str:
        return urlparse(self.url).netloc


class EvidenceType(models.Model):
    name = models.TextField(unique=True)

    def __str__(self):
        return self.name


class EvidenceArea(models.Model):
    name = models.TextField(unique=True)

    def __str__(self):
        return self.name


class Institution(models.Model):
    name = models.CharField(unique=True, max_length=255, verbose_name=_("name"))

    def __str__(self):
        return self.name


class Position(models.Model):
    name = models.TextField(unique=True)
    comment = models.TextField()

    def __str__(self):
        return self.name


class Status(models.Model):
    name = models.TextField(unique=True)

    def __str__(self):
        return self.name


class Person(models.Model):
    name = models.TextField(unique=True)
    institution = models.ForeignKey(Institution, on_delete=models.PROTECT)
    highest_position = models.ForeignKey(Position, on_delete=models.PROTECT)
    georegion = models.ForeignKey(GeoRegion, on_delete=models.PROTECT)
    status = models.ForeignKey(Status, on_delete=models.PROTECT)
    note = models.TextField()

    def __str__(self):
        return self.name


class PersonOrOrganization(models.Model):
    name = models.CharField(unique=True, max_length=255, verbose_name=_("name"))
    affiliations = models.ManyToManyField(
        Institution,
        through="Affiliation",
        verbose_name=_("affiliations"),
        related_name="persons",
    )
    regions = models.ManyToManyField(GeoRegion, verbose_name=_("regions"))
    is_active = models.BooleanField(default=True, verbose_name=_("is active"))
    review_comment = models.TextField(
        null=True, blank=True, verbose_name=_("review comment")
    )

    class Meta:
        verbose_name = _("person/organization")
        verbose_name_plural = _("persons/organizations")

    def __str__(self):
        return self.name


class Function(models.Model):
    name = models.CharField(unique=True, max_length=255, verbose_name=_("name"))

    class Meta:
        verbose_name = _("function")
        verbose_name_plural = _("functions")

    def __str__(self):
        return self.name


class Affiliation(models.Model):
    person_or_organization = models.ForeignKey(
        PersonOrOrganization,
        on_delete=models.CASCADE,
        verbose_name=_("person/organization"),
    )
    institution = models.ForeignKey(
        Institution, on_delete=models.PROTECT, verbose_name=_("institution/party level")
    )
    function = models.ForeignKey(
        Function, on_delete=models.PROTECT, verbose_name=_("function")
    )

    class Meta:
        unique_together = ("person_or_organization", "institution", "function")
        verbose_name = _("affiliation")
        verbose_name_plural = _("affiliations")

    def __str__(self):
        return f"{self.person_or_organization} - {self.institution} - {self.function}"


class Quality(models.Model):
    name = models.TextField(unique=True)

    def __str__(self):
        return self.name


class Evidence(models.Model):
    date = models.DateField()
    source = models.ForeignKey(
        Source, verbose_name=_("Source"), on_delete=models.PROTECT
    )
    title = models.TextField()
    description = models.TextField()
    type = models.ForeignKey(
        EvidenceType, verbose_name=_("Evidence Type"), on_delete=models.PROTECT
    )
    area = models.ForeignKey(
        EvidenceArea, verbose_name=_("Evidence Area"), on_delete=models.PROTECT
    )
    person = models.ForeignKey(
        Person, verbose_name=_("Person"), on_delete=models.CASCADE
    )
    quality = models.ForeignKey(
        Quality, verbose_name=_("Evidence Quality"), on_delete=models.PROTECT
    )
    note = models.TextField()
    checked_on = models.DateTimeField(null=True)
    published_on = models.DateTimeField(null=True)

    def __str__(self):
        return f"{self.date}: {self.person} - {self.description}"

    def get_absolute_url(self):
        return reverse("evidencecollection:evidence-detail", kwargs={"pk": self.pk})
