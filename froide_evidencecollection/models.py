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
    name = models.TextField(unique=True)

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
