from urllib.parse import urlparse

from django.contrib.postgres.fields import ArrayField
from django.db import models
from django.urls import reverse
from django.utils.translation import gettext_lazy as _

from froide.georegion.models import GeoRegion
from froide.publicbody.models import PublicBody


class PersonOrOrganization(models.Model):
    external_id = models.PositiveIntegerField(
        unique=True, verbose_name=_("external ID")
    )
    name = models.CharField(unique=True, max_length=255, verbose_name=_("name"))
    affiliations = models.ManyToManyField(
        "Institution",
        through="Affiliation",
        verbose_name=_("affiliations"),
        related_name="persons",
    )
    regions = models.ManyToManyField(GeoRegion, verbose_name=_("regions"))
    special_regions = ArrayField(
        models.CharField(max_length=50),
        default=list,
        blank=True,
        verbose_name=_("special regions"),
    )
    is_active = models.BooleanField(default=True, verbose_name=_("is active"))
    review_comment = models.TextField(
        default="", blank=True, verbose_name=_("review comment")
    )

    class Meta:
        verbose_name = _("person/organization")
        verbose_name_plural = _("persons/organizations")

    def __str__(self):
        return self.name


class Institution(models.Model):
    name = models.CharField(unique=True, max_length=255, verbose_name=_("name"))

    class Meta:
        verbose_name = _("institution")
        verbose_name_plural = _("institutions")

    def __str__(self):
        return self.name


class Role(models.Model):
    name = models.CharField(unique=True, max_length=255, verbose_name=_("name"))

    class Meta:
        verbose_name = _("role")
        verbose_name_plural = _("roles")

    def __str__(self):
        return self.name


class Affiliation(models.Model):
    person_or_organization = models.ForeignKey(
        PersonOrOrganization,
        on_delete=models.CASCADE,
        verbose_name=_("person/organization"),
    )
    institution = models.ForeignKey(
        Institution, on_delete=models.CASCADE, verbose_name=_("institution")
    )
    role = models.ForeignKey(
        Role,
        on_delete=models.CASCADE,
        verbose_name=_("role"),
    )

    class Meta:
        unique_together = ("person_or_organization", "institution", "role")
        verbose_name = _("affiliation")
        verbose_name_plural = _("affiliations")

    def __str__(self):
        return f"{self.person_or_organization} - {self.institution} - {self.role}"


class Group(models.Model):
    external_id = models.PositiveIntegerField(
        unique=True, verbose_name=_("external ID")
    )
    name = models.CharField(unique=True, max_length=255, verbose_name=_("name"))
    members = models.ManyToManyField(
        PersonOrOrganization, verbose_name=_("members"), related_name="groups"
    )

    class Meta:
        verbose_name = _("group")
        verbose_name_plural = _("groups")

    def __str__(self):
        return self.name


class Source(models.Model):
    external_id = models.PositiveIntegerField(
        unique=True, verbose_name=_("external ID")
    )
    reference_value = models.CharField(
        unique=True, max_length=255, verbose_name=_("reference value")
    )
    persons_or_organizations = models.ManyToManyField(
        PersonOrOrganization,
        verbose_name=_("persons/organizations"),
        related_name="sources",
    )
    url = models.URLField(max_length=500, verbose_name=_("URL"))
    attribution_bases = models.ManyToManyField(
        "AttributionBasis",
        blank=True,
        verbose_name=_("attribution bases"),
        related_name="sources",
    )
    file_reference = models.CharField(
        max_length=255, default="", blank=True, verbose_name=_("file reference")
    )
    document_number = models.CharField(
        max_length=255, default="", blank=True, verbose_name=_("document number")
    )
    review_comment = models.TextField(
        default="", blank=True, verbose_name=_("review comment")
    )
    is_on_record = models.BooleanField(default=False, verbose_name=_("is on record"))
    # This field is modelled as m2m in NocoDB for convenience but really should be a foreign key.
    recorded_by = models.ForeignKey(
        PublicBody,
        null=True,
        blank=True,
        on_delete=models.SET_NULL,
        verbose_name=_("recorded by"),
        related_name="recorded_sources",
    )

    class Meta:
        verbose_name = _("source")
        verbose_name_plural = _("sources")

    def __str__(self):
        return self.reference_value

    @property
    def domain(self) -> str:
        return urlparse(self.url).netloc


class AttributionBasis(models.Model):
    name = models.CharField(max_length=255, unique=True, verbose_name=_("name"))

    class Meta:
        verbose_name = _("attribution basis")
        verbose_name_plural = _("attribution bases")

    def __str__(self):
        return self.name


class Attachment(models.Model):
    external_id = models.CharField(
        unique=True, max_length=100, verbose_name=_("external ID")
    )
    source = models.ForeignKey(
        Source,
        on_delete=models.CASCADE,
        verbose_name=_("source"),
        related_name="attachments",
    )
    title = models.CharField(max_length=255, verbose_name=_("title"))
    file = models.FileField(upload_to="attachments", verbose_name=_("file"))
    mimetype = models.CharField(max_length=100, blank=True, verbose_name=_("mimetype"))
    size = models.PositiveIntegerField(null=True, blank=True, verbose_name=_("size"))
    width = models.PositiveIntegerField(null=True, blank=True, verbose_name=_("width"))
    height = models.PositiveIntegerField(
        null=True, blank=True, verbose_name=_("height")
    )

    class Meta:
        verbose_name = _("attachment")
        verbose_name_plural = _("attachments")

    def __str__(self):
        return f"{self.source} - {self.file.name}"


class Evidence(models.Model):
    external_id = models.PositiveIntegerField(
        unique=True, verbose_name=_("external ID")
    )
    description = models.TextField(verbose_name=_("description"))
    date = models.DateField(
        null=True, blank=True, verbose_name=_("date of statement/action")
    )
    type = models.ForeignKey(
        "EvidenceType",
        null=True,
        blank=True,
        on_delete=models.PROTECT,
        verbose_name=_("evidence type"),
    )
    fdgo_features = models.ManyToManyField(
        "FdgoFeature", blank=True, verbose_name=_("FDGO features")
    )
    spread_level = models.ForeignKey(
        "SpreadLevel",
        null=True,
        blank=True,
        on_delete=models.PROTECT,
        verbose_name=_("spread level"),
    )
    distribution_channels = models.ManyToManyField(
        "DistributionChannel", blank=True, verbose_name=_("distribution channels")
    )
    sources = models.ManyToManyField(Source, verbose_name=_("sources"))
    is_verified = models.BooleanField(default=False, verbose_name=_("is verified"))
    requires_additional_review = models.BooleanField(
        default=False, verbose_name=_("requires additional review")
    )
    submission_comment = models.TextField(
        default="", blank=True, verbose_name=_("submission comment")
    )
    review_comment = models.TextField(
        default="", blank=True, verbose_name=_("review comment")
    )

    class Meta:
        verbose_name = _("piece of evidence")
        verbose_name_plural = _("pieces of evidence")

    def __str__(self):
        return self.title

    @property
    def title(self):
        s = f"{self.description}"
        if len(s) > 50:
            s = s[:50] + "..."

        return s

    @property
    def persons_or_organizations(self):
        return PersonOrOrganization.objects.filter(
            sources__in=self.sources.all()
        ).distinct()

    @property
    def public_bodies(self):
        return PublicBody.objects.filter(
            recorded_sources__in=self.sources.all()
        ).distinct()

    @property
    def attachments(self):
        return Attachment.objects.filter(source__in=self.sources.all()).distinct()

    def get_absolute_url(self):
        return reverse("evidencecollection:evidence-detail", kwargs={"pk": self.pk})


class EvidenceType(models.Model):
    name = models.CharField(unique=True, max_length=255, verbose_name=_("name"))

    class Meta:
        verbose_name = _("evidence type")
        verbose_name_plural = _("evidence types")

    def __str__(self):
        return self.name


class FdgoFeature(models.Model):
    name = models.CharField(unique=True, max_length=255, verbose_name=_("name"))

    class Meta:
        verbose_name = _("FDGO feature")
        verbose_name_plural = _("FDGO features")

    def __str__(self):
        return self.name


class SpreadLevel(models.Model):
    name = models.CharField(unique=True, max_length=255, verbose_name=_("name"))

    class Meta:
        verbose_name = _("spread level")
        verbose_name_plural = _("spread levels")

    def __str__(self):
        return self.name


class DistributionChannel(models.Model):
    name = models.CharField(unique=True, max_length=255, verbose_name=_("name"))

    class Meta:
        verbose_name = _("distribution channel")
        verbose_name_plural = _("distribution channels")

    def __str__(self):
        return self.name
