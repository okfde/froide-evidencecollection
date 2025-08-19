import textwrap
from urllib.parse import urlparse

from django.contrib.postgres.fields import ArrayField
from django.db import models
from django.urls import reverse
from django.utils.functional import cached_property
from django.utils.translation import gettext_lazy as _

from froide.georegion.models import GeoRegion


class Actor(models.Model):
    """
    Base model for actors with subclasses `Person` and `Organization`.

    It needs to have its own table so that it can be used as a foreign key
    in other models, so it cannot be an abstract model. It should not be
    instantiated directly, but only through its subclasses.
    """

    ACTOR_TYPES = [
        ("P", _("person")),
        ("O", _("organization")),
    ]
    external_id = models.PositiveIntegerField(
        unique=True, verbose_name=_("external ID")
    )
    # This field is automatically set to the actor type based on the subclass.
    actor_type = models.CharField(
        max_length=1,
        blank=True,
        choices=ACTOR_TYPES,
        verbose_name=_("actor type"),
    )
    # This field is computed from the subclasses' name fields.
    name = models.CharField(
        max_length=255,
        blank=True,
        verbose_name=_("name"),
    )
    also_known_as = ArrayField(
        models.CharField(max_length=50),
        default=list,
        blank=True,
        verbose_name=_("also known as"),
    )
    wikidata_id = models.CharField(
        max_length=20, blank=True, null=True, verbose_name=_("Wikidata ID")
    )

    class Meta:
        verbose_name = _("actor")
        verbose_name_plural = _("actors")

    def __str__(self):
        return self.name

    @cached_property
    def wikidata_url(self):
        if self.wikidata_id:
            return f"https://www.wikidata.org/wiki/{self.wikidata_id}"
        return None


class Person(Actor):
    first_name = models.CharField(
        max_length=50,
        verbose_name=_("first name"),
    )
    last_name = models.CharField(
        max_length=50,
        verbose_name=_("last name"),
    )
    title = models.CharField(
        max_length=20,
        blank=True,
        null=True,
        verbose_name=_("title"),
    )
    aw_politician_id = models.PositiveIntegerField(
        blank=True, null=True, verbose_name=_("abgeordnetenwatch.de politician ID")
    )
    status = models.ForeignKey(
        "PersonStatus", blank=True, null=True, on_delete=models.SET_NULL
    )

    class Meta:
        verbose_name = _("person")
        verbose_name_plural = _("persons")

    def save(self, *args, **kwargs):
        self.actor_type = "P"
        self.name = f"{self.title or ''} {self.first_name} {self.last_name}".strip()

        super().save(*args, **kwargs)

    @cached_property
    def aw_url(self):
        if self.aw_politician_id:
            return (
                f"https://www.abgeordnetenwatch.de/politician/{self.aw_politician_id}"
            )
        return None


class PersonStatus(models.Model):
    name = models.CharField(unique=True, max_length=50, verbose_name=_("name"))

    class Meta:
        verbose_name = _("person status")
        verbose_name_plural = _("person statuses")

    def __str__(self):
        return self.name


class Organization(Actor):
    organization_name = models.CharField(
        max_length=255,
        verbose_name=_("organization name"),
    )
    institutional_level = models.ForeignKey(
        "InstitutionalLevel",
        on_delete=models.PROTECT,
        verbose_name=_("institutional level"),
    )
    regions = models.ManyToManyField(GeoRegion, verbose_name=_("regions"))
    special_regions = ArrayField(
        models.CharField(max_length=50),
        default=list,
        blank=True,
        verbose_name=_("special regions"),
    )
    status = models.ForeignKey(
        "OrganizationStatus", blank=True, null=True, on_delete=models.SET_NULL
    )

    class Meta:
        verbose_name = _("organization")
        verbose_name_plural = _("organizations")

    def save(self, *args, **kwargs):
        self.actor_type = "O"
        self.name = self.organization_name.strip()

        super().save(*args, **kwargs)


class OrganizationStatus(models.Model):
    name = models.CharField(unique=True, max_length=50, verbose_name=_("name"))

    class Meta:
        verbose_name = _("organization status")
        verbose_name_plural = _("organization statuses")

    def __str__(self):
        return self.name


class InstitutionalLevel(models.Model):
    name = models.CharField(unique=True, max_length=255, verbose_name=_("name"))

    class Meta:
        verbose_name = _("institutional level")
        verbose_name_plural = _("institutional levels")

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
    external_id = models.PositiveIntegerField(
        unique=True, verbose_name=_("external ID")
    )
    person = models.ForeignKey(
        Person,
        on_delete=models.CASCADE,
        related_name="affiliations",
        verbose_name=_("person"),
    )
    organization = models.ForeignKey(
        Organization,
        on_delete=models.CASCADE,
        related_name="affiliations",
        verbose_name=_("organization"),
    )
    role = models.ForeignKey(
        Role,
        on_delete=models.CASCADE,
        verbose_name=_("role"),
        blank=True,
        null=True,
    )
    start_date_string = models.CharField(
        max_length=10,
        blank=True,
        null=True,
        verbose_name=_("start date (string)"),
    )
    end_date_string = models.CharField(
        max_length=10,
        blank=True,
        null=True,
        verbose_name=_("end date (string)"),
    )

    class Meta:
        verbose_name = _("affiliation")
        verbose_name_plural = _("affiliations")


class Evidence(models.Model):
    external_id = models.PositiveIntegerField(
        unique=True, verbose_name=_("external ID")
    )
    citation = models.TextField(blank=False, default="", verbose_name=_("citation"))
    description = models.TextField(
        blank=False, default="", verbose_name=_("description")
    )
    evidence_type = models.ForeignKey(
        "EvidenceType",
        null=True,
        blank=True,
        on_delete=models.PROTECT,
        verbose_name=_("evidence type"),
    )
    collections = models.ManyToManyField(
        "Collection",
        blank=True,
        verbose_name=_("collections"),
    )
    originators = models.ManyToManyField(
        Actor, verbose_name=_("originators"), related_name="originated_evidence"
    )
    related_actors = models.ManyToManyField(
        Actor, verbose_name=_("related actors"), related_name="related_evidence"
    )
    event_date = models.DateField(null=True, blank=True, verbose_name=_("event date"))
    publishing_date = models.DateField(
        null=True, blank=True, verbose_name=_("publishing date")
    )
    documentation_date = models.DateField(
        null=True, blank=True, verbose_name=_("documentation date")
    )
    reference_url = models.URLField(
        max_length=500, blank=True, null=True, verbose_name=_("reference URL")
    )
    reference_info = models.TextField(
        blank=True, default="", verbose_name=_("reference (additional information)")
    )
    primary_source_url = models.URLField(
        max_length=500, blank=True, null=True, verbose_name=_("primary source URL")
    )
    primary_source_info = models.TextField(
        blank=True,
        default="",
        verbose_name=_("primary source (additional information)"),
    )
    attribution_justification = models.TextField(
        blank=True, default="", verbose_name=_("attribution justification")
    )
    attribution_evidence = models.ManyToManyField(
        "Evidence", blank=True, verbose_name=_("attribution evidence")
    )
    attribution_problems = models.ManyToManyField(
        "AttributionProblem", blank=True, verbose_name=_("attribution problems")
    )
    comment = models.TextField(blank=True, default="", verbose_name=_("comment"))
    legal_assessment = models.PositiveIntegerField(
        choices=[
            (1, "⭐"),
            (2, "⭐⭐"),
            (3, "⭐⭐⭐"),
            (4, "⭐⭐⭐⭐"),
            (5, "⭐⭐⭐⭐⭐"),
        ],
        null=True,
        blank=True,
        verbose_name=_("legal assessment"),
    )

    class Meta:
        verbose_name = _("piece of evidence")
        verbose_name_plural = _("pieces of evidence")

    def __str__(self):
        return f"{self.external_id} - {self.title}"

    @cached_property
    def title(self):
        return textwrap.shorten(
            self.citation or self.description, width=50, placeholder="..."
        )

    @cached_property
    def domain(self) -> str:
        return urlparse(self.reference_url).netloc

    def get_absolute_url(self):
        return reverse("evidencecollection:evidence-detail", kwargs={"pk": self.pk})


class Collection(models.Model):
    name = models.CharField(max_length=255, unique=True, verbose_name=_("name"))
    description = models.TextField(
        blank=True, default="", verbose_name=_("description")
    )

    class Meta:
        verbose_name = _("collection")
        verbose_name_plural = _("collections")

    def __str__(self):
        return self.name


class Attachment(models.Model):
    external_id = models.CharField(
        unique=True, max_length=100, verbose_name=_("external ID")
    )
    evidence = models.ForeignKey(
        Evidence,
        on_delete=models.CASCADE,
        verbose_name=_("evidence"),
        related_name="attachments",
    )
    title = models.CharField(max_length=255, verbose_name=_("title"))
    file = models.FileField(
        upload_to="attachments", max_length=255, verbose_name=_("file")
    )
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
        return f"{self.evidence} - {self.file.name}"


class AttributionProblem(models.Model):
    name = models.CharField(max_length=255, unique=True, verbose_name=_("name"))

    class Meta:
        verbose_name = _("attribution problem")
        verbose_name_plural = _("attribution problems")

    def __str__(self):
        return self.name


class EvidenceType(models.Model):
    name = models.CharField(unique=True, max_length=255, verbose_name=_("name"))

    class Meta:
        verbose_name = _("evidence type")
        verbose_name_plural = _("evidence types")

    def __str__(self):
        return self.name
