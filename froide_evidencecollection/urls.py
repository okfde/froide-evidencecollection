from django.urls import path
from django.utils.translation import pgettext_lazy

from .views import (
    ActorDetailExportView,
    ActorDetailView,
    EvidenceDetailExportView,
    EvidenceDetailView,
    EvidenceTopicCloudView,
)

urlpatterns = [
    path("cloud/", EvidenceTopicCloudView.as_view(), name="evidence-topic-cloud"),
    path(
        pgettext_lazy("url part", "actor/<int:pk>/"),
        ActorDetailView.as_view(),
        name="actor-detail",
    ),
    path(
        pgettext_lazy("url part", "actor/<int:pk>/export/"),
        ActorDetailExportView.as_view(),
        name="actor-detail-export",
    ),
    path(
        pgettext_lazy("url part", "evidence/<slug:slug>/"),
        EvidenceDetailView.as_view(),
        name="evidence-detail",
    ),
    path(
        pgettext_lazy("url part", "evidence/<slug:slug>/export/"),
        EvidenceDetailExportView.as_view(),
        name="evidence-detail-export",
    ),
]
