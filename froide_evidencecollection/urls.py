from django.urls import path

from .views import (
    EvidenceDetailExportView,
    EvidenceDetailView,
    EvidenceExportView,
    EvidenceListView,
)

urlpatterns = [
    path("", EvidenceListView.as_view(), name="evidence-list"),
    path("export/", EvidenceExportView.as_view(), name="evidence-export"),
    path("<int:pk>/", EvidenceDetailView.as_view(), name="evidence-detail"),
    path(
        "<int:pk>/export/",
        EvidenceDetailExportView.as_view(),
        name="evidence-detail-export",
    ),
]
