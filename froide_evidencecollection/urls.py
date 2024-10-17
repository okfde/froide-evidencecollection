from django.urls import path

from .views import EvidenceDetailView, EvidenceExportView, EvidenceListView

urlpatterns = [
    path("", EvidenceListView.as_view(), name="evidence-list"),
    path("export/", EvidenceExportView.as_view(), name="evidence-export"),
    path("<int:pk>/", EvidenceDetailView.as_view(), name="evidence-detail"),
]
