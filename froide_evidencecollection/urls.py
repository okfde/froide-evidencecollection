from django.urls import path

from .views import EvidenceDetailView, EvidenceListView

urlpatterns = [
    path("", EvidenceListView.as_view(), name="evidence-list"),
    path("<int:pk>/", EvidenceDetailView.as_view(), name="evidence-detail"),
]
