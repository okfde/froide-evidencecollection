from django.urls import path

from .views import EvidenceDetailView

urlpatterns = [
    path("<int:pk>/", EvidenceDetailView.as_view(), name="evidence-detail"),
]
