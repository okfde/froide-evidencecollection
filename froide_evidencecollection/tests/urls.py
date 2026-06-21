"""Minimal urlconf for tests that render the topic cloud view's full context.

The project test urlconf doesn't mount the evidencecollection app, but
`EvidenceTopicCloudView.get_context_data` reverses `evidencecollection:...`
for the reset link. Point such tests at this module via `override_settings`.
"""

from django.urls import include, path

urlpatterns = [
    path(
        "",
        include(
            ("froide_evidencecollection.urls", "evidencecollection"),
            namespace="evidencecollection",
        ),
    ),
]
