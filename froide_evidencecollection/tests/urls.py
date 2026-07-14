"""URLconf for tests that render views reversing `evidencecollection:*` URLs.

In production those URLs are mounted by the CMS apphook, so the namespace only
exists on a page the apphook is attached to. Tests that don't build a CMS page
opt into this module with `@pytest.mark.urls`.
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
