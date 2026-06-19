import pytest

from froide_evidencecollection.models import invalidate_global_redactor


@pytest.fixture(autouse=True)
def _reset_redactor_cache():
    # The compiled global-redaction rules are cached at module level and only
    # invalidated by RedactionRule signals. A rule created in one test is rolled
    # back at its end without firing a delete signal, so reset the cache around
    # every test to keep them isolated.
    invalidate_global_redactor()
    yield
    invalidate_global_redactor()


@pytest.fixture
def fxt_mock_response():
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code
            self.url = "http://mocked.url"

        def json(self):
            return self.json_data

        def raise_for_status(self):
            if self.status_code != 200:
                raise Exception(f"HTTP Error: {self.status_code}")

    def _make_mock_response(json_data, status_code=200):
        return MockResponse(json_data, status_code)

    return _make_mock_response
