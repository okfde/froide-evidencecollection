import pytest


@pytest.fixture
def fxt_mock_response():
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data

        def raise_for_status(self):
            if self.status_code != 200:
                raise Exception(f"HTTP Error: {self.status_code}")

    def _make_mock_response(json_data, status_code=200):
        return MockResponse(json_data, status_code)

    return _make_mock_response
