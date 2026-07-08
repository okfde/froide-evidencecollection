import datetime
import uuid

import pytest

from froide_evidencecollection.utils import (
    equals,
    normalize_name,
)


def test_equals():
    assert equals(10, 10)
    assert not equals(10, 20)
    assert not equals(10, None)

    assert equals("test", "test")
    assert not equals("test", "TEST")
    assert not equals("test", None)

    date_value = datetime.date(2023, 1, 1)
    assert equals(date_value, "2023-01-01")
    assert not equals(date_value, "2023-01-02")
    assert not equals(date_value, None)

    uuid_value = uuid.uuid4()
    assert equals(uuid_value, str(uuid_value))
    assert not equals(uuid_value, str(uuid.uuid4()))
    assert not equals(uuid_value, None)

    assert equals(None, None)


@pytest.mark.parametrize(
    "a, b",
    [
        ("Thorsten Moriße", "Thorsten Morisse"),  # ß folds to ss
        ("André Barth (AfD)", "André Barth"),  # party token / parenthetical dropped
        ("Gunnar Lindemann", "gunnar  lindemann"),  # separators / case collapsed
    ],
)
def test_normalize_name_matches_variants(a, b):
    assert normalize_name(a) == normalize_name(b)
