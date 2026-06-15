import datetime
import uuid

from django.db import models

import pytest

from froide_evidencecollection.utils import (
    compute_hash,
    equals,
    get_base_class_name,
    get_default_value,
)


def test_get_base_class_name():
    class BaseModel(models.Model):
        pass

    class SubclassModel(BaseModel):
        pass

    class SubclassModelDirect(models.Model):
        pass

    assert get_base_class_name(SubclassModel) == "BaseModel"
    assert get_base_class_name(SubclassModel, exclude=[BaseModel]) == "SubclassModel"
    assert get_base_class_name(SubclassModelDirect) == "SubclassModelDirect"


def test_get_default_value():
    class TestModel(models.Model):
        name = models.CharField(max_length=100, default="default_name")
        age = models.IntegerField(default=30)
        yes_no = models.BooleanField(default=True)
        uuid_field = models.UUIDField()
        callable_default = models.DateField(default=datetime.date.today)

    assert get_default_value(TestModel, "name") == "default_name"
    assert get_default_value(TestModel, "age") == 30
    assert get_default_value(TestModel, "yes_no") is True
    assert get_default_value(TestModel, "uuid_field") is None
    assert get_default_value(TestModel, "callable_default") == datetime.date.today()


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
    "text,text_hash",
    [
        ("", ""),
        (
            "Christine Anderson",
            "8d049814f352759b1334aca12d2000c29691705ed84e345d8e04c87b6ac86b03",
        ),
        (
            "Irmhild Boßdorf",
            "366c3347f29f737d127f9f1c74ecfa5bc141cfa59df8bdaa89b24eb4f4cc78fd",
        ),
        (
            "Götz Frömming",
            "b86c56eb05803e2ff208af356570500bba8c046b2fe563a445f41b14ce9c93af",
        ),
        (
            "Hans-Jürgen Goßner",
            "841d9b92bda8dbdf64d8694f91d95103661bda51e7e7cf4bb6fad72e5631891e",
        ),
        (
            "Marie-Thérèse Kaiser",
            "5a9c48d541ac203c23c845e49ffd20d46e932e2cb10fe4d7f47494c3ac1f2418",
        ),
        (
            "https://t.me/frohnmaier/1303",
            "adeb45e8cf9b32e438bd50ef7b5bd9ea50e8d90ef898f18bce095f1cdba151d6",
        ),
        (
            "https://www.facebook.com/rimuehl/posts/pfbid02FV9aF8YVJNunDJz7agTzxu7C8BMAYwkitL926CVj6sJ1x1Azc9CPjDCrgWndXkkhl",
            "d39ec65c9dc87e97e20061e101fb29d4480b1c3f4c83890598967e34081c2ece",
        ),
    ],
)
def test_compute_hash(text, text_hash):
    assert compute_hash(text) == text_hash
