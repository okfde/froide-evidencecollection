import datetime
import uuid

from django.db import models

from froide_evidencecollection.utils import (
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
