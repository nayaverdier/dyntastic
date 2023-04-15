import pytest
from pydantic import Field

from dyntastic import Dyntastic


def test_table_name_required():
    with pytest.raises(ValueError, match="Dyntastic table must have __table_name__ defined"):

        class MyObject(Dyntastic):
            pass

    with pytest.raises(ValueError, match="Dyntastic table must have __table_name__ defined"):

        class MyObjectWithHash(Dyntastic):
            __hash_key__ = "my_hash_key"
            my_hash_key: str

    with pytest.raises(ValueError, match="Dyntastic table must have __table_name__ defined"):

        class MyObjectWithHashAndRange(Dyntastic):
            __hash_key__ = "my_hash_key"
            __range_key__ = "my_range_key"

            my_hash_key: str
            my_range_key: str


def test_hash_key_required():
    with pytest.raises(ValueError, match="Dyntastic table must have __hash_key__ defined"):

        class MyObject(Dyntastic):
            __table_name__ = "my_object"

            my_hash_key: str

    with pytest.raises(ValueError, match="Dyntastic table must have __hash_key__ defined"):

        class MyObjectWithRange(Dyntastic):
            __table_name__ = "my_object"
            __range_key__ = "my_range_key"

            my_range_key: str


def test_key_fields_must_exist():
    with pytest.raises(ValueError, match="Dyntastic __hash_key__ is not defined as a field: 'my_hash_key'"):

        class MyObject(Dyntastic):
            __table_name__ = "my_object"
            __hash_key__ = "my_hash_key"

    with pytest.raises(ValueError, match="Dyntastic __range_key__ is not defined as a field: 'my_range_key'"):

        class MyObjectWithRange(Dyntastic):
            __table_name__ = "my_object"
            __hash_key__ = "my_hash_key"
            __range_key__ = "my_range_key"

            my_hash_key: str


def test_basic_dyntastic_instance():
    class MyObject(Dyntastic):
        __table_name__ = "my_object"
        __hash_key__ = "my_hash_key"

        my_hash_key: str

    instance = MyObject(my_hash_key="example_key")
    assert instance.dict() == {"my_hash_key": "example_key"}


def test_range_dyntastic_instance():
    class MyObject(Dyntastic):
        __table_name__ = "my_object"
        __hash_key__ = "my_hash_key"
        __range_key__ = "my_range_key"

        my_hash_key: str
        my_range_key: str

    instance = MyObject(my_hash_key="example_key", my_range_key="some_range_key")
    assert instance.dict() == {"my_hash_key": "example_key", "my_range_key": "some_range_key"}


def test_model_alias():
    class MyAliasObject(Dyntastic):
        __table_name__ = "my_alias_object"
        __hash_key__ = "my/alias"

        my_field: str = Field(..., alias="my/alias")

    instance = MyAliasObject(my_field="test")
    assert instance.dict(by_alias=True) == {"my/alias": "test"}


def test_table_host():
    class MyObject(Dyntastic):
        __table_name__ = "my_object"
        __hash_key__ = "my_hash_key"
        __table_host__ = "http://localhost:8000"

        my_hash_key: str

    assert MyObject.__table_host__ == "http://localhost:8000"
    client = MyObject._dynamodb_client()
    resource = MyObject._dynamodb_resource()
    assert client.meta.endpoint_url == "http://localhost:8000"
    assert resource.meta.client.meta.endpoint_url == "http://localhost:8000"
