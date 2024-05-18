from unittest.mock import patch

import pytest
from pydantic import Field

from dyntastic import Dyntastic, pydantic_compat


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


def test_table_name_callable():
    class MyObject(Dyntastic):
        __table_name__ = lambda: "my_object"  # noqa: E731
        __hash_key__ = "my_hash_key"

        my_hash_key: str

    assert MyObject._resolve_table_name() == "my_object"


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
    assert pydantic_compat.model_dump(instance) == {"my_hash_key": "example_key"}


def test_range_dyntastic_instance():
    class MyObject(Dyntastic):
        __table_name__ = "my_object"
        __hash_key__ = "my_hash_key"
        __range_key__ = "my_range_key"

        my_hash_key: str
        my_range_key: str

    instance = MyObject(my_hash_key="example_key", my_range_key="some_range_key")
    assert pydantic_compat.model_dump(instance) == {"my_hash_key": "example_key", "my_range_key": "some_range_key"}


def test_model_alias():
    class MyAliasObject(Dyntastic):
        __table_name__ = "my_alias_object"
        __hash_key__ = "my/alias"

        my_field: str = Field(..., alias="my/alias")

    instance = MyAliasObject(my_field="test")
    assert pydantic_compat.model_dump(instance, by_alias=True) == {"my/alias": "test"}


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


@patch.dict("os.environ", {"DYNTASTIC_HOST": "http://localhost:8000"})
def test_table_host_env():
    class MyObject(Dyntastic):
        __table_name__ = "my_object"
        __hash_key__ = "my_hash_key"

        my_hash_key: str

    client = MyObject._dynamodb_client()
    resource = MyObject._dynamodb_resource()
    assert client.meta.endpoint_url == "http://localhost:8000"
    assert resource.meta.client.meta.endpoint_url == "http://localhost:8000"


@patch.dict("os.environ", {"DYNTASTIC_HOST": "http://some-other-host"})
def test_table_host_meta_and_env():
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


def test_table_region():
    class MyObject(Dyntastic):
        __table_name__ = "my_object"
        __hash_key__ = "my_hash_key"
        __table_region__ = "fake-region"

        my_hash_key: str

    assert MyObject.__table_region__ == "fake-region"
    client = MyObject._dynamodb_client()
    resource = MyObject._dynamodb_resource()
    assert client.meta.region_name == "fake-region"
    assert resource.meta.client.meta.region_name == "fake-region"


@patch.dict("os.environ", {"DYNTASTIC_REGION": "fake-region"})
def test_table_region_env():
    class MyObject(Dyntastic):
        __table_name__ = "my_object"
        __hash_key__ = "my_hash_key"

        my_hash_key: str

    client = MyObject._dynamodb_client()
    resource = MyObject._dynamodb_resource()
    assert client.meta.region_name == "fake-region"
    assert resource.meta.client.meta.region_name == "fake-region"


@patch.dict("os.environ", {"DYNTASTIC_REGION": "other-region"})
def test_table_region_meta_and_env():
    class MyObject(Dyntastic):
        __table_name__ = "my_object"
        __hash_key__ = "my_hash_key"
        __table_region__ = "fake-region"

        my_hash_key: str

    assert MyObject.__table_region__ == "fake-region"
    client = MyObject._dynamodb_client()
    resource = MyObject._dynamodb_resource()
    assert client.meta.region_name == "fake-region"
    assert resource.meta.client.meta.region_name == "fake-region"


def test_table_host_region_callable():
    """Disabling flake rule "E371: do not assign a lambda expression, use a def" for this test"""

    class MyObject(Dyntastic):
        __table_name__ = "my_object"
        __hash_key__ = "my_hash_key"
        __table_host__ = lambda: "http://localhost:8000"  # noqa #E371 do not assign a lambda expression, use a def
        __table_region__ = lambda: "fake-region"  # noqa #E371 do not assign a lambda expression, use a def

        my_hash_key: str

    client = MyObject._dynamodb_client()
    resource = MyObject._dynamodb_resource()
    assert client.meta.endpoint_url == "http://localhost:8000"
    assert resource.meta.client.meta.endpoint_url == "http://localhost:8000"
    assert client.meta.region_name == "fake-region"
    assert resource.meta.client.meta.region_name == "fake-region"


def test_table_with_swapped_aliases_works():
    class MyObject(Dyntastic):
        __table_name__ = "my_object"
        __hash_key__ = "my_hash_key"

        my_hash_key: str = Field(..., alias="another_field")
        another_field: str = Field(..., alias="my_hash_key")

    instance = MyObject(my_hash_key="my_hash_key", another_field="another_field")
    assert instance.another_field == "my_hash_key"
    assert instance.my_hash_key == "another_field"
    assert instance._dyntastic_key_dict == {"my_hash_key": "another_field"}


def test_table_with_duplicate_aliases_errors():
    with pytest.raises(ValueError, match="Duplicate alias 'my_hash_key' found in MyObject"):

        class MyObject1(Dyntastic):
            __table_name__ = "my_object"
            __hash_key__ = "my_hash_key"

            my_hash_key: str
            another_field: str = Field(..., alias="my_hash_key")

    with pytest.raises(ValueError, match="Duplicate alias 'my_hash_key' found in MyObject"):

        class MyObject2(Dyntastic):
            __table_name__ = "my_object"
            __hash_key__ = "my_hash_key"

            my_hash_key: str = Field(..., alias="my_hash_key")
            another_field: str = Field(..., alias="my_hash_key")

    with pytest.raises(ValueError, match="Duplicate alias 'my_hash_key' found in MyObject"):

        class MyObject3(Dyntastic):
            __table_name__ = "my_object"
            __hash_key__ = "my_hash_key"

            my_hash_key: str = Field(..., alias="my_hash_key")
            another_field: str = Field(..., alias="my_hash_key")

    with pytest.raises(ValueError, match="Duplicate alias 'some_alias' found in MyObject"):

        class MyObject4(Dyntastic):
            __table_name__ = "my_object"
            __hash_key__ = "some_alias"

            my_hash_key: str = Field(..., alias="some_alias")
            another_field: str = Field(..., alias="some_alias")
