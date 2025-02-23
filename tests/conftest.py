import os
from datetime import datetime
from decimal import Decimal
from ipaddress import (
    IPv4Address,
    IPv4Interface,
    IPv4Network,
    IPv6Address,
    IPv6Interface,
    IPv6Network,
)
from typing import Any, Dict, List, Optional, Set
from uuid import uuid4

import pytest
from moto import mock_dynamodb
from pydantic import BaseModel, Field, IPvAnyAddress, IPvAnyInterface, IPvAnyNetwork

from dyntastic import Dyntastic, Index

os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(autouse=True)
def mock_dynamo():
    with mock_dynamodb():
        yield


class MyNestedModel(BaseModel):
    sample_field: str


class MyObject(Dyntastic):
    __table_name__ = "my_object"
    __hash_key__ = "id"

    id: str = Field(default_factory=lambda: str(uuid4()))
    my_str: Optional[str] = None
    my_datetime: Optional[datetime] = None
    my_bytes: Optional[bytes] = None
    my_int: Optional[int] = None
    my_decimal: Optional[Decimal] = None
    my_str_set: Optional[Set[str]] = None
    my_bytes_set: Optional[Set[bytes]] = None
    my_int_set: Optional[Set[int]] = None
    my_str_list: Optional[List[str]] = None
    my_int_list: Optional[List[int]] = None
    my_dict: Optional[dict] = None
    my_nested_data: Optional[Any] = None
    my_nested_model: Optional[MyNestedModel] = None
    my_ipv4_address: Optional[IPvAnyAddress] = None
    my_ipv4_interface: Optional[IPvAnyInterface] = None
    my_ipv4_network: Optional[IPvAnyNetwork] = None
    my_ipv6_address: Optional[IPv6Address] = None
    my_ipv6_interface: Optional[IPv6Interface] = None
    my_ipv6_network: Optional[IPv6Network] = None


class MyObjectWithRequiredField(MyObject):
    unindexed_field: str


class MyIntObject(Dyntastic):
    __table_name__ = "my_int_object"
    __hash_key__ = "id"

    id: int


# No hash key defined because this inherits from MyObject
# This class also covers testing __table_name__ being a function instead of a string
class MyRangeObject(MyObject):
    @classmethod
    def __table_name__(cls):
        return "my_range_object"

    __range_key__ = "timestamp"

    timestamp: datetime = Field(default_factory=datetime.now)


class MyRangeObjectWithRequiredField(MyRangeObject):
    unindexed_field: str


class MyAliasObject(Dyntastic):
    __table_name__ = "my_alias_object"
    __hash_key__ = "id/alias"

    id: str = Field(..., alias="id/alias")


def _create_item(DyntasticModel, **kwargs):
    DyntasticModel.create_table()
    data = {
        "my_str": "foo",
        "my_datetime": datetime(2022, 2, 12, 12, 26, 35),
        "my_bytes": b"foobar",
        "my_int": 5,
        "my_decimal": Decimal("1.5"),
        "my_str_set": {"a", "b", "c"},
        "my_bytes_set": {b"a", b"b", b"c"},
        "my_int_set": {1, 2},
        "my_str_list": ["a", "b", "c", "d"],
        "my_int_list": [10, 20, 30],
        "my_dict": {"a": 1, "b": 2, "c": 3},
        "my_nested_data": [{"a": [{"foo": "bar"}], "b": "test"}, "some_string"],
        "my_nested_model": MyNestedModel(sample_field="hello"),
        "my_ipv4_address": IPv4Address("10.66.0.1"),
        "my_ipv4_interface": IPv4Interface("10.66.0.1"),
        "my_ipv4_network": IPv4Network("10.66.0.1/32"),
        "my_ipv6_address": IPv6Address("001:db8::"),
        "my_ipv6_interface": IPv6Interface("001:db8::"),
        "my_ipv6_network": IPv6Network("2001:db8::1000/124"),
    }
    data.update(kwargs)

    item = DyntasticModel(**data)
    item.save()
    return item


params = [[MyObject], [MyRangeObject]]


@pytest.fixture
def hash_item():
    instance = _create_item(MyObject)
    yield instance
    instance._clear_boto3_state()


@pytest.fixture
def range_item():
    instance = _create_item(MyRangeObject)
    yield instance
    instance._clear_boto3_state()


@pytest.fixture(params=params)
def item(request):
    instance = _create_item(request.param[0])
    yield instance
    instance._clear_boto3_state()


@pytest.fixture
def alias_item():
    MyAliasObject.create_table()
    instance = MyAliasObject(id="foo")
    yield instance
    instance._clear_boto3_state()


@pytest.fixture(params=params)
def item_no_my_str(request):
    instance = _create_item(request.param[0], my_str=None)
    assert instance.my_str is None
    yield instance
    instance._clear_boto3_state()


@pytest.fixture(params=params)
def item_no_my_str_list(request):
    instance = _create_item(request.param[0], my_str_list=None)
    assert instance.my_str_list is None
    yield instance
    instance._clear_boto3_state()


@pytest.fixture(params=params)
def item_no_my_int(request):
    instance = _create_item(request.param[0], my_int=None)
    assert instance.my_int is None
    yield instance
    instance._clear_boto3_state()


@pytest.fixture(params=params)
def item_no_my_decimal(request):
    instance = _create_item(request.param[0], my_decimal=None)
    assert instance.my_decimal is None
    yield instance
    instance._clear_boto3_state()


@pytest.fixture(params=params)
def item_no_my_str_set(request):
    instance = _create_item(request.param[0], my_str_set=None)
    assert instance.my_str_set is None
    yield instance
    instance._clear_boto3_state()


alias_query_data: List[Dict[str, Any]] = [
    {
        "id": "id1",
    },
    {
        "id": "id2",
    },
    {
        "id": "id3",
    },
]


query_data: List[Dict[str, Any]] = [
    {
        "id": "id1",
        "my_str": "str_1",
        "my_int": 1,
    },
    {
        "id": "id2",
        "my_str": "str_1",
        "my_int": 2,
    },
    {
        "id": "id3",
        "my_str": "str_2",
        "my_int": 3,
    },
    {
        "id": "id4",
        "my_str": "str_2",
        "my_int": 4,
    },
]

range_query_data: List[Dict[str, Any]] = [
    {
        "id": "id1",
        "timestamp": datetime(2022, 2, 12),
        "my_str": "str_1",
        "my_int": 1,
    },
    {
        "id": "id1",
        "timestamp": datetime(2022, 2, 13),
        "my_str": "str_1",
        "my_int": 2,
    },
    {
        "id": "id2",
        "timestamp": datetime(2022, 2, 12),
        "my_str": "str_2",
        "my_int": 3,
    },
    {
        "id": "id2",
        "timestamp": datetime(2022, 2, 13),
        "my_str": "str_2",
        "my_int": 4,
    },
]


@pytest.fixture
def populated_alias_model(request):
    MyAliasObject.create_table("id/alias")
    for item in alias_query_data:
        MyAliasObject(**item).save()
    yield MyAliasObject
    MyAliasObject._clear_boto3_state()


@pytest.fixture
def populated_model(request):
    MyObject.create_table(
        "my_str",
        Index("my_str", "my_int"),
        Index("my_str", "my_int", index_name="keys-only-index", keys_only=True),
    )
    for item in query_data:
        MyObject(**item).save()
    yield MyObject
    MyObject._clear_boto3_state()


@pytest.fixture
def populated_model_with_unindexed_field(request):
    MyObjectWithRequiredField.create_table(
        "my_str",
        Index("my_str", "my_int"),
        Index("my_str", "my_int", index_name="keys-only-index", keys_only=True),
    )
    for item in query_data:
        MyObjectWithRequiredField(**item, unindexed_field="unindexed").save()
    yield MyObjectWithRequiredField
    MyObjectWithRequiredField._clear_boto3_state()


@pytest.fixture
def populated_int_model(request):
    MyIntObject.create_table()
    for i in range(10):
        MyIntObject(id=i).save()
    yield MyIntObject
    MyIntObject._clear_boto3_state()


@pytest.fixture
def populated_range_model(request):
    MyRangeObject.create_table(
        "my_str",
        Index("my_str", "my_int", index_name="my_str_my_int-index"),
        Index("my_str", "my_int", index_name="keys-only-index", keys_only=True),
    )
    for item in range_query_data:
        MyRangeObject(**item).save()
    yield MyRangeObject
    MyRangeObject._clear_boto3_state()


@pytest.fixture
def populated_range_model_with_unindexed_field(request):
    MyRangeObjectWithRequiredField.create_table(
        "my_str",
        Index("my_str", "my_int", index_name="my_str_my_int-index"),
        Index("my_str", "my_int", index_name="keys-only-index", keys_only=True),
    )
    for item in range_query_data:
        MyRangeObjectWithRequiredField(**item, unindexed_field="unindexed").save()
    yield MyRangeObjectWithRequiredField
    MyRangeObjectWithRequiredField._clear_boto3_state()
