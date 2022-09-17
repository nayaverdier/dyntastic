from dyntastic import Index

from .conftest import MyObject


def test_create_table():
    MyObject.create_table(wait=False)
    assert list(MyObject.scan()) == []


def test_create_table_wait():
    MyObject.create_table()
    assert list(MyObject.scan()) == []


def test_create_table_with_indexes():
    MyObject.create_table(Index("my_bytes"))
    assert list(MyObject.scan()) == []


class MyRegionObject(MyObject):
    __table_region__ = "us-east-2"


def test_create_table_region():
    MyRegionObject._clear_boto3_state()
    assert MyRegionObject._dynamodb_resource().meta.client.meta.region_name == "us-east-2"
    assert MyRegionObject._dynamodb_client().meta.region_name == "us-east-2"
    MyRegionObject.create_table()
    assert list(MyRegionObject.scan()) == []
