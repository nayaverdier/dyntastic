import pytest

from dyntastic import DoesNotExist

from .conftest import MyIntObject, MyObject, MyRangeObject


def test_get_by_hash_key(hash_item):
    retrieved = MyObject.get(hash_item.id)
    safe_retrieved = MyObject.safe_get(hash_item.id)
    assert retrieved == safe_retrieved == hash_item


def test_get_by_int_hash_key(populated_int_model):
    retrieved = MyIntObject.get(1)
    safe_retrieved = MyIntObject.safe_get(1)
    assert retrieved == safe_retrieved == MyIntObject(id=1)


def test_get_with_range_key(range_item):
    retrieved = MyRangeObject.get(range_item.id, range_item.timestamp)
    safe_retrieved = MyRangeObject.safe_get(range_item.id, range_item.timestamp)
    assert retrieved == safe_retrieved == range_item


def test_range_key_required(range_item):
    with pytest.raises(ValueError, match="Must provide range_key to MyRangeObject.get\\(\\)"):
        MyRangeObject.get(range_item.id)


def test_range_key_not_expected(hash_item):
    with pytest.raises(ValueError, match="Did not expect range_key for MyObject.get\\(\\), found 'my_range_value'"):
        MyObject.get(hash_item.id, "my_range_value")


def test_get_nonexistent(hash_item, range_item):
    with pytest.raises(DoesNotExist):
        MyObject.get("nonexistent")

    with pytest.raises(ValueError, match="Must provide range_key to MyRangeObject.get\\(\\)"):
        MyRangeObject.get("nonexistent")

    with pytest.raises(DoesNotExist):
        MyRangeObject.get("nonexistent", "nonexistent_range")

    assert MyObject.safe_get("nonexistent") is None
    assert MyRangeObject.safe_get("nonexistent", "nonexistent_range") is None

    with pytest.raises(ValueError, match="Must provide range_key to MyRangeObject.get\\(\\)"):
        MyRangeObject.safe_get("nonexistent")


def test_get_nonexistent_int_hash_key(populated_int_model):
    with pytest.raises(DoesNotExist):
        MyIntObject.get(100)

    assert MyIntObject.safe_get(100) is None
