from decimal import Decimal

from dyntastic import pydantic_compat


def test_save(item):
    item.my_str = "new_string"
    assert item.get(item.id, getattr(item, "timestamp", None)).my_str == "foo"
    item.save()
    assert item.my_str == "new_string"
    assert item.get(item.id, getattr(item, "timestamp", None)).my_str == "new_string"

    expected_raw = {
        "id": item.id,
        "my_str": "new_string",
        "my_datetime": "2022-02-12T12:26:35",
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
        "my_nested_model": {"sample_field": "hello"},
        "my_ipv4_address": "10.66.0.1",
        "my_ipv4_interface": "10.66.0.1/32",
        "my_ipv4_network": "10.66.0.1/32",
        "my_ipv6_address": "1:db8::",
        "my_ipv6_interface": "1:db8::/128",
        "my_ipv6_network": "2001:db8::1000/124",
    }

    if hasattr(item, "timestamp"):
        timestamp = pydantic_compat.to_jsonable_python(item.timestamp)
        expected_raw["timestamp"] = timestamp
        raw_key = {"id": item.id, "timestamp": timestamp}
        raw_item = item._dynamodb_table().get_item(Key=raw_key)["Item"]
        assert raw_item == expected_raw
    else:
        raw_key = {"id": item.id}
        raw_item = item._dynamodb_table().get_item(Key=raw_key)["Item"]
        assert raw_item == expected_raw


def test_save_different_id(item):
    original_id = (item.id, getattr(item, "timestamp", None))
    item.id = "new_id"
    new_id = ("new_id", original_id[1])

    item.save()

    assert item.get(*new_id) == item
    assert item.get(*original_id).id == original_id[0]

    item.refresh()
    assert item.id == "new_id"


def test_save_aliased_item(alias_item):
    alias_item.save()

    assert alias_item.get(alias_item.id) is not None
    raw_item = alias_item._dynamodb_table().get_item(Key={"id/alias": alias_item.id})["Item"]
    assert raw_item == {"id/alias": alias_item.id, "my_str": alias_item.my_str}
    assert type(alias_item)(**raw_item).id == alias_item.id
