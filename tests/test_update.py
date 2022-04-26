import re
from datetime import datetime
from decimal import Decimal

import botocore
import pytest

from dyntastic import A, Attr

from .conftest import MyNestedModel


def test_must_provide_actions(item):
    with pytest.raises(ValueError, match="Must provide at least one action to update"):
        item.update()


def test_set_new_attribute(item_no_my_str):
    item_no_my_str.update(A.my_str.set("bar"))
    assert item_no_my_str.my_str == "bar"


def test_set_existing_attribute(item):
    new_data = {
        "my_str": "new_value",
        "my_datetime": datetime(2022, 2, 13, 0, 0, 0),
        "my_bytes": b"new_bytes",
        "my_int": 6,
        "my_decimal": Decimal("2.5"),
        "my_str_set": {"d", "e"},
        "my_bytes_set": {b"e", b"f"},
        "my_int_set": {3, 4},
        "my_str_list": ["e", "f"],
        "my_int_list": [40, 50],
        "my_dict": {"d": 4, "e": 5},
        "my_nested_data": {"foo": ["bar", "baz", "bat"]},
        "my_nested_model": MyNestedModel(sample_field="updated"),
    }

    updates = [getattr(A, key).set(value) for key, value in new_data.items()]
    item.update(*updates)
    item.refresh()
    refreshed_data = item.dict()
    refreshed_data.pop("id")
    refreshed_data.pop("timestamp", None)
    assert refreshed_data == new_data


def test_no_refresh(item):
    item.update(A.my_str.set("bar"), refresh=False)
    with pytest.raises(
        ValueError,
        match=re.escape(
            "Dyntastic instance was not refreshed after update. "
            "Call refresh() or ignore_unrefreshed() to ignore safety checks"
        ),
    ):
        item.my_str

    # make sure these attributes always are accessible
    assert item.ConditionException
    assert item.refresh
    assert item.ignore_unrefreshed

    item.refresh()
    assert item.my_str == "bar"


def test_ignore_refresh(item):
    item.update(A.my_str.set("bar"), refresh=False)
    with pytest.raises(
        ValueError,
        match=re.escape(
            "Dyntastic instance was not refreshed after update. "
            "Call refresh() or ignore_unrefreshed() to ignore safety checks"
        ),
    ):
        item.my_str

    # make sure these attributes always are accessible
    assert item.ConditionException
    assert item.refresh
    assert item.ignore_unrefreshed

    item.ignore_unrefreshed()
    assert item.my_str == "foo"

    item.refresh()
    assert item.my_str == "bar"


def test_update_with_Attr(item):
    item.update(Attr.my_str.set("bar"))
    assert item.my_str == "bar"


def test_update_nested_string(item):
    item.update(A("my_nested_model.sample_field").set("new_value"))
    assert item.my_nested_model.sample_field == "new_value"


def test_set_and_remove_multiple_attributes(item):
    item.update(A.my_str.set("new_value"), A.my_int.set(500), A.my_decimal.remove(), A.my_dict.remove())
    assert item.my_str == "new_value"
    assert item.my_int == 500
    assert item.my_decimal is None
    assert item.my_dict is None


def test_set_if_not_exist_exists(item):
    item.update(A.my_str.set(A.my_str.if_not_exists("bar")))
    assert item.my_str == "foo"


def test_set_if_not_exist_does_not_exists(item_no_my_str):
    item_no_my_str.update(A.my_str.set(A.my_str.if_not_exists("bar")))
    assert item_no_my_str.my_str == "bar"


def test_set_default_exists(item):
    item.update(A.my_str.set_default("bar"))
    assert item.my_str == "foo"


def test_set_default_does_not_exist(item_no_my_str):
    item_no_my_str.update(A.my_str.set_default("bar"))
    assert item_no_my_str.my_str == "bar"


def test_set_int_plus(item):
    item.update(A.my_int.set(A.my_int + 100))
    assert item.my_int == 105


def test_set_int_rplus(item):
    item.update(A.my_int.set(100 + A.my_int))
    assert item.my_int == 105


def test_set_int_minus(item):
    item.update(A.my_int.set(A.my_int - 100))
    assert item.my_int == -95


def test_set_int_rminus(item):
    item.update(A.my_int.set(100 - A.my_int))
    assert item.my_int == 95


def test_list_append(item):
    item.update(A.my_str_list.set(A.my_str_list.list_append("z")))
    assert item.my_str_list == ["a", "b", "c", "d", "z"]


def test_list_append_if_not_exists(item_no_my_str_list):
    item = item_no_my_str_list

    default_list = A.my_str_list.if_not_exists(["y"])
    # TODO: fix type hinting around this pattern
    appended_list = A.list_append(default_list, "z")

    item.update(A.my_str_list.set(appended_list))
    assert item.my_str_list == ["y", "z"]


def test_append(item):
    item.update(A.my_str_list.append("z"))
    assert item.my_str_list == ["a", "b", "c", "d", "z"]


def test_remove_attribute(item):
    item.update(A.my_str.remove())
    assert item.my_str is None
    item.update(A.my_str_list.remove())
    assert item.my_str_list is None


def test_remove_list_element(item):
    item.update(A.my_str_list.remove(0))
    assert item.my_str_list == ["b", "c", "d"]


def test_add_single_string_to_set(item):
    item.update(A.my_str_set.add("z"))
    assert item.my_str_set == {"a", "b", "c", "z"}


@pytest.mark.parametrize("collection_type", [list, tuple, set])
def test_add_multiple_strings_to_set(collection_type, item):
    item.update(A.my_str_set.add(collection_type(["y", "z"])))
    assert item.my_str_set == {"a", "b", "c", "y", "z"}


def test_add_single_string_to_new_set(item_no_my_str_set):
    item = item_no_my_str_set
    item.update(A.my_str_set.add("z"))
    assert item.my_str_set == {"z"}


@pytest.mark.parametrize("collection_type", [list, tuple, set])
def test_add_multiple_strings_to_new_set(collection_type, item_no_my_str_set):
    item = item_no_my_str_set
    item.update(A.my_str_set.add(collection_type(["y", "z"])))
    assert item.my_str_set == {"y", "z"}


def test_cannot_add_int_to_set(item):
    with pytest.raises(
        botocore.exceptions.ClientError, match="An operand in the update expression has an incorrect data type"
    ):  # type: ignore
        item.update(A.my_int_set.add(5))


def test_add_int(item):
    item.update(A.my_int.add(100))
    assert item.my_int == 105


def test_add_new_int(item_no_my_int):
    item = item_no_my_int
    item.update(A.my_int.add(100))
    assert item.my_int == 100


def test_add_decimal(item):
    item.update(A.my_decimal.add(Decimal("3.25")))
    assert item.my_decimal == Decimal("4.75")


def test_add_new_decimal(item_no_my_decimal):
    item = item_no_my_decimal
    item.update(A.my_decimal.add(Decimal("100.5")))
    assert item.my_decimal == Decimal("100.5")


def test_delete_single_string_from_set(item):
    item.update(A.my_str_set.delete("a"))
    assert item.my_str_set == {"b", "c"}


@pytest.mark.parametrize("collection_type", [list, tuple, set])
def test_delete_multiple_strings(collection_type, item):
    item.update(A.my_str_set.delete(collection_type(["a", "b"])))
    assert item.my_str_set == {"c"}


def test_delete_all_strings(item):
    item.update(A.my_str_set.delete(item.my_str_set))
    assert item.my_str_set is None


@pytest.fixture(params=["success", "failure", "raise_failure"])
def condition_tester(request):
    action = A.my_str.set("new_value")

    def tester(success_case, failure_case, item):
        if request.param == "success":
            item.update(action, condition=success_case, require_condition=True)
            assert item.my_str == "new_value"
        elif request.param == "failure":
            item.update(action, condition=failure_case)
            assert item.my_str == "foo"
        else:
            with pytest.raises(item.ConditionException(), match="The conditional request failed"):
                item.update(action, condition=failure_case, require_condition=True)

            item.refresh()
            assert item.my_str == "foo"

    return tester


nested_str = A("my_nested_model.sample_field")


@pytest.mark.parametrize(
    "success,failure",
    [
        # equality (all data types to check serialization)
        (A.my_str == "foo", A.my_str == "non-matching"),
        (A.my_str.eq("foo"), A.my_str.eq("non-matching")),
        (A.my_datetime == datetime(2022, 2, 12, 12, 26, 35), A.my_datetime == datetime(2022, 3, 1)),
        (A.my_bytes == b"foobar", A.my_bytes == b"not this"),
        (A.my_int == 5, A.my_int == -1000),
        (A.my_decimal == Decimal("1.5"), A.my_decimal == Decimal("100.5")),
        (A.my_str_set == {"a", "b", "c"}, A.my_str_set == {"x", "y"}),
        (A.my_bytes_set == {b"a", b"b", b"c"}, A.my_bytes_set == {b"x", b"y"}),
        (A.my_int_set == {1, 2}, A.my_int_set == {5, 6}),
        (A.my_str_list == ["a", "b", "c", "d"], A.my_str_list == ["x", "y", "z"]),
        (A.my_int_list == [10, 20, 30], A.my_int_list == [500, 600]),
        (A.my_dict == {"a": 1, "b": 2, "c": 3}, A.my_dict == {"x": 100, "y": 200}),
        (
            A.my_nested_data == [{"a": [{"foo": "bar"}], "b": "test"}, "some_string"],
            A.my_nested_data == ["not", {"this": "data"}],
        ),
        (nested_str == "hello", nested_str == "non-matching"),
        # inequality
        (A.my_str != "non-matching", A.my_str != "foo"),
        (A.my_str.ne("non-matching"), A.my_str.ne("foo")),
        (A.my_int != 5000, A.my_int != 5),
        (nested_str != "non-matching", nested_str != "hello"),
        (nested_str.ne("non-matching"), nested_str.ne("hello")),
        # </<=/>/>=
        (A.my_int < 10, A.my_int < 0),
        (A.my_int <= 5, A.my_int <= 4),
        (A.my_str < "zzz", A.my_str < "abc"),
        (A.my_str_set < {"a", "b", "c", "d"}, A.my_str_set < {"z"}),
        (A.my_int > 0, A.my_int > 10),
        (A.my_int >= 5, A.my_int >= 6),
        (A.my_str > "fff", A.my_str > "zzz"),
        (nested_str > "he", nested_str > "hello"),
        (nested_str >= "hello", nested_str >= "hello2"),
        (nested_str < "hello2", nested_str < "hello"),
        (nested_str <= "hello", nested_str <= "he"),
        # string operators
        (A.my_str.begins_with("f"), A.my_str.begins_with("z")),
        (A.my_str.between("f", "food"), A.my_str.between("z", "zebra")),
        (A.my_str.is_in({"foo", "bar", "baz"}), A.my_str.is_in({"d", "e"})),
        (A.my_str.contains("fo"), A.my_str.contains("blah")),
        (A.my_str_set.contains("a"), A.my_str_set.contains(A.my_str)),
        (nested_str.begins_with("h"), nested_str.begins_with("z")),
        (nested_str.between("h", "hello2"), nested_str.between("h", "he")),
        (nested_str.is_in({"a", "hello", "b"}), nested_str.is_in({"a", "b"})),
        (nested_str.contains("hel"), nested_str.contains("foo")),
        (A.my_str_set.contains("a"), A.my_str_set.contains(A.my_str)),
        # attribute_type/size/existence functions
        (A.my_str.attribute_type("S"), A.my_str.attribute_type("N")),
        (A.my_str.size < 15, A.my_str.size > 20),
        (A.my_str.size == 3, ~(A.my_str.size == 3)),
        (A.my_str.exists(), A.my_str.not_exists()),
        (nested_str.attribute_type("S"), nested_str.attribute_type("N")),
        (nested_str.size < 15, nested_str.size > 20),
        (nested_str.size == 5, ~(nested_str.size == 5)),
        (nested_str.exists(), nested_str.not_exists()),
        # logical operators
        (~(A.my_str == "bar"), ~(A.my_str == "foo")),
        ((A.my_str == "foo") | (A.my_str == "bar"), (A.my_str == "foo") & (A.my_int == 10)),
    ],
)
def test_update_condition(success, failure, condition_tester, item):
    condition_tester(success, failure, item)
