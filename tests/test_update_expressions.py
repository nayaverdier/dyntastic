from decimal import Decimal

import pytest

from dyntastic import A
from dyntastic.attr import translate_updates


def _assert_expression(expression, attributes, values, *update_actions):
    translated_attributes = {f"#{i}": attr for i, attr in enumerate(attributes)}
    translated_values = {f":{i}": value for i, value in enumerate(values)}

    expected = {"UpdateExpression": expression}
    if translated_attributes:
        expected["ExpressionAttributeNames"] = translated_attributes
    if translated_values:
        expected["ExpressionAttributeValues"] = translated_values

    assert translate_updates(*update_actions) == expected


def test_update_action_str():
    assert (
        str(A.my_field.set(5))
        == repr(A.my_field.set(5))
        == "_ActionSet(Expression='SET #0 = :0', Attributes={'#0': 'my_field'}, Values={':0': 5})"
    )


def test_attr_str():
    assert str(A.my_field) == repr(A.my_field) == "Attr<my_field>"
    assert str(A("my_field.nested_field")) == repr(A("my_field.nested_field")) == "Attr<my_field.nested_field>"
    assert (
        str(A("my_field.nested_field.nested2"))
        == repr(A("my_field.nested_field.nested2"))
        == "Attr<my_field.nested_field.nested2>"
    )


def test_set_value():
    _assert_expression("SET #0 = :0", ["my_str"], ["my_value"], A.my_str.set("my_value"))
    _assert_expression("SET #0.#1 = :0", ["my_dict", "my_str"], ["my_value"], A("my_dict.my_str").set("my_value"))
    _assert_expression(
        "SET #0.#1.#2 = :0",
        ["my_dict", "nested_dict", "my_str"],
        ["my_value"],
        A("my_dict.nested_dict.my_str").set("my_value"),
    )


def test_set_attribute():
    _assert_expression("SET #0 = #1", ["my_str", "my_other_str"], [], A.my_str.set(A.my_other_str))
    _assert_expression(
        "SET #0.#1 = #2", ["my_dict", "my_str", "my_other_str"], [], A("my_dict.my_str").set(A.my_other_str)
    )
    _assert_expression(
        "SET #0.#1.#2 = #3.#4.#5",
        ["my_dict1", "nested1", "my_str", "my_dict2", "nested2", "my_str2"],
        [],
        A("my_dict1.nested1.my_str").set(A("my_dict2.nested2.my_str2")),
    )


def test_set_multiple_attributes_and_values():
    _assert_expression(
        "SET #0 = #1, #2 = :0, #3.#4 = #5.#6.#7",
        ["my_str", "my_other_str", "my_int", "my_dict", "nested_str", "dict2", "nested_dict", "nested_str2"],
        [1],
        A.my_str.set(A.my_other_str),
        A.my_int.set(1),
        A("my_dict.nested_str").set(A("dict2.nested_dict.nested_str2")),
    )


def test_set_default():
    expected = ("SET #0 = if_not_exists(#1, :0)", ["my_int", "my_int"], [100])
    _assert_expression(*expected, A.my_int.set_default(100))
    _assert_expression(*expected, A.my_int.set(A.my_int.if_not_exists(100)))

    expected = ("SET #0 = if_not_exists(#1, #2)", ["my_int", "my_int", "my_other_int"], [])
    _assert_expression(*expected, A.my_int.set_default(A.my_other_int))
    _assert_expression(*expected, A.my_int.set(A.my_int.if_not_exists(A.my_other_int)))

    expected = ("SET #0.#1 = if_not_exists(#2.#3, #4)", ["my_dict", "my_int", "my_dict", "my_int", "my_other_int"], [])
    _assert_expression(*expected, A("my_dict.my_int").set_default(A.my_other_int))
    _assert_expression(*expected, A("my_dict.my_int").set(A("my_dict.my_int").if_not_exists(A.my_other_int)))


def test_if_not_exists():
    _assert_expression(
        "SET #0 = if_not_exists(#1, :0)",
        ["my_field", "my_other_field"],
        ["my_value"],
        A.my_field.set(A.my_other_field.if_not_exists("my_value")),
    )

    _assert_expression(
        "SET #0.#1 = if_not_exists(#2.#3, :0)",
        ["my_dict", "my_field", "my_other_dict", "my_other_field"],
        ["my_value"],
        A("my_dict.my_field").set(A("my_other_dict.my_other_field").if_not_exists("my_value")),
    )


def test_append():
    expected = ("SET #0 = list_append(#1, :0)", ["my_field", "my_field"], [[1, 2]])
    _assert_expression(*expected, A.my_field.append([1, 2]))
    _assert_expression(*expected, A.my_field.append((1, 2)))


def test_append_single_value():
    expected = ("SET #0 = list_append(#1, :0)", ["my_field", "my_field"], [[1]])
    _assert_expression(*expected, A.my_field.append(1))

    expected = ("SET #0.#1 = list_append(#2.#3, :0)", ["my_dict", "my_field", "my_dict", "my_field"], [[1]])
    _assert_expression(*expected, A("my_dict.my_field").append(1))


def test_list_append_two_values():
    _assert_expression(
        "SET #0 = list_append(:0, :1)",
        ["my_field"],
        [[1, 2], [3, 4]],
        A.my_field.set(A.list_append([1, 2], [3, 4])),
    )

    _assert_expression(
        "SET #0.#1 = list_append(:0, :1)",
        ["my_dict", "my_field"],
        [[1, 2], [3, 4]],
        A("my_dict.my_field").set(A.list_append([1, 2], [3, 4])),
    )


def test_set_plus_value():
    _assert_expression("SET #0 = #1 + :0", ["my_field", "my_field"], [5], A.my_field.set(A.my_field + 5))
    _assert_expression("SET #0 = :0 + #1", ["my_field", "my_field"], [5], A.my_field.set(5 + A.my_field))
    _assert_expression(
        "SET #0.#1 = #2.#3 + :0",
        ["my_dict", "my_field", "my_dict", "my_field"],
        [5],
        A("my_dict.my_field").set(A("my_dict.my_field") + 5),
    )
    _assert_expression(
        "SET #0.#1 = :0 + #2.#3",
        ["my_dict", "my_field", "my_dict", "my_field"],
        [5],
        A("my_dict.my_field").set(5 + A("my_dict.my_field")),
    )


def test_set_plus_attribute():
    _assert_expression(
        "SET #0 = #1 + #2",
        ["my_field", "first_arg", "second_arg"],
        [],
        A.my_field.set(A.first_arg + A.second_arg),
    )

    _assert_expression(
        "SET #0.#1 = #2.#3 + #4.#5",
        ["dict_field", "my_field", "first_dict", "first_arg", "second_dict", "second_arg"],
        [],
        A("dict_field.my_field").set(A("first_dict.first_arg") + A("second_dict.second_arg")),
    )


def test_set_minus_value():
    _assert_expression("SET #0 = #1 - :0", ["my_field", "my_field"], [5], A.my_field.set(A.my_field - 5))
    _assert_expression("SET #0 = :0 - #1", ["my_field", "my_field"], [5], A.my_field.set(5 - A.my_field))
    _assert_expression(
        "SET #0.#1 = #2.#3 - :0",
        ["my_dict", "my_field", "my_dict", "my_field"],
        [5],
        A("my_dict.my_field").set(A("my_dict.my_field") - 5),
    )
    _assert_expression(
        "SET #0.#1 = :0 - #2.#3",
        ["my_dict", "my_field", "my_dict", "my_field"],
        [5],
        A("my_dict.my_field").set(5 - A("my_dict.my_field")),
    )


def test_set_minus_attribute():
    _assert_expression(
        "SET #0 = #1 - #2", ["my_field", "first_arg", "second_arg"], [], A.my_field.set(A.first_arg - A.second_arg)
    )
    _assert_expression(
        "SET #0.#1 = #2.#3 - #4.#5",
        ["my_dict", "my_field", "my_dict", "my_field", "another_dict", "another_field"],
        [],
        A("my_dict.my_field").set(A("my_dict.my_field") - A("another_dict.another_field")),
    )


def test_remove_attribute():
    _assert_expression("REMOVE #0", ["my_field"], [], A.my_field.remove())
    _assert_expression(
        "REMOVE #0, #1, #2",
        ["my_field", "second_field", "third_field"],
        [],
        A.my_field.remove(),
        A.second_field.remove(),
        A.third_field.remove(),
    )


def test_remove_index():
    _assert_expression("REMOVE #0[0]", ["my_list"], [], A.my_list.remove(0))
    _assert_expression("REMOVE #0[1]", ["my_list"], [], A.my_list.remove(1))
    _assert_expression("REMOVE #0.#1[0]", ["my_dict", "my_list"], [], A("my_dict.my_list").remove(0))
    _assert_expression("REMOVE #0.#1[1]", ["my_dict", "my_list"], [], A("my_dict.my_list").remove(1))


def test_cannot_remove_non_int():
    with pytest.raises(ValueError, match="Dyntastic remove\\(\\) update must be given an int, found 'str'"):
        A.my_list.remove("asd")  # type: ignore


def test_cannot_remove_int_subclass():
    class CustomInt(int):
        def __str__(self):
            return "gibberish"

    with pytest.raises(ValueError, match="Dyntastic remove\\(\\) update must be given an int, found 'CustomInt'"):
        A.my_list.remove(CustomInt(0))


def test_remove_attribute_and_index():
    _assert_expression("REMOVE #0, #1[2]", ["my_field", "my_list"], [], A.my_field.remove(), A.my_list.remove(2))


def test_add_int_value():
    _assert_expression("ADD #0 :0", ["my_int"], [5], A.my_int.add(5))
    _assert_expression("ADD #0 :0", ["my_int"], [-5], A.my_int.add(-5))


def test_add_decimal_value():
    value = Decimal("1.2")
    _assert_expression("ADD #0 :0", ["my_decimal"], [value], A.my_decimal.add(value))
    _assert_expression("ADD #0 :0", ["my_decimal"], [-value], A.my_decimal.add(-value))


def test_add_attribute():
    _assert_expression("ADD #0 #1", ["field", "other_field"], [], A.field.add(A.other_field))
    _assert_expression(
        "ADD #0 #1, #2 :0",
        ["field", "other_field", "third_field"],
        [10],
        A.field.add(A.other_field),
        A.third_field.add(10),
    )


def test_add_single_set_element():
    expected = ("ADD #0 :0", ["my_set"], [{"a"}])
    _assert_expression(*expected, A.my_set.add("a"))
    _assert_expression(*expected, A.my_set.add(["a"]))
    _assert_expression(*expected, A.my_set.add(("a")))


def test_add_multiple_set_elements():
    expected = ("ADD #0 :0", ["my_set"], [{1, 2}])
    _assert_expression(*expected, A.my_set.add({1, 2}))
    _assert_expression(*expected, A.my_set.add([1, 2]))
    _assert_expression(*expected, A.my_set.add((1, 2)))


def test_delete_single_set_element():
    expected = ("DELETE #0 :0", ["my_set"], [{"a"}])
    _assert_expression(*expected, A.my_set.delete("a"))
    _assert_expression(*expected, A.my_set.delete(["a"]))
    _assert_expression(*expected, A.my_set.delete(("a")))


def test_delete_multiple_set_elements():
    expected = ("DELETE #0 :0", ["my_set"], [{1, 2}])
    _assert_expression(*expected, A.my_set.delete({1, 2}))
    _assert_expression(*expected, A.my_set.delete([1, 2]))
    _assert_expression(*expected, A.my_set.delete((1, 2)))


def test_delete_attribute():
    _assert_expression("DELETE #0 #1", ["my_set", "my_other_set"], [], A.my_set.delete(A.my_other_set))
