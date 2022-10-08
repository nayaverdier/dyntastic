from datetime import datetime

import botocore
import pytest

from dyntastic import A


def test_zero_page_size_errors(populated_model):
    with pytest.raises(
        botocore.exceptions.ParamValidationError, match="valid min value: 1|Invalid range for parameter Limit, value: 0"
    ):
        list(populated_model.query("id1", per_page=0))


@pytest.mark.parametrize("per_page", [1, 2])
def test_query_pager(per_page, populated_model):
    for hash_key in ["id1", "id2", "id3", "id4"]:
        results = list(populated_model.query(hash_key, per_page=per_page))
        assert len(results) == 1
        assert results[0].id == hash_key


@pytest.mark.parametrize("per_page", [1, 2, 3])
def test_query_pager_range(per_page, populated_range_model):
    for hash_key in ["id1", "id2"]:
        results = list(populated_range_model.query(hash_key, per_page=per_page))
        assert len(results) == 2
        assert results[0].id == hash_key
        assert results[1].id == hash_key


def test_query_with_condition(populated_range_model):
    results = list(populated_range_model.query("id1", filter_condition=A.my_int == 1))
    assert len(results) == 1


@pytest.mark.parametrize(
    "range_condition,expected_count",
    [
        (A.timestamp > datetime(2022, 2, 12), 1),
        (A.timestamp >= datetime(2022, 2, 12), 2),
        (A.timestamp > datetime(2022, 2, 13), 0),
        (A.timestamp >= datetime(2022, 2, 13), 1),
        (A.timestamp < datetime(2022, 2, 14), 2),
        (A.timestamp <= datetime(2022, 2, 13), 2),
        (A.timestamp < datetime(2022, 2, 13), 1),
        (A.timestamp <= datetime(2022, 2, 12), 1),
        (A.timestamp < datetime(2022, 2, 12), 0),
        (A.timestamp == datetime(2022, 2, 12), 1),
        (A.timestamp == datetime(2022, 2, 14), 0),
        # note: key expressions do not support !=
        (A.timestamp.begins_with("2022"), 2),
        (A.timestamp.begins_with("2022-02-1"), 2),
        (A.timestamp.begins_with("2022-02-13"), 1),
        (A.timestamp.between("2022", "2023"), 2),
        (A.timestamp.between("2023", "2024"), 0),
        (A.timestamp.between("2022-02-12", "2022-02-12T01"), 1),
    ],
)
def test_query_range_condition(range_condition, expected_count, populated_range_model):
    results = list(populated_range_model.query("id1", range_key_condition=range_condition))
    assert len(results) == expected_count
    assert all(item.id == "id1" for item in results)


@pytest.mark.parametrize("str_value", ["str_1", "str_2"])
@pytest.mark.parametrize("per_page", [1, 2, 3])
def test_query_index_pager(str_value, per_page, populated_model):
    results = list(populated_model.query(A.my_str == str_value, index="my_str-index", per_page=per_page))
    assert len(results) == 2
    assert all(item.my_str == str_value for item in results)


@pytest.mark.parametrize(
    "range_condition,expected_count",
    [
        (A.my_int > 1, 1),
        (A.my_int >= 1, 2),
        (A.my_int > 2, 0),
        (A.my_int >= 2, 1),
        (A.my_int < 3, 2),
        (A.my_int <= 2, 2),
        (A.my_int < 2, 1),
        (A.my_int <= 1, 1),
        (A.my_int < 1, 0),
        (A.my_int == 1, 1),
        (A.my_int == 3, 0),
    ],
)
def test_query_ranged_index_range_condition(range_condition, expected_count, populated_range_model):
    results = list(
        populated_range_model.query(
            A.my_str == "str_1",
            range_key_condition=range_condition,
            index="my_str_my_int-index",
        )
    )
    assert len(results) == expected_count
    assert all(item.my_str == "str_1" for item in results)


@pytest.mark.parametrize("str_value", ["str_1", "str_2"])
@pytest.mark.parametrize("per_page", [1, 2, 3])
def test_query_ranged_index_pager(str_value, per_page, populated_model):
    results = list(populated_model.query(A.my_str == str_value, index="my_str_my_int-index", per_page=per_page))
    assert len(results) == 2
    assert all(item.my_str == str_value for item in results)

    results = list(populated_model.query(A.my_str == str_value, index="my_str_my_int-index", per_page=per_page))
    assert len(results) == 2
    assert all(item.my_str == str_value for item in results)


def test_index_query_must_use_explicit_condition(populated_model):
    with pytest.raises(
        ValueError, match="Must specify attribute condition for index, e.g. A.my_index_hash_key == 'example_value'"
    ):
        list(populated_model.query("str_1", index="my_str_my_int-index"))


def test_cannot_query_consistently_on_index(populated_model):
    with pytest.raises(ValueError, match="Cannot perform a consistent read against a secondary index"):
        list(populated_model.query(A.my_str == "str_1", index="my_str", consistent_read=True))


def test_query_by_page(populated_range_model):
    first_page = populated_range_model.query_page("id1", per_page=1)
    assert len(first_page.items) == 1
    assert first_page.last_evaluated_key == {"id": "id1", "timestamp": "2022-02-12T00:00:00"}
    assert first_page.has_more

    assert str(first_page) == repr(first_page)
    assert "'has_more': True" in str(first_page)
    assert "'last_evaluated_key': {'id': 'id1', 'timestamp': '2022-02-12T00:00:00'}" in str(first_page)

    second_page = populated_range_model.query_page("id1", per_page=1, last_evaluated_key=first_page.last_evaluated_key)
    assert len(second_page.items) == 1
    assert second_page.last_evaluated_key is None
    assert not second_page.has_more
