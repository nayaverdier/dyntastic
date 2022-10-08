from datetime import datetime

import botocore
import pytest

from dyntastic import A


def test_zero_page_size_errors(populated_model):
    with pytest.raises(
        botocore.exceptions.ParamValidationError, match="valid min value: 1|Invalid range for parameter Limit, value: 0"
    ):
        list(populated_model.scan(per_page=0))


@pytest.mark.parametrize("value", [1, 2, 3, 4])
@pytest.mark.parametrize("per_page", [1, 2])
def test_scan_pager(value, per_page, populated_model):
    results = list(populated_model.scan(A.my_int == value, per_page=per_page))
    assert len(results) == 1
    assert results[0].my_int == value


def test_scan_pager_multiple_conditions(populated_range_model):
    results = list(populated_range_model.scan((A.my_int == 1) & (A.my_str == "str_1")))
    assert len(results) == 1
    assert results[0].my_int == 1
    assert results[0].my_str == "str_1"


@pytest.mark.parametrize(
    "condition,expected_count",
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
        (A.timestamp != datetime(2022, 2, 14), 2),
        (A.timestamp != datetime(2022, 2, 12), 1),
        (A.timestamp.begins_with("2022"), 2),
        (A.timestamp.begins_with("2022-02-1"), 2),
        (A.timestamp.begins_with("2022-02-13"), 1),
        (A.timestamp.between("2022", "2023"), 2),
        (A.timestamp.between("2023", "2024"), 0),
        (A.timestamp.between("2022-02-12", "2022-02-12T01"), 1),
    ],
)
def test_scan_range_condition(condition, expected_count, populated_range_model):
    results = list(populated_range_model.scan((A.id == "id1") & condition))
    assert len(results) == expected_count
    assert all(item.id == "id1" for item in results)


@pytest.mark.parametrize("int_values", [(1, 2), (3, 4)])
@pytest.mark.parametrize("per_page", [1, 2, 3])
def test_scan_index_pager(int_values, per_page, populated_model):
    results = list(populated_model.scan(A.my_int.is_in(int_values), index="my_str-index", per_page=per_page))
    assert len(results) == 2
    assert all(item.my_int in int_values for item in results)


def test_scan_by_page(populated_range_model):
    first_page = populated_range_model.scan_page(per_page=2)
    assert len(first_page.items) == 2
    assert first_page.last_evaluated_key == {"id": "id1", "timestamp": "2022-02-13T00:00:00"}
    assert first_page.has_more

    assert str(first_page) == repr(first_page)
    assert "'has_more': True" in str(first_page)
    assert "'last_evaluated_key': {'id': 'id1', 'timestamp': '2022-02-13T00:00:00'}" in str(first_page)

    second_page = populated_range_model.scan_page(per_page=2, last_evaluated_key=first_page.last_evaluated_key)
    assert len(second_page.items) == 2
    assert second_page.last_evaluated_key is None
    assert not second_page.has_more
