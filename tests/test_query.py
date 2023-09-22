import re
from datetime import datetime

import botocore
import pytest

from dyntastic import A
from tests.conftest import MyRangeObject


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


@pytest.mark.parametrize("scan_index_forward", [True, False])
def test_query_scan_index_forward(populated_range_model, scan_index_forward):
    results = list(
        populated_range_model.query(
            "id1", range_key_condition=A.timestamp.begins_with("2022"), scan_index_forward=scan_index_forward
        )
    )

    assert len(results) == 2

    timestamp1 = results[0].timestamp
    timestamp2 = results[1].timestamp

    if scan_index_forward:
        assert timestamp1 < timestamp2
    else:
        assert timestamp2 < timestamp1


@pytest.mark.parametrize("per_page", [25, 100, 2000, None])
def test_query_with_empty_pages_due_to_filter(per_page):
    MyRangeObject.create_table()
    with MyRangeObject.batch_writer():
        for i in range(1000):
            MyRangeObject(
                id="id1", timestamp=datetime(2023, 8, 25, 0, 0, 0, i), my_str="str_1", unindexed_field="unindexed"
            ).save()

    MyRangeObject(
        id="id1", timestamp=datetime(2023, 8, 25, 0, 0, 1), my_str="str_2", unindexed_field="unindexed"
    ).save()

    results = list(
        MyRangeObject.query(
            "id1",
            filter_condition=A.my_str == "str_2",
            per_page=per_page,
        )
    )
    assert len(results) == 1


@pytest.fixture(params=["hash_only", "hash_and_range"])
def keys_only_model(request, populated_model_with_unindexed_field, populated_range_model_with_unindexed_field):
    if request.param == "hash_only":
        return populated_model_with_unindexed_field
    elif request.param == "hash_and_range":
        return populated_range_model_with_unindexed_field


def test_query_manually_refresh_keys_only(keys_only_model):
    result = list(keys_only_model.query(A.my_str == "str_1", index="keys-only-index"))
    assert len(result) == 2

    for item in result:
        assert item.id is not None
        assert item.my_str == "str_1"
        assert item.my_int is not None

        if keys_only_model.__range_key__ is not None:
            assert item.timestamp is not None

        for attr in ("unindexed_field", "my_str_list", "NONEXISTENT"):
            with pytest.raises(
                ValueError,
                match=re.escape(
                    "Dyntastic instance was loaded from a KEYS_ONLY or INCLUDE index. "
                    r"Call refresh() to load the full item, or pass load_full_item=True to query() or scan()"
                ),
            ):
                getattr(item, attr)
                item.unindexed_field

        item.refresh()

        for attr in ("unindexed_field", "my_str_list"):
            getattr(item, attr)


def test_query_auto_refresh_keys_only(keys_only_model):
    result = list(keys_only_model.query(A.my_str == "str_1", index="keys-only-index", load_full_item=True))
    assert len(result) == 2

    for item in result:
        assert item.id is not None
        assert item.my_str == "str_1"
        assert item.my_int is not None

        if keys_only_model.__range_key__ is not None:
            assert item.timestamp is not None

        for attr in ("unindexed_field", "my_str_list"):
            getattr(item, attr)
