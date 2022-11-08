import pytest

from .conftest import MyIntObject, MyObject, MyRangeObject, query_data, range_query_data

hash_keys = [item["id"] for item in query_data]
loaded_hash_data = [MyObject(**item) for item in query_data]

hash_range_keys = [(item["id"], item["timestamp"]) for item in range_query_data]
loaded_range_data = [MyRangeObject(**item) for item in range_query_data]


def test_get_by_hash_key(populated_model):
    assert populated_model.batch_get(hash_keys) == loaded_hash_data
    assert populated_model.batch_get([*hash_keys, "nonexistent"]) == loaded_hash_data


def test_get_by_int_hash_key(populated_int_model):
    assert populated_int_model.batch_get([1, 2, 3]) == [MyIntObject(id=1), MyIntObject(id=2), MyIntObject(id=3)]
    assert populated_int_model.batch_get([1, 2, 3, 1000]) == [MyIntObject(id=1), MyIntObject(id=2), MyIntObject(id=3)]


def test_get_by_hash_key_and_range_key(populated_range_model):
    assert populated_range_model.batch_get(hash_range_keys) == loaded_range_data
    assert populated_range_model.batch_get([*hash_range_keys, ("nonexistent", "TS")]) == loaded_range_data


def test_invalid_keys(populated_model):
    error_message = rf"Must only provide str types as `keys` to {populated_model.__name__}\.batch_get\(\)"

    with pytest.raises(ValueError, match=error_message):
        assert populated_model.batch_get([("hash", "range")]) == []

    with pytest.raises(ValueError, match=error_message):
        assert populated_model.batch_get(["good", ("hash", "range")]) == []

    with pytest.raises(ValueError, match=error_message):
        assert populated_model.batch_get(["good", ("hash", "range"), "good2"]) == []


def test_invalid_int_keys(populated_int_model):
    error_message = rf"Must only provide int types as `keys` to {populated_int_model.__name__}\.batch_get\(\)"

    with pytest.raises(ValueError, match=error_message):
        assert populated_int_model.batch_get([1, "bad"]) == []

    with pytest.raises(ValueError, match=error_message):
        assert populated_int_model.batch_get([1, "bad", 2]) == []

    with pytest.raises(ValueError, match=error_message):
        assert populated_int_model.batch_get([1, (2, "bad_range")]) == []


def test_invalid_keys_on_range_model(populated_range_model):
    error_message = (
        rf"Must provide \(hash_key, range_key\) tuples as `keys` to {populated_range_model.__name__}\.batch_get\(\)"
    )

    with pytest.raises(ValueError, match=error_message):
        assert populated_range_model.batch_get(["hash"]) == []

    with pytest.raises(ValueError, match=error_message):
        assert populated_range_model.batch_get([("good", "range"), "hash"]) == []

    with pytest.raises(ValueError, match=error_message):
        assert populated_range_model.batch_get([("missing_range",)]) == []

    with pytest.raises(ValueError, match=error_message):
        assert populated_range_model.batch_get([("hash", "range", "extra")]) == []
