import re
from typing import Optional, Type

import botocore.exceptions
import pytest

from dyntastic import A, Dyntastic, transaction
from dyntastic.transact import TRANSACTION_MAX_ITEMS, _transaction_writer_var


class _Table(Dyntastic):
    __table_name__ = "table"
    __hash_key__ = "hash_key"

    hash_key: str
    data: Optional[str] = None


@pytest.fixture
def Table():
    _Table.create_table(wait=True)
    return _Table


class _Table2(Dyntastic):
    __table_name__ = "table2"
    __hash_key__ = "hash_key"

    hash_key: str
    data: Optional[str] = None


@pytest.fixture
def Table2():
    _Table2.create_table(wait=True)
    return _Table2


@pytest.mark.parametrize("auto_commit", [True, False])
def test_no_items_exits_cleanly(auto_commit: bool):
    with transaction(auto_commit=auto_commit):
        assert isinstance(_transaction_writer_var.get(), transaction)

    assert _transaction_writer_var.get() is None


def test_multiple_regions_fails(Table: Type[_Table]):
    class Table2(Dyntastic):
        __table_name__ = "table2"
        __table_region__ = "us-west-2"
        __hash_key__ = "hash_key"

        hash_key: str

    Table2.create_table(wait=True)

    with transaction():
        Table(hash_key="foo").save()
        with pytest.raises(
            AssertionError,
            match=re.escape("All tables in a transaction must be in the same region (found None and us-west-2)"),
        ):
            Table2(hash_key="foo").save()

    assert _transaction_writer_var.get() is None


def test_item_limit_exceeded_without_auto_commit(Table: Type[_Table]):
    with pytest.raises(Exception, match=re.escape(f"Exceeded max items ({TRANSACTION_MAX_ITEMS}) in transaction")):
        with transaction():
            for i in range(TRANSACTION_MAX_ITEMS):
                Table(hash_key=f"foo{i}").save()

            Table(hash_key=f"foo{TRANSACTION_MAX_ITEMS + 1}").save()

    assert _transaction_writer_var.get() is None

    # No items should have been written since exception was raised
    assert not list(Table.scan())


def test_item_default_limit_exceeded_with_auto_commit(Table: Type[_Table]):
    with transaction(auto_commit=True) as w:
        for i in range(TRANSACTION_MAX_ITEMS):
            Table(hash_key=f"foo{i}").save()

        assert len(list(Table.scan())) == 100
        assert w.batches_submitted == 1
        assert w.items == []

        Table(hash_key=f"foo{TRANSACTION_MAX_ITEMS + 1}").save()

    assert _transaction_writer_var.get() is None

    assert len(list(Table.scan())) == 101


def test_item_custom_limit_exceeded_with_auto_commit(Table: Type[_Table]):
    with transaction(auto_commit=True, commit_every=50) as w:
        for i in range(50):
            Table(hash_key=f"foo{i}").save()

        assert len(list(Table.scan())) == 50
        assert w.batches_submitted == 1
        assert w.items == []

        Table(hash_key=f"foo{51}").save()

    assert _transaction_writer_var.get() is None

    assert len(list(Table.scan())) == 51


def test_commit_every_exceeds_max_items(Table: Type[_Table]):
    with pytest.raises(
        Exception,
        match=re.escape(f"commit_every cannot exceed DynamoDB limit of {TRANSACTION_MAX_ITEMS} items"),
    ):
        with transaction(auto_commit=True, commit_every=101):
            pass

    assert _transaction_writer_var.get() is None


def test_multiple_operations_on_same_item_errors(Table: Type[_Table]):
    # TODO: it seems moto deviates from AWS: moto allows a save and
    # update on the same key, but AWS does not

    with pytest.raises(
        botocore.exceptions.ClientError,
        match=re.escape("Transaction request cannot include multiple operations on one item"),
    ):
        with transaction():
            item = Table(hash_key="foo")
            item.save()
            item.save()

    assert _transaction_writer_var.get() is None

    assert not list(Table.scan())


def test_manual_commit(Table: Type[_Table]):
    with transaction() as w:
        item = Table(hash_key="foo")
        item.save()

        w.commit()
        assert w.batches_submitted == 1
        assert w.items == []

        items = list(Table.scan())
        assert len(items) == 1
        assert items[0].hash_key == "foo"
        assert items[0].data is None

        item.data = "some_data"
        item.save()

    assert _transaction_writer_var.get() is None

    items = list(Table.scan())
    assert len(items) == 1
    assert items[0].data == "some_data"
    assert w.batches_submitted == 2
    assert w.items == []


def test_batch_writer_errors_during_transaction(Table: Type[_Table]):
    with transaction():
        item = Table(hash_key="foo")
        item.save()

        with pytest.raises(ValueError, match=re.escape("Cannot use batch_writer() and transaction() at the same time")):
            with Table.batch_writer():
                item.update(A.data.set("bar"), refresh=False)

    assert _transaction_writer_var.get() is None

    items = list(Table.scan())
    assert len(items) == 1
    assert items[0].hash_key == "foo"
    assert items[0].data is None


def test_transaction_condition_errors_outside_of_transaction(Table: Type[_Table]):
    with pytest.raises(
        Exception, match=re.escape("Cannot use _Table.transaction_condition() outside of a transaction")
    ):
        Table(hash_key="foo").transaction_condition(A.hash_key.exists())


def test_update_warns_on_refresh(Table: Type[_Table]):
    item = Table(hash_key="foo")
    item.save()

    with transaction():
        with pytest.warns(UserWarning, match=re.escape("Cannot refresh model in transaction, skipping refresh")):
            item.update(A.data.set("bar"), refresh=True)

    assert _transaction_writer_var.get() is None

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Dyntastic instance was not refreshed after update. "
            "Call refresh(), or use ignore_unrefreshed() to ignore safety checks"
        ),
    ):
        item.data

    items = list(Table.scan())
    assert len(items) == 1
    assert items[0].hash_key == "foo"
    assert items[0].data == "bar"


@pytest.mark.parametrize("condition", [None, A.hash_key.not_exists()])
@pytest.mark.parametrize("condition_check", [None, A.hash_key.exists(), A.hash_key == "foo"])
def test_save_single_table(Table: Type[_Table], condition, condition_check):
    with transaction() as w:
        item = Table(hash_key="foo")
        item.save(condition=condition)
        if condition_check:
            item.transaction_condition(condition_check)

    assert _transaction_writer_var.get() is None

    assert len(list(Table.scan())) == 1
    assert w.batches_submitted == 1
    assert w.items == []


@pytest.mark.parametrize("condition", [None, A.hash_key.not_exists()])
@pytest.mark.parametrize("condition_check", [None, A.hash_key.exists(), (A.hash_key == "foo") | (A.hash_key == "bar")])
def test_save_multiple_tables(Table: Type[_Table], Table2: Type[_Table2], condition, condition_check):
    with transaction() as w:
        item1 = Table(hash_key="foo")
        item1.save(condition=condition)
        item2 = Table2(hash_key="bar")
        item2.save(condition=condition)

        if condition_check:
            item1.transaction_condition(condition_check)
            item2.transaction_condition(condition_check)

    assert _transaction_writer_var.get() is None

    assert len(list(Table.scan())) == 1
    assert len(list(Table2.scan())) == 1
    assert w.batches_submitted == 1
    assert w.items == []


TRANSACTION_CANCELED_BASE_ERROR = (
    "An error occurred (TransactionCanceledException) when calling the "
    "TransactWriteItems operation: Transaction cancelled, please refer "
    "cancellation reasons for specific reasons"
)
SINGLE_CONDITION_FAILED_ERROR = re.escape(TRANSACTION_CANCELED_BASE_ERROR + " [ConditionalCheckFailed]")


def test_save_single_table_failing_condition(Table: Type[_Table]):
    with pytest.raises(
        botocore.exceptions.ClientError,
        match=SINGLE_CONDITION_FAILED_ERROR,
    ):
        with transaction():
            Table(hash_key="foo").save(condition=A.hash_key.exists())

    assert _transaction_writer_var.get() is None
    assert not list(Table.scan())


def test_save_single_table_failing_condition_check(Table: Type[_Table]):
    with pytest.raises(
        botocore.exceptions.ClientError,
        match=re.escape(TRANSACTION_CANCELED_BASE_ERROR + " [None, ConditionalCheckFailed]"),
    ):
        with transaction():
            item = Table(hash_key="foo")
            item.save()
            item2 = Table(hash_key="foo2")
            item2.transaction_condition(A.hash_key.exists())

    assert _transaction_writer_var.get() is None
    assert not list(Table.scan())


def test_save_multiple_tables_failing_condition(Table: Type[_Table], Table2: Type[_Table]):
    with pytest.raises(
        botocore.exceptions.ClientError,
        match=re.escape(TRANSACTION_CANCELED_BASE_ERROR + " [ConditionalCheckFailed, ConditionalCheckFailed]"),
    ):
        with transaction():
            Table(hash_key="foo").save(condition=A.hash_key.exists())
            Table2(hash_key="bar").save(condition=A.hash_key.exists())

    assert _transaction_writer_var.get() is None
    assert not list(Table.scan())


def test_save_multiple_tables_failing_condition_check(Table: Type[_Table], Table2: Type[_Table]):
    with pytest.raises(
        botocore.exceptions.ClientError,
        match=re.escape(TRANSACTION_CANCELED_BASE_ERROR + " [None, None, ConditionalCheckFailed]"),
    ):
        with transaction():
            Table(hash_key="foo").save()
            Table2(hash_key="foo2").save()
            item3 = Table(hash_key="bar")
            item3.transaction_condition(A.hash_key.exists())

    assert _transaction_writer_var.get() is None
    assert not list(Table.scan())


@pytest.mark.parametrize("condition", [None, A.hash_key.exists()])
@pytest.mark.parametrize("condition_check", [None, A.hash_key.not_exists()])
def test_update_single_table(Table: Type[_Table], condition, condition_check):
    Table(hash_key="foo").save()

    with transaction():
        item = Table.get("foo")
        item.update(A.data.set("bar"), condition=condition, refresh=False)
        if condition_check:
            Table(hash_key="foo2").transaction_condition(condition_check)

    assert _transaction_writer_var.get() is None

    items = list(Table.scan())
    assert len(items) == 1
    assert items[0].hash_key == "foo"
    assert items[0].data == "bar"


def test_update_single_table_failing_condition(Table: Type[_Table]):
    Table(hash_key="foo").save()

    with pytest.raises(
        botocore.exceptions.ClientError,
        match=SINGLE_CONDITION_FAILED_ERROR,
    ):
        with transaction():
            item = Table.get("foo")
            item.update(A.data.set("bar"), condition=A.hash_key.not_exists(), refresh=False)

    assert _transaction_writer_var.get() is None

    items = list(Table.scan())
    assert len(items) == 1
    assert items[0].hash_key == "foo"
    assert items[0].data is None


def test_update_single_table_failing_condition_check(Table: Type[_Table]):
    Table(hash_key="foo").save()

    with pytest.raises(
        botocore.exceptions.ClientError,
        match=re.escape(TRANSACTION_CANCELED_BASE_ERROR + " [None, ConditionalCheckFailed]"),
    ):
        with transaction():
            item = Table.get("foo")
            item.update(A.data.set("bar"), refresh=False)
            Table(hash_key="foo2").transaction_condition(A.hash_key.exists())

    assert _transaction_writer_var.get() is None

    items = list(Table.scan())
    assert len(items) == 1
    assert items[0].hash_key == "foo"
    assert items[0].data is None


@pytest.mark.parametrize("condition", [None, A.hash_key.exists()])
@pytest.mark.parametrize("condition_check", [None, A.hash_key.not_exists()])
def test_update_multiple_tables(Table: Type[_Table], Table2: Type[_Table2], condition, condition_check):
    Table(hash_key="foo").save()
    Table2(hash_key="bar").save()

    with transaction():
        item1 = Table.get("foo")
        item2 = Table2.get("bar")
        item1.update(A.data.set("data1"), condition=condition, refresh=False)
        item2.update(A.data.set("data2"), condition=condition, refresh=False)
        if condition_check:
            Table(hash_key="foo2").transaction_condition(condition_check)

    assert _transaction_writer_var.get() is None

    items = list(Table.scan())
    assert len(items) == 1
    assert items[0].hash_key == "foo"
    assert items[0].data == "data1"

    items2 = list(Table2.scan())
    assert len(items2) == 1
    assert items2[0].hash_key == "bar"
    assert items2[0].data == "data2"


def test_update_multiple_tables_failing_condition(Table: Type[_Table], Table2: Type[_Table2]):
    Table(hash_key="foo").save()
    Table2(hash_key="bar").save()

    with pytest.raises(
        botocore.exceptions.ClientError,
        match=re.escape(TRANSACTION_CANCELED_BASE_ERROR + " [ConditionalCheckFailed, ConditionalCheckFailed]"),
    ):
        with transaction():
            item1 = Table.get("foo")
            item2 = Table2.get("bar")
            item1.update(A.data.set("bar"), condition=A.hash_key.not_exists(), refresh=False)
            item2.update(A.data.set("bar"), condition=A.hash_key.not_exists(), refresh=False)

    assert _transaction_writer_var.get() is None

    items = list(Table.scan())
    assert len(items) == 1
    assert items[0].hash_key == "foo"
    assert items[0].data is None

    items2 = list(Table2.scan())
    assert len(items2) == 1
    assert items2[0].hash_key == "bar"
    assert items2[0].data is None


def test_update_multiple_tables_failing_condition_check(Table: Type[_Table], Table2: Type[_Table2]):
    Table(hash_key="foo").save()
    Table2(hash_key="bar").save()

    with pytest.raises(
        botocore.exceptions.ClientError,
        match=re.escape(TRANSACTION_CANCELED_BASE_ERROR + " [None, None, ConditionalCheckFailed]"),
    ):
        with transaction():
            item1 = Table.get("foo")
            item2 = Table2.get("bar")
            item1.update(A.data.set("bar"), refresh=False)
            item2.update(A.data.set("bar2"), refresh=False)
            Table(hash_key="foo2").transaction_condition(A.hash_key.exists())

    assert _transaction_writer_var.get() is None

    items = list(Table.scan())
    assert len(items) == 1
    assert items[0].hash_key == "foo"
    assert items[0].data is None

    items2 = list(Table2.scan())
    assert len(items2) == 1
    assert items2[0].hash_key == "bar"
    assert items2[0].data is None


@pytest.mark.parametrize("condition", [None, A.hash_key.exists()])
@pytest.mark.parametrize("condition_check", [None, A.hash_key.not_exists()])
def test_delete_single_table(Table: Type[_Table], condition, condition_check):
    Table(hash_key="foo").save()

    with transaction():
        item = Table.get("foo")
        item.delete(condition=condition)
        if condition_check:
            Table(hash_key="foo2").transaction_condition(condition_check)

    assert _transaction_writer_var.get() is None

    assert not list(Table.scan())


def test_delete_single_table_failing_condition(Table: Type[_Table]):
    Table(hash_key="foo").save()

    with pytest.raises(
        botocore.exceptions.ClientError,
        match=SINGLE_CONDITION_FAILED_ERROR,
    ):
        with transaction():
            item = Table.get("foo")
            item.delete(condition=A.hash_key.not_exists())

    assert _transaction_writer_var.get() is None

    items = list(Table.scan())
    assert len(items) == 1
    assert items[0].hash_key == "foo"
    assert items[0].data is None


def test_delete_single_table_failing_condition_check(Table: Type[_Table]):
    Table(hash_key="foo").save()

    with pytest.raises(
        botocore.exceptions.ClientError,
        match=re.escape(TRANSACTION_CANCELED_BASE_ERROR + " [None, ConditionalCheckFailed]"),
    ):
        with transaction():
            item = Table.get("foo")
            item.delete()
            Table(hash_key="foo2").transaction_condition(A.hash_key.exists())

    assert _transaction_writer_var.get() is None

    items = list(Table.scan())
    assert len(items) == 1
    assert items[0].hash_key == "foo"
    assert items[0].data is None


@pytest.mark.parametrize("condition", [None, A.hash_key.exists()])
@pytest.mark.parametrize("condition_check", [None, A.hash_key.not_exists()])
def test_delete_multiple_tables(Table: Type[_Table], Table2: Type[_Table2], condition, condition_check):
    Table(hash_key="foo").save()
    Table2(hash_key="bar").save()

    with transaction():
        item1 = Table.get("foo")
        item2 = Table2.get("bar")
        item1.delete(condition=condition)
        item2.delete(condition=condition)
        if condition_check:
            Table(hash_key="foo2").transaction_condition(condition_check)

    assert _transaction_writer_var.get() is None

    assert not list(Table.scan())
    assert not list(Table2.scan())


def test_delete_multiple_tables_failing_condition(Table: Type[_Table], Table2: Type[_Table2]):
    item = Table(hash_key="foo")
    item.save()
    item2 = Table2(hash_key="bar")
    item2.save()

    with pytest.raises(
        botocore.exceptions.ClientError,
        match=re.escape(TRANSACTION_CANCELED_BASE_ERROR + " [ConditionalCheckFailed, ConditionalCheckFailed]"),
    ):
        with transaction():
            item.delete(condition=A.hash_key.not_exists())
            item2.delete(condition=A.hash_key.not_exists())

    assert _transaction_writer_var.get() is None

    items = list(Table.scan())
    assert len(items) == 1
    assert items[0].hash_key == "foo"
    assert items[0].data is None

    items2 = list(Table2.scan())
    assert len(items2) == 1
    assert items2[0].hash_key == "bar"
    assert items2[0].data is None


def test_delete_multiple_tables_failing_condition_check(Table: Type[_Table], Table2: Type[_Table2]):
    item = Table(hash_key="foo")
    item.save()
    item2 = Table2(hash_key="bar")
    item2.save()

    with pytest.raises(
        botocore.exceptions.ClientError,
        match=re.escape(TRANSACTION_CANCELED_BASE_ERROR + " [None, None, ConditionalCheckFailed]"),
    ):
        with transaction():
            item.delete()
            item2.delete()
            Table(hash_key="foo2").transaction_condition(A.hash_key.exists())

    assert _transaction_writer_var.get() is None

    items = list(Table.scan())
    assert len(items) == 1
    assert items[0].hash_key == "foo"
    assert items[0].data is None

    items2 = list(Table2.scan())
    assert len(items2) == 1
    assert items2[0].hash_key == "bar"
    assert items2[0].data is None
