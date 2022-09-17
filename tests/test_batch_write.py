import re

import pytest

from dyntastic import A
from tests.conftest import MyObject


def test_batch_write():
    MyObject.create_table()

    with MyObject.batch_writer() as writer:
        MyObject(id="1").save()
        assert list(MyObject.scan()) == []
        second_object = MyObject(id="2")
        second_object.save()
        assert list(MyObject.scan()) == []
        MyObject(id="3").save()
        assert list(MyObject.scan()) == []

    assert len(writer) == 1

    assert len(list(MyObject.scan())) == 3
    MyObject(id="4").save()
    assert len(list(MyObject.scan())) == 4

    with MyObject.batch_writer():
        MyObject(id="5").save()
        MyObject(id="1").delete()
        second_object.delete()

    assert len(list(MyObject.scan())) == 3


def test_batch_write_with_batch_size():
    MyObject.create_table()

    with MyObject.batch_writer(batch_size=2):
        MyObject(id="1").save()
        assert list(MyObject.scan()) == []
        MyObject(id="2").save()
        assert len(list(MyObject.scan())) == 2
        MyObject(id="3").save()
        assert len(list(MyObject.scan())) == 2

    assert len(list(MyObject.scan())) == 3


def test_batch_write_with_batch_size_no_exit_submit():
    MyObject.create_table()

    with MyObject.batch_writer(batch_size=2):
        MyObject(id="1").save()
        assert list(MyObject.scan()) == []
        MyObject(id="7").save()
        assert len(list(MyObject.scan())) == 2
        MyObject(id="3").save()
        assert len(list(MyObject.scan())) == 2
        MyObject(id="4").save()
        assert len(list(MyObject.scan())) == 4

    # add coverage for skipping submit when no items in batch
    MyObject.submit_batch_write([])

    assert len(list(MyObject.scan())) == 4


def test_batch_write_save_with_conditions_errors():
    MyObject.create_table()

    with pytest.raises(
        ValueError,
        match=re.escape("Cannot provide additional arguments to MyObject.save() when using batch_writer()"),
    ):
        with MyObject.batch_writer():
            MyObject(id="1").save(condition=A.id == "1")


def test_batch_write_delete_with_conditions_errors():
    MyObject.create_table()

    with pytest.raises(
        ValueError,
        match=re.escape("Cannot provide additional arguments to MyObject.delete() when using batch_writer()"),
    ):
        with MyObject.batch_writer():
            MyObject(id="1").delete(condition=A.id == "1")
