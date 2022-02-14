from dyntastic import A

from .conftest import MyObject


def test_refresh_hash(hash_item):
    retrieved = MyObject.get(hash_item.id)
    retrieved.update(A.my_str.set("new_value"))
    assert hash_item.my_str == "foo"
    assert retrieved.my_str == "new_value"

    hash_item.refresh()
    assert hash_item.my_str == "new_value"
    assert retrieved.my_str == "new_value"
