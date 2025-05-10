import pytest

from dyntastic.exceptions import DoesNotExist


def test_delete(item):
    item.delete()
    assert item.safe_get(item.id, getattr(item, "timestamp", None)) is None

    with pytest.raises(DoesNotExist):
        item.refresh()


def test_delete_alias_item(alias_item):
    alias_item.delete()
    assert alias_item.safe_get(alias_item.id) is None

    with pytest.raises(DoesNotExist):
        alias_item.refresh()
