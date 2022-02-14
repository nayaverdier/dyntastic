import pytest

from dyntastic.exceptions import DoesNotExist


def test_delete(item):
    item.delete()
    assert item.safe_get(item.id, getattr(item, "timestamp", None)) is None

    with pytest.raises(DoesNotExist):
        item.refresh()
