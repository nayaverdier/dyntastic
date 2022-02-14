def test_save(item):
    item.my_str = "new_string"
    assert item.get(item.id, getattr(item, "timestamp", None)).my_str == "foo"
    item.save()
    assert item.my_str == "new_string"
    assert item.get(item.id, getattr(item, "timestamp", None)).my_str == "new_string"


def test_save_different_id(item):
    original_id = (item.id, getattr(item, "timestamp", None))
    item.id = "new_id"
    new_id = ("new_id", original_id[1])

    item.save()

    assert item.get(*new_id) == item
    assert item.get(*original_id).id == original_id[0]

    item.refresh()
    assert item.id == "new_id"
