def test_module_import():
    import dyntastic

    assert dyntastic.__all__ == ["A", "Attr", "DoesNotExist", "Dyntastic", "Index", "__version__"]


def test_direct_import():
    from dyntastic import DoesNotExist  # noqa: F401
    from dyntastic import Dyntastic  # noqa: F401
    from dyntastic import __version__  # noqa: F401
    from dyntastic import A, Attr, Index  # noqa: F401

    assert A is Attr
