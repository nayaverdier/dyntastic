# Changelog

## 0.7.0a1 2022-10-08

- Change dependency version pinning to be more flexible
- Only require `importlib_metadata` for python3.7 and earlier

## 0.6.0 2022-09-17

- Added support for `__table_name__` being a `Callable[[], str]` to allow dynamic table name
- Added support for batch reads and writes
- Fixed `consistent_read` behavior for `safe_get` (previously was always set to `True`)

## 0.5.0 2022-05-09

- Added support for multiple subclasses within one table (`get_model` function)

## 0.4.1 2022-04-26

- Fixed serialization of dynamo types when using Pydantic aliases

## 0.4.0 2022-04-26

- Fixed compatibility with Pydantic aliases

## 0.3.0 2022-04-25

- Added support for nested attribute conditions and update expressions
- Fixed bug where `refresh()` would cause nested Pydantic models to be
  converted to dictionaries instead of loaded into their models
- Added Pydantic aliases (models will all be dumped using pydantic's
  `by_alias=True` flag).

## 0.2.0 2022-04-23

**BREAKING**: Accessing attributes after calling `update(..., refresh=False)`
will trigger a ValueError. Read below for more information.

- Added built in safety for unrefreshed instances after an update. Any
  attribute accesses on an instance that was updated with `refresh=False`
  will raise a ValueError. This can be fixed by calling `refresh()` to get
  the most up-to-date data of the item, or by calling `ignore_unrefreshed()`
  to explicitly opt-in to using stale data.

## 0.1.0 2022-02-13

- Initial release
