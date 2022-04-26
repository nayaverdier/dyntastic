# Changelog

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
