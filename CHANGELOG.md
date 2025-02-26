# Changelog

## 0.17.0 2025-02-25

- Add support for ipv4 and ipv6 IP addresses (Thanks @GitToby)
- Fix handling of aliases, including bug where tables with an aliased hash key
  could not be deleted (Thanks @Nathan-Kr for reporting)

## 0.16.0 2024-12-18

- Fix double serialization of conditions inside a transaction (Thanks @krewx)
- Fix validation error due to empty ExpressionAttributeValue during transaction
  (Thanks @regoawt for reporting)

## 0.15.0 2024-05-18

- Make Dyntastic.batch_get work with keys that are aliases on the model fields.
- Improve error messages when validating keys passed to `get`, `safe_get` or `batch_get`
- Minor fixes to `batch_get` type hints

## 0.14.0 2023-12-21

- Add support for `__table_region__` and `__table_host__` to be lazy callables
- Default `__table_region__` and `__table_host__` to `DYNTASTIC_REGION` and
  `DYNTASTIC_HOST` environment variables if not otherwise defined

## 0.13.1 2023-11-21

- Fix import error when using `pydantic>=2.5`

## 0.13.0 2023-11-18

- Add support for python3.12

## 0.13.0a1 2023-11-03

- Add support for pydantic v2

## 0.12.0 2023-09-22

- Support KEYS_ONLY and INCLUDE DynamoDB indexes

## 0.11.0 2023-09-22

- Make commit limit configurable on `transaction()` context manager

## 0.11.0a2 2023-08-25

- Fix issue with query returning no results when using a filter with sparse matches

## 0.11.0a1 2023-07-20

- Add support for transaction writes using `transaction()` context manager
- No longer commit batch when an exception is raised during the batch context
  manager `__exit__`

## 0.10.0 2023-04-16

- Add support for `scan_index_forward` for specifying ascending (True) or
  descending (False) traversal of the index.

## 0.9.0 2023-04-15

- Add support for `__table_host__` for local testing

## 0.8.2 2022-11-12

- Make mypy linting more strict

## 0.8.1 2022-11-08

- Fixed `batch_read` to support non-string hash keys

## 0.8.0 2022-10-12

- Add `py.typed` marker to indicate this library ships with type hints

## 0.7.0 2022-10-11

- No changes since 0.7.0a1

## 0.7.0a1 2022-10-08

- Change dependency version pinning to be more flexible
- Only require `importlib_metadata` for python3.7 and earlier

## 0.6.0 2022-09-17

- Added support for `__table_name__` being a `Callable[[], str]` to allow
  dynamic table name
- Added support for batch reads and writes
- Fixed `consistent_read` behavior for `safe_get` (previously was always set to
  `True`)

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
