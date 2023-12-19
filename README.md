# dyntastic

[![CI](https://github.com/nayaverdier/dyntastic/actions/workflows/ci.yml/badge.svg)](https://github.com/nayaverdier/dyntastic/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/nayaverdier/dyntastic/branch/main/graph/badge.svg)](https://codecov.io/gh/nayaverdier/dyntastic)
[![pypi](https://img.shields.io/pypi/v/dyntastic)](https://pypi.org/project/dyntastic)
[![license](https://img.shields.io/github/license/nayaverdier/dyntastic.svg)](https://github.com/nayaverdier/dyntastic/blob/main/LICENSE)

A DynamoDB library on top of Pydantic and boto3.

## Installation

```bash
pip3 install dyntastic
```

If the Pydantic binaries are too large for you (they can exceed 90MB),
use the following:

```bash
pip3 uninstall pydantic  # if pydantic is already installed
pip3 install dyntastic --no-binary pydantic
```

## Usage

The core functionality of this library is provided by the `Dyntastic` class.

`Dyntastic` is a subclass of Pydantic's `BaseModel`, so can be used in all the
same places a Pydantic model can be used (FastAPI, etc).

```python
import uuid
from datetime import datetime
from typing import Optional

from dyntastic import Dyntastic
from pydantic import Field

class Product(Dyntastic):
    __table_name__ = "products"
    __hash_key__ = "product_id"

    product_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    description: Optional[str] = None
    price: float
    tax: Optional[float] = None


class Event(Dyntastic):
    __table_name__ = "events"
    __hash_key__ = "event_id"
    __range_key__ = "timestamp"

    event_id: str
    timestamp: datetime
    data: dict

# All your favorite pydantic functionality still works:

p = Product(name="bread", price=3.49)
# Product(product_id='d2e91c30-e701-422f-b71b-465b02749f18', name='bread', description=None, price=3.49, tax=None)

p.model_dump()
# {'product_id': 'd2e91c30-e701-422f-b71b-465b02749f18', 'name': 'bread', 'description': None, 'price': 3.49, 'tax': None}

p.model_dump_json()
# '{"product_id": "d2e91c30-e701-422f-b71b-465b02749f18", "name": "bread", "description": null, "price": 3.49, "tax": null}'

```

### Inserting into DynamoDB

Using the `Product` example from above, simply:

```python
product = Product(name="bread", description="Sourdough Bread", price=3.99)
product.product_id
# d2e91c30-e701-422f-b71b-465b02749f18

# Nothing is written to DynamoDB until .save() is called:
product.save()
```

### Getting Items from DynamoDB

```python
Product.get("d2e91c30-e701-422f-b71b-465b02749f18")
# Product(product_id='d2e91c30-e701-422f-b71b-465b02749f18', name='bread', description="Sourdough Bread", price=3.99, tax=None)
```

The range key must be provided if one is defined:

```python
Event.get("d2e91c30-e701-422f-b71b-465b02749f18", "2022-02-12T18:27:55.837Z")
```

Consistent reads are supported:

```python
Event.get(..., consistent_read=True)
```

A `DoesNotExist` error is raised by `get` if a key is not found:

```python
Product.get("nonexistent")
# Traceback (most recent call last):
#   ...
# dyntastic.exceptions.DoesNotExist
```

Use `safe_get` instead to return `None` if the key is not found:

```python
Product.safe_get("nonexistent")
# None
```

### Querying Items in DynamoDB

```python
# A is shorthand for the Attr class (i.e. attribute)
from dyntastic import A

# auto paging iterable
for event in Event.query("some_event_id"):
    print(event)


Event.query("some_event_id", per_page=10)
Event.query("some_event_id")
Event.query("some_event_id", range_key_condition=A.timestamp < datetime(2022, 2, 13))
Event.query("some_event_id", filter_condition=A.some_field == "foo")

# query an index
Event.query(A.my_other_field == 12345, index="my_other_field-index")

# note: Must provide a condition expression rather than just the value
Event.query(123545, index="my_other_field-index")  # errors!

# query an index with an optional filter expression
filter_expression = None
if filter_value:
    filter_expression = A('filter_field').eq(filter_value)
Event.query(
    A.my_other_field == 12345,
    index="my_other_field-index",
    filter_expression=filter_expression
)

# consistent read
Event.query("some_event_id", consistent_read=True)

# specifies the order for index traversal, the default is ascending order
# returns the results in the order in which they are stored by sort key value
Event.query("some_event_id", range_key_condition=A.version.begins_with("2023"), scan_index_forward=False)
```

DynamoDB Indexes using a `KEYS_ONLY` or `INCLUDE` projection are supported:

```python
for event in Event.query("2023-09-22", index="date-keys-only-index"):
    event.id
    # "..."
    event.timestamp
    # datetime(...)

    event.data
    # ValueError: Dyntastic instance was loaded from a KEYS_ONLY or INCLUDE index.
    #             Call refresh() to load the full item, or pass load_full_item=True
    #             to query() or scan()

# automatically fetch the full items
for event in Event.query("2023-09-22", index="date-keys-only-index", load_full_item=True):
    event.data
    # {...}
```

If you need to manually handle pagination, use `query_page`:

```python
page = Event.query_page(...)
page.items
# [...]
page.has_more
# True
page.last_evaluated_key
# {"event_id": "some_event_id", "timestamp": "..."}

Event.query_page(..., last_evaluated_key=page.last_evaluated_key)
```

### Scanning Items in DynamoDB

Scanning is done identically to querying, except there are no hash key
or range key conditions.

```python
# auto paging iterable
for event in Event.scan():
    pass

Event.scan((A.my_field < 5) & (A.some_other_field.is_in(["a", "b", "c"])))
Event.scan(..., consistent_read=True)
```

### Updating Items in DynamoDB

Examples:

```python
my_item.update(A.my_field.set("new_value"))
my_item.update(A.my_field.set(A.another_field))
my_item.update(A.my_int.set(A.another_int - 10))
my_item.update(A.my_int.plus(1))
my_item.update(A.my_list.append("new_element"))
my_item.update(A.some_attribute.set_default("value_if_not_already_present"))

my_item.update(A.my_field.remove())
my_item.update(A.my_list.remove(2))  # remove by index

my_item.update(A.my_string_set.add("new_element"))
my_item.update(A.my_string_set.add({"new_1", "new_2"}))
my_item.update(A.my_string_set.delete("element_to_remove"))
my_item.update(A.my_string_set.delete({"remove_1", "remove_2"}))
```

The data is automatically refreshed after the update request. To disable this
behavior, pass `refresh=False`:

```python
my_item.update(..., refresh=False)
```

Supports conditions:

```python
my_item.update(..., condition=A.my_field == "something")
```

By default, if the condition is not met, the update call will be a noop.
To instead error in this situation, pass `require_condition=True`:

```python
my_item.update(..., require_condition=True)
```

### Batch Reads

Multiple items can be read from a table at the same time using the `batch_get` function.

Note that DynamoDB limits the number of items that can be read at one time to
100 items or 16MB, whichever comes first.

Note that if any of the provided keys are missing from dynamo, they will simply
be excluded in the result set.

```python
MyModel.batch_get(["hash_key_1", "hash_key_2", "hash_key_3"])
# => [MyModel(...), MyModel(...)]
```

For models with a range key defined:

```python
MyModel.batch_get([("hash_key_1", "range_key_1"), ("hash_key_2", "range_key_2")])
# => [MyModel(...), MyModel(...)]
```

### Batch Writes

Save and delete operations may also be performed in batches.

Note that DynamoDB limits the number of items that can be written in a single
batch to 25 items or 16MB, whichever comes first. Dyntastic will automatically
batch in chunks of 25, or less if desired.

```python
with MyModel.batch_writer():
    MyModel(id="0").delete()
    MyModel(id="1").save()
    MyModel(id="2").save()

# all operations are performed once the `with` context is exited
```

To configure a smaller batch size, for example when each item is relatively large:

```python
with MyModel.batch_writer(batch_size=2):
    MyModel(id="1").save()
    MyModel(id="2").save()
    # the previous two models are written immediately, since the batch size was reached
    MyModel(id="3).save()

# The final operation is performed here now that the `with` context has exited
```


### Transactions

Dyntastic supports DynamoDB transactions. Transactions are performed using the
`transaction` context manager and can be used to perform operations across one or multiple
tables that reside in the same region.

```python
from dyntastic import transaction

with transaction():
    item1 = SomeTable(...)
    item2 = AnotherTable.get(...)
    item1.save()
    item2.update(A.something.set("..."))
```

Note that DynamoDB limits the number of items that can be written in a single
transaction to 100 items or 4MB, whichever comes first. Dyntastic can automatically
flush the transaction in chunks of 100 (or fewer if desired) by passing `auto_commit=True`.

For example, to commit every 50 items:
```python
with transaction(auto_commit=True, commit_every=50):
    item1 = SomeTable(...)
    item2 = AnotherTable.get(...)
    item1.save()
    item2.update(A.something.set("..."))
```

### Create a DynamoDB Table

This functionality is currently meant only for use in unit tests as it does not
support configuring throughput.

To create a table with no secondary indexes:

```python
MyModel.create_table()

# Do not wait until the table creation is complete (subsequent operations
# may error if they are performed before the table creation is finished)
MyModel.create_table(wait=False)
```

To define global secondary indexes (creating local secondary indexes is not
currently supported):

```python
# All of the following are equivalent
index1 = "my_field"
index1 = Index("my_field")
index1 = Index("my_field", index_name="my_field-index")

# Range keys are also supported
index2 = Index("my_field", "my_second_field")
index2 = Index("my_field", "my_second_field", index_name="my_field_my_second_field-index")

MyModel.create_table(index1, index2)
```

## Dynamic table names
In some circumstances you may want the table name to be defined dynamically.
This can be done by setting the `__table_name__` attribute to a Callable that returns the table name
from the source of your choice. In the example below, we are using an environment variable. 

```python
import os
from dyntastic import Dyntastic

os.environ["MY_TABLE_NAME"] = "my_table"

class Product(Dyntastic):
    __table_name__ = lambda: os.getenv("MY_TABLE_NAME")
    __hash_key__ = "product_id"

    product_id: str
```

## Custom dynamodb endpoint or region for local development
To explicitly define an AWS region or DynamoDB endpoint url (for using a local
dynamodb docker instance, for example), set `__table_region__` or `__table_host__`.
These attributes can be a string or a Callable that returns a string.

```python
from dyntastic import Dyntastic

class Product(Dyntastic):
    __table_name__ = "products"
    __table_region__ = "us-east-1"
    __table_host__ = "http://localhost:8000"
    __hash_key__ = "product_id"

    product_id: str
```

You can also set the environment variables `DYNTASTIC_HOST` and/or `DYNTASTIC_REGION` to control the behavior
of the underlying boto3 client and resource objects. 

*Note*: if both the environment variables and the class attributes are set,
the class attributes will take precedence. 

```python
import os
from dyntastic import Dyntastic

os.environ["DYNTASTIC_HOST"] = "http://localhost:8000"
os.environ["DYNTASTIC_REGION"] = "us-east-1"

class Product(Dyntastic):
    __table_name__ = "products"
    __hash_key__ = "product_id"

    product_id: str
```

## Contributing / Developer Setup

Make sure [`just`](https://github.com/casey/just) is installed on your system

To setup the dev environment and install dependencies:

```bash
# create and activate a new venv
python3 -m venv .venv
. .venv/bin/activate

# install all dev dependencies
just install-dev

# to automatically run pre-commit before all commits
pre-commit install
```

After making changes, lint all code + run tests:

```bash
just pre-commit

# or individually:
just isort
just black
just flake8
just mypy
just test

# run a specific test/tests
just test tests/test_save.py tests/test_get.py
just test tests/some_save.py::test_save_aliased_item
```
