import time
from decimal import Decimal
from typing import Any, Callable, Dict, Generator, Generic, List, Optional, Tuple, Type, TypeVar, Union

import boto3

try:
    # Python 3.8+
    import importlib.metadata as _metadata  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    # Python 3.7
    import importlib_metadata as _metadata  # type: ignore

from boto3.dynamodb.conditions import ConditionBase
from pydantic import BaseModel, PrivateAttr

from .attr import Attr, _UpdateAction, serialize, translate_updates
from .batch import BatchWriter, invoke_with_backoff
from .exceptions import DoesNotExist

__version__ = _metadata.version("dyntastic")


_T = TypeVar("_T", bound="Dyntastic")


class _TableMetadata:
    # TODO: add __table_host__?
    __table_name__: Union[str, Callable[[], str]]
    __table_region__: Optional[str] = None

    __hash_key__: str
    __range_key__: Optional[str] = None

    _dyntastic_batch_writer: Optional[BatchWriter] = None


class ResultPage(Generic[_T]):
    def __init__(self, items: List[_T], last_evaluated_key: Optional[dict]):
        self.items = items
        self.last_evaluated_key = last_evaluated_key
        self.has_more = last_evaluated_key is not None

    def __str__(self):
        return f"ResultPage: {self.__dict__}"

    def __repr__(self):
        return str(self)


class Index:
    def __init__(self, hash_key: str, range_key: str = None, index_name: str = None):
        self.hash_key = hash_key
        self.range_key = range_key

        if not index_name:
            if range_key:
                index_name = f"{hash_key}_{range_key}-index"
            else:
                index_name = f"{hash_key}-index"

        self.index_name = index_name


class Dyntastic(_TableMetadata, BaseModel):
    _dyntastic_unrefreshed: bool = PrivateAttr(default=False)

    @classmethod
    def get_model(cls, item: dict):
        """Get a model instance from a DynamoDB item.

        This method can be overridden to support a single-table design pattern
        (i.e. multiple schemas shared in a single table).
        """

        return cls

    @classmethod
    def _dyntastic_load_model(cls, item: dict):
        return cls.get_model(item)(**item)

    @classmethod
    def get(cls: Type[_T], hash_key, range_key=None, *, consistent_read: bool = False) -> _T:
        if cls.__range_key__ and range_key is None:
            raise ValueError(f"Must provide range_key to {cls.__name__}.get()")
        elif range_key and cls.__range_key__ is None:
            raise ValueError(f"Did not expect range_key for {cls.__name__}.get(), found '{range_key}'")

        key = {cls.__hash_key__: hash_key}
        if cls.__range_key__:
            key[cls.__range_key__] = range_key

        serialized_key = serialize(key)

        response = cls._dynamodb_table().get_item(Key=serialized_key, ConsistentRead=consistent_read)  # type: ignore
        data = response.get("Item")
        if data:
            return cls._dyntastic_load_model(data)
        else:
            raise DoesNotExist

    @classmethod
    def safe_get(cls: Type[_T], hash_key, range_key=None, *, consistent_read: bool = False) -> Optional[_T]:
        try:
            return cls.get(hash_key, range_key=range_key, consistent_read=consistent_read)
        except DoesNotExist:
            return None

    @classmethod
    def batch_get(
        cls: Type[_T],
        keys: Union[List[str], List[Tuple[str, str]]],
        consistent_read: bool = False,
    ) -> List[_T]:
        hash_key_type = cls.__fields__[cls.__hash_key__].type_

        if cls.__range_key__ and not all(isinstance(key, (list, tuple)) and len(key) == 2 for key in keys):
            raise ValueError(f"Must provide (hash_key, range_key) tuples as `keys` to {cls.__name__}.batch_get()")

        if cls.__range_key__ is None and not all(isinstance(key, hash_key_type) for key in keys):
            raise ValueError(
                f"Must only provide {hash_key_type.__name__} types as `keys` to {cls.__name__}.batch_get()"
            )

        serialized_keys = []
        for key in keys:
            if cls.__range_key__:
                key_dict = {cls.__hash_key__: key[0], cls.__range_key__: key[1]}
            else:
                assert isinstance(key, hash_key_type)
                key_dict = {cls.__hash_key__: key}

            serialized_keys.append(serialize(key_dict))

        responses = invoke_with_backoff(
            cls._dynamodb_resource().batch_get_item,
            {cls._resolve_table_name(): {"Keys": serialized_keys, "ConsistentRead": consistent_read}},
            "UnprocessedKeys",
        )

        items: List[_T] = []
        for response in responses:
            raw_items = response["Responses"][cls._resolve_table_name()]
            items.extend(cls._dyntastic_load_model(item) for item in raw_items)

        return items

    @classmethod
    def query(
        cls: Type[_T],
        hash_key,
        *,
        consistent_read: bool = False,
        range_key_condition=None,
        filter_condition: ConditionBase = None,
        index: str = None,
        per_page: int = None,
        last_evaluated_key: dict = None,
    ) -> Generator[_T, None, None]:
        while True:
            result = cls.query_page(
                hash_key,
                consistent_read=consistent_read,
                range_key_condition=range_key_condition,
                filter_condition=filter_condition,
                index=index,
                per_page=per_page,
                last_evaluated_key=last_evaluated_key,
            )

            last_evaluated_key = result.last_evaluated_key
            yield from result.items

            if not result.has_more or not result.items:
                break

    @classmethod
    def query_page(
        cls: Type[_T],
        hash_key: Union[str, ConditionBase],
        *,
        consistent_read: bool = False,
        range_key_condition: ConditionBase = None,
        filter_condition: ConditionBase = None,
        index: str = None,
        per_page: int = None,
        last_evaluated_key: dict = None,
    ) -> ResultPage[_T]:
        if index and consistent_read:
            raise ValueError("Cannot perform a consistent read against a secondary index")

        if isinstance(hash_key, ConditionBase):
            key_condition = hash_key
        elif index is not None:
            raise ValueError("Must specify attribute condition for index, e.g. A.my_index_hash_key == 'example_value'")
        else:
            key_condition: ConditionBase = Attr(cls.__hash_key__) == hash_key  # type: ignore

        if range_key_condition:
            key_condition &= range_key_condition

        response = cls._dyntastic_call(
            "query",
            ConsistentRead=consistent_read,
            IndexName=index,
            Limit=per_page,
            ExclusiveStartKey=last_evaluated_key,
            KeyConditionExpression=key_condition,
            FilterExpression=filter_condition,
        )

        raw_items = response.get("Items")
        items = [cls._dyntastic_load_model(item) for item in raw_items]
        last_evaluated_key = response.get("LastEvaluatedKey")

        return ResultPage(items, last_evaluated_key)

    @classmethod
    def scan(
        cls: Type[_T],
        filter_condition: ConditionBase = None,
        *,
        consistent_read: bool = False,
        index: str = None,
        per_page: int = None,
        last_evaluated_key: dict = None,
    ):
        while True:
            result = cls.scan_page(
                filter_condition=filter_condition,
                consistent_read=consistent_read,
                index=index,
                per_page=per_page,
                last_evaluated_key=last_evaluated_key,
            )

            last_evaluated_key = result.last_evaluated_key
            yield from result.items

            if not result.has_more:
                break

    @classmethod
    def scan_page(
        cls: Type[_T],
        filter_condition: ConditionBase = None,
        *,
        consistent_read: bool = False,
        index: str = None,
        per_page: int = None,
        last_evaluated_key: dict = None,
    ) -> ResultPage[_T]:
        response = cls._dyntastic_call(
            "scan",
            ConsistentRead=consistent_read,
            IndexName=index,
            Limit=per_page,
            ExclusiveStartKey=last_evaluated_key,
            FilterExpression=filter_condition,
        )

        raw_items = response.get("Items")
        items = [cls._dyntastic_load_model(item) for item in raw_items]
        last_evaluated_key = response.get("LastEvaluatedKey")

        return ResultPage(items, last_evaluated_key)

    def save(self, *, condition: ConditionBase = None):
        data = self.dict(by_alias=True)
        dynamo_serialized = serialize(data)
        return self._dyntastic_call("put_item", Item=dynamo_serialized, ConditionExpression=condition)

    def delete(self, *, condition: ConditionBase = None):
        return self._dyntastic_call("delete_item", Key=self._dyntastic_key_dict, ConditionExpression=condition)

    # TODO: Support ReturnValues
    def update(
        self,
        *actions: _UpdateAction,
        condition: ConditionBase = None,
        require_condition: bool = False,
        refresh: bool = True,
    ):
        if not actions:
            raise ValueError("Must provide at least one action to update")

        # TODO: Run all of the expression value through pydantic validators on
        # the class, to support all of the various input type casting (do this
        # before serialize)
        update_data: Dict[str, Any] = serialize(translate_updates(*actions))  # type: ignore
        try:
            response = self._dyntastic_call(
                "update_item",
                Key=self._dyntastic_key_dict,
                ConditionExpression=condition,
                **update_data,
            )
            self._dyntastic_unrefreshed = True
            if refresh:
                # TODO: utilize ReturnValues in response when possible
                self.refresh()

            return response
        except self.ConditionException():
            if require_condition:
                raise

    def refresh(self):
        self._dyntastic_unrefreshed = False
        data = self.get(self._dyntastic_hash_key, self._dyntastic_range_key)
        # Note: we have to use pydantic's private _iter function here instead of data.dict()
        # because we don't want nested objects to be converted to dictionaries as well.
        self.__dict__.update(dict(data._iter(to_dict=False, by_alias=False, exclude_unset=False)))

    @classmethod
    def batch_writer(cls, batch_size: int = 25):
        return BatchWriter(cls, batch_size=batch_size)

    @classmethod
    def submit_batch_write(cls, batch: List[dict]):
        if not batch:
            return

        responses = invoke_with_backoff(
            cls._dynamodb_resource().batch_write_item,
            {cls._resolve_table_name(): batch},
            "UnprocessedItems",
        )

        return responses

    # Note: This cannot use @classmethod and @property together for python <3.9
    @classmethod
    def ConditionException(cls):
        return cls._dynamodb_table().meta.client.exceptions.ConditionalCheckFailedException

    # TODO: support more configuration for new table
    @classmethod
    def create_table(cls, *indexes: Union[str, Index], wait: bool = True):
        """Creates a DynamoDB table (primarily for testing, limited configuration supported)"""

        throughput = {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1}
        attributes = {cls.__hash_key__}
        key_schema = [{"AttributeName": cls.__hash_key__, "KeyType": "HASH"}]
        if cls.__range_key__:
            attributes.add(cls.__range_key__)
            key_schema.append({"AttributeName": cls.__range_key__, "KeyType": "RANGE"})

        kwargs = {}
        if indexes:
            secondary_indexes = []
            for index in indexes:
                if isinstance(index, str):
                    index = Index(index)

                attributes.add(index.hash_key)
                index_schema = [{"AttributeName": index.hash_key, "KeyType": "HASH"}]
                if index.range_key:
                    attributes.add(index.range_key)
                    index_schema.append({"AttributeName": index.range_key, "KeyType": "RANGE"})

                secondary_indexes.append(
                    {
                        "IndexName": index.index_name,
                        "KeySchema": index_schema,
                        "Projection": {"ProjectionType": "ALL"},
                        "ProvisionedThroughput": {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
                    }
                )

            kwargs["GlobalSecondaryIndexes"] = secondary_indexes

        attribute_definitions = [
            {"AttributeName": attr, "AttributeType": cls._dynamodb_type(attr)} for attr in attributes
        ]

        cls._dynamodb_resource().create_table(  # type: ignore
            TableName=cls._resolve_table_name(),
            KeySchema=key_schema,
            AttributeDefinitions=attribute_definitions,
            ProvisionedThroughput=throughput,
            **kwargs,
        )

        if wait:
            cls._wait_until_exists()

    # Internal helpers

    @classmethod
    def _resolve_table_name(cls) -> str:
        if callable(cls.__table_name__):
            return cls.__table_name__()
        else:
            return cls.__table_name__

    @classmethod
    def _dynamodb_type(cls, key: str) -> str:
        # Note: pragma nocover on the following line as coverage marks the ->exit branch as
        # being missed (since we can always find a field matching the key passed in)
        python_type = next(field.type_ for field in cls.__fields__.values() if field.alias == key)  # pragma: nocover
        if python_type == bytes:
            return "B"
        elif python_type in (int, Decimal, float):
            return "N"
        else:
            # TODO: how to properly differentiate between types like datetime
            # which serialize to str, and other types that do not?
            return "S"

    @property
    def _dyntastic_hash_key(self):
        return getattr(self, self.__hash_key__)

    @property
    def _dyntastic_range_key(self):
        if self.__range_key__:
            return getattr(self, self.__range_key__)
        else:
            return None

    @property
    def _dyntastic_key_dict(self):
        key = {self.__hash_key__: self._dyntastic_hash_key}
        if self.__range_key__:
            key[self.__range_key__] = self._dyntastic_range_key

        return serialize(key)

    @classmethod
    def _dynamodb_resource(cls):
        if cls._dynamodb_resource_instance is None:
            kwargs = {}
            if cls.__table_region__:
                kwargs["region_name"] = cls.__table_region__
            cls._dynamodb_resource_instance = boto3.resource("dynamodb", **kwargs)
        return cls._dynamodb_resource_instance

    @classmethod
    def _dynamodb_table(cls):
        if cls._dynamodb_table_instance is None:
            cls._dynamodb_table_instance = cls._dynamodb_resource().Table(cls._resolve_table_name())
        return cls._dynamodb_table_instance

    @classmethod
    def _dynamodb_client(cls):
        if cls._dynamodb_client_instance is None:
            kwargs = {}
            if cls.__table_region__:
                kwargs["region_name"] = cls.__table_region__
            cls._dynamodb_client_instance = boto3.client("dynamodb", **kwargs)
        return cls._dynamodb_client_instance

    @classmethod
    def _wait_until_exists(cls):
        # wait a maximum of 15 * 2 = 30 seconds
        for _ in range(15):  # pragma: no cover
            response = cls._dynamodb_client().describe_table(TableName=cls._resolve_table_name())
            if response["Table"].get("TableStatus") == "ACTIVE":  # pragma: no cover
                break

            time.sleep(2)

    @classmethod
    def _clear_boto3_state(cls):
        cls._dynamodb_table_instance = None
        cls._dynamodb_resource_instance = None
        cls._dynamodb_client_instance = None

    @classmethod
    def _dyntastic_call(cls, operation, **kwargs):
        method = getattr(cls._dynamodb_table(), operation)
        filtered_kwargs = {key: value for key, value in kwargs.items() if value is not None}

        # Logic to support writing in batches without changing the API at all
        if cls._dyntastic_batch_writer is not None and operation not in ["query", "scan"]:
            if operation == "delete_item":
                if filtered_kwargs.keys() != {"Key"}:
                    raise ValueError(
                        f"Cannot provide additional arguments to {cls.__name__}.delete() when using batch_writer()"
                    )

                batch_item = {"DeleteRequest": filtered_kwargs}
            elif operation == "put_item":
                if filtered_kwargs.keys() != {"Item"}:
                    raise ValueError(
                        f"Cannot provide additional arguments to {cls.__name__}.save() when using batch_writer()"
                    )
                batch_item = {"PutRequest": filtered_kwargs}
            else:  # pragma: nocover
                raise ValueError(f"Operation {operation} not supported with {cls.__name__}.batch_writer()")

            cls._dyntastic_batch_writer.add(batch_item)
        else:
            return method(**filtered_kwargs)

    def ignore_unrefreshed(self):
        self._dyntastic_unrefreshed = False

    def __getattribute__(self, attr: str):
        # All of the code in this function works to "disable" an instance
        # that has been updated with refresh=False, to avoid accidentally
        # working with stale data

        if attr.startswith("_") or attr in {"refresh", "ignore_unrefreshed", "ConditionException"}:
            return super().__getattribute__(attr)

        if object.__getattribute__(self, "_dyntastic_unrefreshed"):
            raise ValueError(
                "Dyntastic instance was not refreshed after update. "
                "Call refresh() or ignore_unrefreshed() to ignore safety checks"
            )

        return super().__getattribute__(attr)

    class Config:
        allow_population_by_field_name = True

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        cls._clear_boto3_state()

        if not hasattr(cls, "__table_name__"):
            raise ValueError("Dyntastic table must have __table_name__ defined")

        if not hasattr(cls, "__hash_key__"):
            raise ValueError("Dyntastic table must have __hash_key__ defined")

        if not _has_alias(cls, cls.__hash_key__):
            raise ValueError(f"Dyntastic __hash_key__ is not defined as a field: '{cls.__hash_key__}'")

        if cls.__range_key__ and not _has_alias(cls, cls.__range_key__):
            raise ValueError(f"Dyntastic __range_key__ is not defined as a field: '{cls.__range_key__}'")


def _has_alias(model: Type[BaseModel], name: str) -> bool:
    for field in model.__fields__.values():
        if field.alias == name:
            return True

    return False
