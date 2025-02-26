import os
import time
import warnings
from decimal import Decimal
from typing import Any, Callable, ClassVar, Dict, Generator, Generic, List, Optional, Tuple, Type, TypeVar, Union

import boto3

try:
    # Python 3.8+
    import importlib.metadata as _metadata
except ModuleNotFoundError:  # pragma: no cover
    # Python 3.7
    import importlib_metadata as _metadata  # type: ignore[no-redef, unused-ignore]

from contextvars import ContextVar

from boto3.dynamodb.conditions import ConditionBase
from pydantic import BaseModel, PrivateAttr

from . import attr, pydantic_compat, transact
from .attr import Attr, _UpdateAction, translate_updates
from .batch import BatchWriter, invoke_with_backoff
from .exceptions import DoesNotExist
from .transact import current_transaction_writer

__version__ = _metadata.version("dyntastic")

_T = TypeVar("_T", bound="Dyntastic")


class _TableMetadata:
    __table_name__: Union[str, Callable[[], str]]
    __table_region__: Optional[str] = None
    __table_host__: Optional[str] = None

    __hash_key__: str
    __range_key__: Optional[str] = None

    _dyntastic_batch_writer: ContextVar[Optional[BatchWriter]]


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
    def __init__(
        self,
        hash_key: str,
        range_key: Optional[str] = None,
        index_name: Optional[str] = None,
        keys_only: bool = False,
    ):
        self.hash_key = hash_key
        self.range_key = range_key
        # TODO: support INCLUDE projection?
        self.projection = "KEYS_ONLY" if keys_only else "ALL"

        if not index_name:
            if range_key:
                index_name = f"{hash_key}_{range_key}-index"
            else:
                index_name = f"{hash_key}-index"

        self.index_name = index_name


class Dyntastic(_TableMetadata, pydantic_compat.BaseModel):
    _dyntastic_unrefreshed: bool = PrivateAttr(default=False)
    _dyntastic_missing_attributes_from_index: bool = PrivateAttr(default=False)

    @classmethod
    def get_model(cls, item: dict):
        """Get a model instance from a DynamoDB item.

        This method can be overridden to support a single-table design pattern
        (i.e. multiple schemas shared in a single table).
        """

        return cls

    @classmethod
    def _dyntastic_load_model(cls, item: dict, load_full_item: bool = False):
        model = cls.get_model(item)

        data, had_validation_errors = pydantic_compat.try_model_construct(model, item)
        if had_validation_errors:
            # assume KEYS_ONLY or INCLUDE index
            data._dyntastic_missing_attributes_from_index = True

        if load_full_item:
            data.refresh()

        return data

    @classmethod
    def _serialize_key(
        cls,
        method: str,
        hash_key: Any,
        range_key: Any,
        hash_key_type: Optional[Type] = None,
        range_key_type: Optional[Type] = None,
    ) -> dict:
        key = {cls.__hash_key__: hash_key}
        if cls.__range_key__:
            key[cls.__range_key__] = range_key

        if hash_key_type is None:
            hash_key_type = pydantic_compat.field_type(cls, cls.__hash_key__)

        # hash key checks

        if not isinstance(hash_key, hash_key_type):
            raise ValueError(
                f"Expected hash key to be of type {hash_key_type.__name__}, "
                f"got {type(hash_key).__name__} in {cls.__name__}.{method}()"
            )

        if cls.__range_key__ is None:
            if range_key is not None:
                raise ValueError(
                    f"Range key `{range_key}` provided to {cls.__name__}.{method}(), "
                    "but table does not have a range key"
                )
            return attr.serialize(key)

        # range key checks

        if range_key_type is None:
            range_key_type = pydantic_compat.field_type(cls, cls.__range_key__)

        if range_key is None:
            raise ValueError(f"Range key required but not provided to {cls.__name__}.{method}()")

        # TODO: In order to run the following check, we would need to support
        #       *deserializing* the range key e.g. from a string to a datetime, just
        #       for this check, then re-serialize it before sending to DynamoDB

        # if not isinstance(range_key, range_key_type):
        #     raise ValueError(
        #         f"Expected range key to be of type {range_key_type.__name__}, "
        #         f"got {type(range_key).__name__} in {cls.__name__}.{method}()"
        #     )

        return attr.serialize(key)

    @classmethod
    def get(cls: Type[_T], hash_key, range_key=None, *, consistent_read: bool = False) -> _T:
        serialized_key = cls._serialize_key("get", hash_key, range_key)
        response = cls._dynamodb_table().get_item(Key=serialized_key, ConsistentRead=consistent_read)
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
        keys: Union[List[Any], List[Tuple[Any, Any]]],
        consistent_read: bool = False,
    ) -> List[_T]:
        hash_key_type = pydantic_compat.field_type(cls, cls.__hash_key__)
        range_key_type = None
        if cls.__range_key__:
            range_key_type = pydantic_compat.field_type(cls, cls.__range_key__)

        serialized_keys = []
        for key in keys:
            if cls.__range_key__ and (not isinstance(key, (list, tuple)) or len(key) != 2):
                raise ValueError(
                    f"Must provide (hash_key, range_key) tuples as `keys` to {cls.__name__}.batch_get(), got {key}"
                )
            hash_key, range_key = key if cls.__range_key__ else (key, None)
            serialized_key = cls._serialize_key("batch_get", hash_key, range_key, hash_key_type, range_key_type)
            serialized_keys.append(serialized_key)

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
        filter_condition: Optional[ConditionBase] = None,
        index: Optional[str] = None,
        per_page: Optional[int] = None,
        last_evaluated_key: Optional[dict] = None,
        scan_index_forward: bool = True,
        load_full_item: bool = False,
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
                scan_index_forward=scan_index_forward,
                load_full_item=load_full_item,
            )

            last_evaluated_key = result.last_evaluated_key
            yield from result.items

            if not result.has_more:
                break  # pragma: no cover (in python 3.8/3.9, this appeared as missing coverage)

    @classmethod
    def query_page(
        cls: Type[_T],
        hash_key: Union[str, ConditionBase],
        *,
        consistent_read: bool = False,
        range_key_condition: Optional[ConditionBase] = None,
        filter_condition: Optional[ConditionBase] = None,
        index: Optional[str] = None,
        per_page: Optional[int] = None,
        last_evaluated_key: Optional[dict] = None,
        scan_index_forward: bool = True,
        load_full_item: bool = False,
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
            ScanIndexForward=scan_index_forward,
        )

        raw_items = response.get("Items")
        items = [cls._dyntastic_load_model(item, load_full_item=load_full_item) for item in raw_items]
        last_evaluated_key = response.get("LastEvaluatedKey")

        return ResultPage(items, last_evaluated_key)

    @classmethod
    def scan(
        cls: Type[_T],
        filter_condition: Optional[ConditionBase] = None,
        *,
        consistent_read: bool = False,
        index: Optional[str] = None,
        per_page: Optional[int] = None,
        last_evaluated_key: Optional[dict] = None,
        load_full_item: bool = False,
    ):
        while True:
            result = cls.scan_page(
                filter_condition=filter_condition,
                consistent_read=consistent_read,
                index=index,
                per_page=per_page,
                last_evaluated_key=last_evaluated_key,
                load_full_item=load_full_item,
            )

            last_evaluated_key = result.last_evaluated_key
            yield from result.items

            if not result.has_more:
                break

    @classmethod
    def scan_page(
        cls: Type[_T],
        filter_condition: Optional[ConditionBase] = None,
        *,
        consistent_read: bool = False,
        index: Optional[str] = None,
        per_page: Optional[int] = None,
        last_evaluated_key: Optional[dict] = None,
        load_full_item: bool = False,
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
        items = [cls._dyntastic_load_model(item, load_full_item=load_full_item) for item in raw_items]
        last_evaluated_key = response.get("LastEvaluatedKey")

        return ResultPage(items, last_evaluated_key)

    def save(self, *, condition: Optional[ConditionBase] = None):
        data = pydantic_compat.model_dump(self, by_alias=True)
        dynamo_serialized = attr.serialize(data)
        return self._dyntastic_call("put_item", Item=dynamo_serialized, ConditionExpression=condition)

    def delete(self, *, condition: Optional[ConditionBase] = None):
        return self._dyntastic_call("delete_item", Key=self._dyntastic_key_dict, ConditionExpression=condition)

    # TODO: Support ReturnValues
    def update(
        self,
        *actions: _UpdateAction,
        condition: Optional[ConditionBase] = None,
        require_condition: bool = False,
        refresh: bool = True,
    ):
        if not actions:
            raise ValueError("Must provide at least one action to update")

        # TODO: Run all of the expression value through pydantic validators on
        # the class, to support all of the various input type casting (do this
        # before serialize)
        update_data: Dict[str, Any] = attr.serialize(translate_updates(*actions))
        try:
            response = self._dyntastic_call(
                "update_item",
                Key=self._dyntastic_key_dict,
                ConditionExpression=condition,
                **update_data,
            )
            self._dyntastic_unrefreshed = True
            if refresh:
                if current_transaction_writer() is not None:
                    warnings.warn("Cannot refresh model in transaction, skipping refresh", stacklevel=2)
                else:
                    # TODO: utilize ReturnValues in response when possible
                    self.refresh()

            return response
        except self.ConditionException():
            if require_condition:
                raise

    def refresh(self):
        self._dyntastic_unrefreshed = False
        self._dyntastic_missing_attributes_from_index = False
        data = self.get(self._dyntastic_hash_key, self._dyntastic_range_key)
        self.__dict__.update(data.__dict__)

    def transaction_condition(self, condition: ConditionBase):
        transaction_writer = current_transaction_writer()
        if transaction_writer is None:
            raise Exception(f"Cannot use {self.__class__.__name__}.transaction_condition() outside of a transaction")

        item = self._construct_transact_item(
            "transaction_condition",
            {"Key": self._dyntastic_key_dict, "ConditionExpression": condition},
        )
        transaction_writer.add(self.__class__, item)

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
                        "Projection": {"ProjectionType": index.projection},
                        "ProvisionedThroughput": {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
                    }
                )

            kwargs["GlobalSecondaryIndexes"] = secondary_indexes

        attribute_definitions = [
            {"AttributeName": attr, "AttributeType": cls._dynamodb_type(attr)} for attr in attributes
        ]

        cls._dynamodb_resource().create_table(
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
    def _resolve_table_region(cls) -> Optional[str]:
        if callable(cls.__table_region__):
            return cls.__table_region__()
        else:
            return cls.__table_region__ or os.getenv("DYNTASTIC_REGION")

    @classmethod
    def _resolve_table_host(cls) -> Optional[str]:
        if callable(cls.__table_host__):
            return cls.__table_host__()
        else:
            return cls.__table_host__ or os.getenv("DYNTASTIC_HOST")

    @classmethod
    def _dynamodb_type(cls, key: str) -> str:
        # Note: pragma nocover on the following line as coverage marks the ->exit branch as
        # being missed (since we can always find a field matching the key passed in)
        python_type = next(
            pydantic_compat.annotation(field)
            for field_name, field in pydantic_compat.model_fields(cls).items()
            if pydantic_compat.alias(field_name, field) == key
        )  # pragma: nocover
        if python_type == bytes:
            return "B"
        elif python_type in (int, Decimal, float):
            return "N"
        else:
            # TODO: how to properly differentiate between types like datetime
            # which serialize to str, and other types that do not?
            # TODO: use boto3.dynamodb.types.TypeSerializer._get_dynamodb_type() as a reference
            return "S"

    # To support using either a field's name or alias as __hash_key__ and
    # __range_key__, we need to search for the actual attribute name when
    # reading the value
    _cached_hash_key_attribute: ClassVar[Optional[str]] = None
    _cached_range_key_attribute: ClassVar[Optional[str]] = None

    @classmethod
    def _dyntastic_hash_key_attribute(cls) -> str:
        if cls._cached_hash_key_attribute is None:
            cls._cached_hash_key_attribute = pydantic_compat.attribute_from_field(cls, cls.__hash_key__)

        return cls._cached_hash_key_attribute

    @classmethod
    def _dyntastic_range_key_attribute(cls) -> str:
        assert cls.__range_key__

        if cls._cached_range_key_attribute is None:
            cls._cached_range_key_attribute = pydantic_compat.attribute_from_field(cls, cls.__range_key__)

        return cls._cached_range_key_attribute

    @property
    def _dyntastic_hash_key(self):
        return getattr(self, self._dyntastic_hash_key_attribute())

    @property
    def _dyntastic_range_key(self):
        if self.__range_key__:
            return getattr(self, self._dyntastic_range_key_attribute())
        else:
            return None

    @property
    def _dyntastic_key_dict(self):
        key = {self.__hash_key__: self._dyntastic_hash_key}
        if self.__range_key__:
            key[self.__range_key__] = self._dyntastic_range_key

        return attr.serialize(key)

    @classmethod
    def _dynamodb_boto3_kwargs(cls):
        kwargs = {}

        region = cls._resolve_table_region()
        if region:
            kwargs["region_name"] = region

        host = cls._resolve_table_host()
        if host:
            kwargs["endpoint_url"] = host

        return kwargs

    @classmethod
    def _dynamodb_resource(cls):
        if cls._dynamodb_resource_instance is None:  # type: ignore
            kwargs = cls._dynamodb_boto3_kwargs()
            cls._dynamodb_resource_instance = boto3.resource("dynamodb", **kwargs)  # type: ignore
        return cls._dynamodb_resource_instance  # type: ignore

    @classmethod
    def _dynamodb_table(cls):
        if cls._dynamodb_table_instance is None:  # type: ignore
            cls._dynamodb_table_instance = cls._dynamodb_resource().Table(cls._resolve_table_name())  # type: ignore
        return cls._dynamodb_table_instance  # type: ignore

    @classmethod
    def _dynamodb_client(cls):
        if cls._dynamodb_client_instance is None:  # type: ignore
            kwargs = cls._dynamodb_boto3_kwargs()
            cls._dynamodb_client_instance = boto3.client("dynamodb", **kwargs)  # type: ignore
        return cls._dynamodb_client_instance  # type: ignore

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
        cls._dynamodb_table_instance = None  # type: ignore
        cls._dynamodb_resource_instance = None  # type: ignore
        cls._dynamodb_client_instance = None  # type: ignore

    @classmethod
    def _construct_batch_item(cls, operation: str, filtered_kwargs: Dict[str, Any]):
        if operation == "delete_item":
            method = "delete"
            key = "DeleteRequest"
            required_kwargs = {"Key"}
        elif operation == "put_item":
            method = "save"
            key = "PutRequest"
            required_kwargs = {"Item"}
        else:  # pragma: nocover
            raise ValueError(f"Operation {operation} not supported with {cls.__name__}.batch_writer()")

        if filtered_kwargs.keys() != required_kwargs:
            raise ValueError(
                f"Cannot provide additional arguments to {cls.__name__}.{method}() when using batch_writer()"
            )

        return {key: filtered_kwargs}

    @classmethod
    def _construct_transact_item(cls, operation: str, filtered_kwargs: Dict[str, Any]):
        filtered_kwargs["TableName"] = cls._resolve_table_name()

        if "ConditionExpression" in filtered_kwargs:
            condition_data = transact.serialize_condition(filtered_kwargs["ConditionExpression"])
            filtered_kwargs["ConditionExpression"] = condition_data["ConditionExpression"]

            # Merging condition expression and update expression names/values so they are both present.
            # boto3 names/values look like '#n...' and ':v...', while dyntastic uses just '#...' and ':...'
            # so they should be mutually exclusive and not overlap at all
            names = filtered_kwargs.setdefault("ExpressionAttributeNames", {})
            names.update(condition_data["ExpressionAttributeNames"])

            if "ExpressionAttributeValues" in condition_data:
                values = filtered_kwargs.setdefault("ExpressionAttributeValues", {})
                values.update(condition_data["ExpressionAttributeValues"])

        for data_key in ("Key", "Item", "ExpressionAttributeValues"):
            if data_key in filtered_kwargs:
                filtered_kwargs[data_key] = transact.serialize_data(filtered_kwargs[data_key])

        key = {
            "delete_item": "Delete",
            "put_item": "Put",
            "update_item": "Update",
            "transaction_condition": "ConditionCheck",
        }.get(operation)

        if key is None:  # pragma: nocover
            raise ValueError(f"Operation {operation} not supported with dyntastic.TransactionWriter")

        return {key: filtered_kwargs}

    @classmethod
    def _dyntastic_call(cls, operation: str, **kwargs):
        method = getattr(cls._dynamodb_table(), operation)
        filtered_kwargs = {key: value for key, value in kwargs.items() if value is not None}

        batch_writer = cls._dyntastic_batch_writer.get()
        transaction_writer = current_transaction_writer()

        if batch_writer is not None and transaction_writer is not None:
            raise ValueError("Cannot use batch_writer() and transaction() at the same time")

        if (batch_writer is None and transaction_writer is None) or operation in ["query", "scan"]:
            return method(**filtered_kwargs)

        if batch_writer is not None:
            batch_item = cls._construct_batch_item(operation, filtered_kwargs)
            batch_writer.add(batch_item)
        elif transaction_writer is not None:
            item = cls._construct_transact_item(operation, filtered_kwargs)
            transaction_writer.add(cls, item)
        else:  # pragma: nocover
            raise Exception("Logically will always have a batch or transaction writer here")

    def ignore_unrefreshed(self):
        self._dyntastic_unrefreshed = False

    def _get_private_field(self, attr: str):
        try:
            return getattr(self, attr)
        except AttributeError:  # pragma: nocover
            # Note: Without this remapping AttributeError -> Exception, it is
            # particularly difficult to debug the issues that arise. For
            # example, without "model_post_init" in the __getattribute__
            # function below, the error appears as all model fields raising
            # AttributeError on access due to private fields like
            # _dyntastic_unrefreshed triggering that during pydantic's
            # __getattr__.
            #
            # Long story short, this should catch bugs in a much more easy-to-debug way.

            raise Exception(f"{attr} could not be accessed, dyntastic<->pydantic bug")

    def __getattribute__(self, attr: str):
        # breakpoint()
        # All of the code in this function works to "disable" an instance
        # that has been updated with refresh=False, to avoid accidentally
        # working with stale data

        if attr.startswith("_") or attr in {
            "refresh",
            "ignore_unrefreshed",
            "ConditionException",
            # Note: Without model_post_init here, _dyntastic_unrefreshed will
            # be accessed below before pydantic v2 is fully initialized,
            # which causes a bad state (for example, no field attribute can be accessed on the class)
            "model_post_init",
        }:
            return super().__getattribute__(attr)

        if self._get_private_field("_dyntastic_unrefreshed"):
            raise ValueError(
                "Dyntastic instance was not refreshed after update. "
                "Call refresh(), or use ignore_unrefreshed() to ignore safety checks"
            )

        try:
            return super().__getattribute__(attr)
        except AttributeError:
            if self._get_private_field("_dyntastic_missing_attributes_from_index"):
                raise ValueError(
                    "Dyntastic instance was loaded from a KEYS_ONLY or INCLUDE index. "
                    "Call refresh() to load the full item, or pass load_full_item=True to query() or scan()"
                )
            raise

    def __init_subclass__(cls, **kwargs):
        # Note: in pydantic v2, our private attributes like __hash_key__ are
        # not exposed on the model until the class is fully initialized, at
        # which point __pydantic_init_subclass__ is called.
        if pydantic_compat.IS_VERSION_1:  # pragma: nocover
            cls.__pydantic_init_subclass__(**kwargs)

    @classmethod
    def __pydantic_init_subclass__(cls, **kwargs):
        if not pydantic_compat.IS_VERSION_1:  # pragma: nocover
            super().__pydantic_init_subclass__(**kwargs)  # type: ignore[unused-ignore, misc]

        cls._clear_boto3_state()

        cls._dyntastic_batch_writer = ContextVar("dyntastic_batch_writer", default=None)

        if not hasattr(cls, "__table_name__"):
            raise ValueError("Dyntastic table must have __table_name__ defined")

        if not hasattr(cls, "__hash_key__"):
            raise ValueError("Dyntastic table must have __hash_key__ defined")

        if not _has_alias(cls, cls.__hash_key__):
            raise ValueError(f"Dyntastic __hash_key__ is not defined as a field: '{cls.__hash_key__}'")

        if cls.__range_key__ and not _has_alias(cls, cls.__range_key__):
            raise ValueError(f"Dyntastic __range_key__ is not defined as a field: '{cls.__range_key__}'")

        all_aliases = set()
        for field_name, field in pydantic_compat.model_fields(cls).items():
            field_identifier = pydantic_compat.alias(field_name, field)
            if field_identifier in all_aliases:
                raise ValueError(f"Duplicate alias '{field_identifier}' found in {cls.__name__}")
            all_aliases.add(field_identifier)


def _has_alias(model: Type[BaseModel], name: str) -> bool:
    for field_name, field in pydantic_compat.model_fields(model).items():
        if pydantic_compat.alias(field_name, field) == name:
            return True

    return False
