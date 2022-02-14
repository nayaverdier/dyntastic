import time
from decimal import Decimal
from typing import Any, Dict, Generator, Generic, List, Optional, Type, TypeVar, Union

import boto3
import importlib_metadata as _metadata
from boto3.dynamodb.conditions import ConditionBase
from pydantic import BaseModel

from .attr import Attr, _UpdateAction, serialize, translate_updates
from .exceptions import DoesNotExist

__version__ = _metadata.version("dyntastic")


_T = TypeVar("_T", bound="Dyntastic")


class _TableMetadata:
    # TODO: add __table_host__?
    __table_name__: str
    __table_region__: Optional[str] = None

    __hash_key__: str
    __range_key__: Optional[str] = None


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
            return cls(**data)
        else:
            raise DoesNotExist

    @classmethod
    def safe_get(cls: Type[_T], hash_key, range_key=None, *, consistent_read: bool = False) -> Optional[_T]:
        try:
            return cls.get(hash_key, range_key=range_key, consistent_read=True)
        except DoesNotExist:
            return None

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
        items = [cls(**item) for item in raw_items]
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
        items = [cls(**item) for item in raw_items]
        last_evaluated_key = response.get("LastEvaluatedKey")

        return ResultPage(items, last_evaluated_key)

    def save(self, *, condition: ConditionBase = None) -> dict:
        data = self.dict()
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
            if refresh:
                # TODO: utilize ReturnValues in response when possible
                self.refresh()

            return response
        except self.ConditionException():
            if require_condition:
                raise

    def refresh(self):
        data = self.get(self._dyntastic_hash_key, self._dyntastic_range_key)
        full_dict = data.dict(exclude_none=False, exclude_defaults=False, exclude_unset=False, by_alias=False)
        self.__dict__.update(full_dict)

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
        # TODO: support RANGE keys in secondary indexes
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
            TableName=cls.__table_name__,
            KeySchema=key_schema,
            AttributeDefinitions=attribute_definitions,
            ProvisionedThroughput=throughput,
            **kwargs,
        )

        if wait:
            cls._wait_until_exists()

    # Internal helpers

    @classmethod
    def _dynamodb_type(cls, key: str) -> str:
        python_type = cls.__fields__[key].type_
        if python_type == bytes:
            return "B"
        elif python_type in (int, Decimal):
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
            cls._dynamodb_table_instance = cls._dynamodb_resource().Table(cls.__table_name__)
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
            response = cls._dynamodb_client().describe_table(TableName=cls.__table_name__)
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
        return method(**filtered_kwargs)

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        cls._clear_boto3_state()

        if not hasattr(cls, "__table_name__"):
            raise ValueError("Dyntastic table must have __table_name__ defined")

        if not hasattr(cls, "__hash_key__"):
            raise ValueError("Dyntastic table must have __hash_key__ defined")

        if cls.__hash_key__ not in cls.__fields__:
            raise ValueError(f"Dyntastic __hash_key__ is not defined as a field: '{cls.__hash_key__}'")

        if cls.__range_key__ and cls.__range_key__ not in cls.__fields__:
            raise ValueError(f"Dyntastic __range_key__ is not defined as a field: '{cls.__range_key__}'")
