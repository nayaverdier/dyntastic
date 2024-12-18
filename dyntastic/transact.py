from contextvars import ContextVar, Token
from typing import Optional, Type

from boto3.dynamodb.conditions import ConditionExpressionBuilder
from boto3.dynamodb.types import TypeSerializer

from . import main

# DynamoDB transaction commit limit
# https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/transaction-apis.html
TRANSACTION_MAX_ITEMS = 100

_transaction_writer_var: ContextVar[Optional["TransactionWriter"]] = ContextVar(
    "transaction_writer",
    default=None,
)


def current_transaction_writer() -> Optional["TransactionWriter"]:
    return _transaction_writer_var.get()


_dynamodb_serializer = TypeSerializer()
_dynamodb_builder = ConditionExpressionBuilder()


def serialize_data(item: dict) -> dict:
    return {k: _dynamodb_serializer.serialize(v) for k, v in item.items()}


def serialize_condition(condition) -> dict:
    expression = _dynamodb_builder.build_expression(condition)

    serialized = {
        "ConditionExpression": expression.condition_expression,
        "ExpressionAttributeNames": expression.attribute_name_placeholders,
    }

    # DynamoDB errors if ExpressionAttributeValues is present but empty.
    # At the time of writing this comment, Moto doesn't enforce this, but real DynamoDB does:
    # https://github.com/getmoto/moto/issues/8405
    # https://github.com/nayaverdier/dyntastic/issues/27
    if expression.attribute_value_placeholders:
        serialized["ExpressionAttributeValues"] = expression.attribute_value_placeholders

    return serialized


class TransactionWriter:
    def __init__(self, auto_commit: bool = False, commit_every: int = TRANSACTION_MAX_ITEMS):
        self.items: list = []
        self.auto_commit = auto_commit
        assert (
            commit_every <= TRANSACTION_MAX_ITEMS
        ), f"commit_every cannot exceed DynamoDB limit of {TRANSACTION_MAX_ITEMS} items"
        self.commit_max = commit_every
        self.batches_submitted = 0
        self._first_table: Optional[Type["main.Dyntastic"]] = None
        self._context_var_reset_token: Optional[Token] = None

    def __enter__(self):
        self._context_var_reset_token = _transaction_writer_var.set(self)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        assert self._context_var_reset_token is not None
        _transaction_writer_var.reset(self._context_var_reset_token)
        self._context_var_reset_token = None

        if not exc_type:
            self.commit()

    def commit(self):
        if self.items:
            assert self._first_table is not None, "If any items were added, there should be a table"
            client = self._first_table._dynamodb_client()
            client.transact_write_items(TransactItems=self.items)
            self.items = []
            self.batches_submitted += 1

    def _register_table(self, table: Type["main.Dyntastic"]):
        if self._first_table is None:
            self._first_table = table
        else:
            assert self._first_table.__table_region__ == table.__table_region__, (
                "All tables in a transaction must be in the same region "
                f"(found {self._first_table.__table_region__} and {table.__table_region__})"
            )

    def _at_max_items(self):
        return len(self.items) == TRANSACTION_MAX_ITEMS

    def _at_commit_max(self):
        return len(self.items) == self.commit_max

    def add(self, table: Type["main.Dyntastic"], item: dict):
        if not self.auto_commit and self._at_max_items():
            raise Exception(f"Exceeded max items ({TRANSACTION_MAX_ITEMS}) in transaction")

        self._register_table(table)
        self.items.append(item)

        if self.auto_commit and self._at_commit_max():
            self.commit()
