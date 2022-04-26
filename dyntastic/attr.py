import json
from collections import defaultdict
from decimal import Decimal
from typing import Union

from boto3.dynamodb.conditions import Attr as _DynamoAttr
from boto3.dynamodb.conditions import Key as _DynamoKey
from boto3.dynamodb.types import TypeDeserializer
from pydantic import BaseModel
from pydantic.json import pydantic_encoder

# serialization helpers


# Except for sets and Decimal, json.loads(pydantic_model.json()) would work.
# To properly support these cases, however, we need to walk through the data.
def serialize(data):
    if isinstance(data, BaseModel):
        return serialize(data.dict())
    elif isinstance(data, dict):
        # TODO: May not actually want to filter out None. Without the filter,
        # all None fields in the pydantic model appear as Null instead of
        # nonexistent
        return {key: serialize(value) for key, value in data.items() if value is not None}
    elif isinstance(data, (list, tuple)):
        return list(map(serialize, data))
    elif isinstance(data, set):
        return set(map(serialize, data))
    elif isinstance(data, (Decimal, str, int, bytes, bool, float, type(None))):
        return data
    else:
        # handle types like datetime
        return json.loads(json.dumps(data, default=pydantic_encoder))


# Hacky way to avoid boto3's annoying Binary type
# used as a wrapper around standard bytes objects
TypeDeserializer._deserialize_b = lambda _, value: value  # type: ignore


# condition helpers


def _ensure_value(value):
    if isinstance(value, _BaseAttr):
        return value._attr
    else:
        return serialize(value)


def _attr_method(method_name: str):
    def _attr_operation(self, value):
        method = getattr(self._attr, method_name)
        return method(_ensure_value(value))

    return _attr_operation


def _key_method(method_name: str):
    def _key_operation(self, value):
        method = getattr(self._key, method_name)
        return method(_ensure_value(value))

    return _key_operation


# update helpers


class _Variables:
    def __init__(self):
        self.attributes = {}
        self.values = {}

    def add_variable(self, variable):
        if isinstance(variable, _UpdateFn):
            return variable.build(self)
        elif isinstance(variable, Attr):
            # TODO: support indexes in the path as well (e.g. "my_list[0].nested_attr")
            data = self.attributes
            key_prefix = "#"
            # if the attribute is a nested path, DynamoDB expects each segment to be
            # in a separate entry in ExpressionAttributeNames
            variables = variable.name.split(".")
        else:
            data = self.values
            key_prefix = ":"
            variables = [variable]

        segments = []
        for var in variables:
            key = f"{key_prefix}{len(data)}"
            data[key] = var
            segments.append(key)

        return ".".join(segments)


class _UpdateAction:
    update_action: str

    def build(self, variables: _Variables) -> str:  # pragma: no cover
        raise NotImplementedError

    def __str__(self):
        variables = _Variables()
        serialized = self.build(variables)

        action = f"{self.update_action} " if hasattr(self, "update_action") else ""
        expression = f"Expression='{action}{serialized}'"
        attributes = f"Attributes={variables.attributes}"
        values = f"Values={variables.values}"

        return f"{self.__class__.__name__}({expression}, {attributes}, {values})"

    def __repr__(self):
        return str(self)


class _UpdatePathValue(_UpdateAction):
    def __init__(self, path, value):
        self.path = path
        self.value = value

    def build(self, variables: _Variables):
        path_var = variables.add_variable(self.path)
        value_var = variables.add_variable(self.value)
        return self.format(path_var, value_var)

    def format(self, path_var: str, value_var: str):
        return f"{path_var} {value_var}"


class _ActionSet(_UpdatePathValue):
    update_action = "SET"

    def format(self, path_var: str, value_var: str):
        return f"{path_var} = {value_var}"


class _UpdateFn(_UpdateAction):
    fn_name: str

    def __init__(self, *args):
        self.args = args

    def build(self, variables: _Variables):
        arg_vars = map(variables.add_variable, self.args)
        formatted_args = ", ".join(arg_vars)
        return f"{self.fn_name}({formatted_args})"


class _IfNotExists(_UpdateFn):
    fn_name = "if_not_exists"


class _ListAppend(_UpdateFn):
    fn_name = "list_append"


class _Operator(_UpdateFn):
    operator: str

    def __init__(self, arg1, arg2):
        self.arg1 = arg1
        self.arg2 = arg2

    def build(self, variables: _Variables):
        arg1_var = variables.add_variable(self.arg1)
        arg2_var = variables.add_variable(self.arg2)
        return f"{arg1_var} {self.operator} {arg2_var}"


class _Plus(_Operator):
    operator = "+"


class _Minus(_Operator):
    operator = "-"


class _ActionRemove(_UpdateAction):
    update_action = "REMOVE"

    def __init__(self, path, index: int = None):
        # This uses `type(...) is not ...` instead of `isinstance` to
        # ensure that subtypes of int which might have a different
        # __str__ method cannot allow injection
        if index is not None and type(index) is not int:
            raise ValueError(f"Dyntastic remove() update must be given an int, found '{index.__class__.__name__}'")

        self.path = path
        self.index = index

    def build(self, variables: _Variables):
        path_var = variables.add_variable(self.path)

        if self.index is None:
            return path_var
        else:
            return f"{path_var}[{self.index}]"


class _ActionAdd(_UpdatePathValue):
    update_action = "ADD"


class _ActionDelete(_UpdatePathValue):
    update_action = "DELETE"


def translate_updates(*actions: _UpdateAction):
    variables = _Variables()
    serialized_actions = defaultdict(list)

    for action in actions:
        serialized_actions[action.update_action].append(action.build(variables))

    action_expressions = []
    for update_action, expressions in serialized_actions.items():
        joined_updates = ", ".join(expressions)
        action_expressions.append(f"{update_action} {joined_updates}")

    update_expression = " ".join(action_expressions)
    update_data = {
        "UpdateExpression": update_expression,
        "ExpressionAttributeNames": variables.attributes,
    }

    if variables.values:
        update_data["ExpressionAttributeValues"] = variables.values

    return update_data


# tying everything together


class _AttrMetaclass(type):
    def __getattr__(cls, name: str) -> "Attr":
        return cls(name)


class _BaseAttr(metaclass=_AttrMetaclass):
    def __init__(self, name: str):
        self.name = name
        self._key = _DynamoKey(name)
        self._attr = _DynamoAttr(name)

    def __str__(self):
        return f"Attr<{self.name}>"

    def __repr__(self):
        return str(self)

    # conditions that can work on a DynamoDB Key

    eq = __eq__ = _key_method("eq")
    lt = __lt__ = _key_method("lt")
    le = __le__ = _key_method("lte")
    gt = __gt__ = _key_method("gt")
    ge = __ge__ = _key_method("gte")
    begins_with = _key_method("begins_with")

    def between(self, low_value, high_value):
        return self._key.between(_ensure_value(low_value), _ensure_value(high_value))


class Attr(_BaseAttr):
    # conditions that can only work on a non-key attribute

    ne = __ne__ = _attr_method("ne")
    is_in = _attr_method("is_in")
    contains = _attr_method("contains")
    attribute_type = _attr_method("attribute_type")

    @property
    def size(self):
        return _Size(self.name)

    def exists(self):
        return self._attr.exists()

    def not_exists(self):
        return self._attr.not_exists()

    # update "subexpressions"

    def if_not_exists(self, value):
        return _IfNotExists(self, value)

    # TODO: allow _ListAppend to be created with list value in either position?
    def list_append(self: Union["Attr", list, tuple], value):
        if isinstance(value, tuple):
            value = list(value)
        elif not isinstance(value, list):
            value = [value]

        return _ListAppend(self, value)

    def __add__(self, value):
        return _Plus(self, value)

    def __radd__(self, value):
        return _Plus(value, self)

    plus = __add__

    def __sub__(self, value):
        return _Minus(self, value)

    def __rsub__(self, value):
        return _Minus(value, self)

    minus = __sub__

    # update expressions

    def set(self, value):
        return _ActionSet(self, value)

    def set_default(self, value):
        return self.set(self.if_not_exists(value))

    def append(self, value):
        return self.set(self.list_append(value))

    def remove(self, index: int = None):
        return _ActionRemove(self, index)

    def add(self, value):
        if isinstance(value, (str, bytes)):
            value = {value}
        elif isinstance(value, (tuple, list)):
            value = set(value)
        return _ActionAdd(self, value)

    def delete(self, value):
        if isinstance(value, (str, bytes)):
            value = {value}
        elif isinstance(value, (tuple, list)):
            value = set(value)
        return _ActionDelete(self, value)


class _Size(_BaseAttr):
    def __init__(self, name: str):
        self.name = name
        self._key = _DynamoAttr(name).size()  # type: ignore
        self._attr = _DynamoAttr(name).size()  # type: ignore


# Alias for ease of use
A = Attr
