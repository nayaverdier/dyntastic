# pragma: nocover
from typing import TYPE_CHECKING, Any, Dict, Tuple, Type, TypeVar

import pydantic

_pydantic_major_version = int(pydantic.VERSION.split(".")[0])
IS_VERSION_1 = _pydantic_major_version == 1


BaseModelT = TypeVar("BaseModelT", bound=pydantic.BaseModel)

if TYPE_CHECKING:
    try:
        from pydantic.fields import ModelField as FieldInfo  # type: ignore[unused-ignore, attr-defined]
    except ImportError:
        from pydantic.fields import FieldInfo  # type: ignore[unused-ignore, assignment]

    def model_fields(model: Type[pydantic.BaseModel]) -> Dict[str, FieldInfo]: ...  # noqa: E704

    def model_dump(instance: pydantic.BaseModel, **kwargs) -> Dict[str, Any]: ...  # noqa: E704

    def annotation(field: FieldInfo) -> Any: ...  # noqa: E704

    def alias(field_name, field: Any) -> str: ...  # noqa: E704

    def to_jsonable_python(value: Any) -> Any: ...  # noqa: E704

    def try_model_construct(model: Type[BaseModelT], item: dict) -> Tuple[BaseModelT, bool]: ...  # noqa: E704

    class BaseModel(pydantic.BaseModel): ...  # noqa: E701

elif IS_VERSION_1:
    from pydantic.fields import ModelField as FieldInfo

    def model_fields(model: Type[pydantic.BaseModel]) -> Dict[str, pydantic.fields.ModelField]:
        return model.__fields__

    def model_dump(instance: pydantic.BaseModel, **kwargs) -> Dict[str, Any]:
        return instance.dict(**kwargs)

    def annotation(field: FieldInfo) -> Any:
        return field.type_

    def alias(field_name, field: FieldInfo) -> str:
        return field.alias

    def to_jsonable_python(value: Any) -> Any:
        import json

        from pydantic.json import pydantic_encoder

        return json.loads(json.dumps(value, default=pydantic_encoder))

    def try_model_construct(model: Type[BaseModelT], item: dict) -> Tuple[BaseModelT, bool]:
        validated, fields_set, errors = pydantic.validate_model(model, item)
        if errors:
            # assume KEYS_ONLY or INCLUDE index
            fields_in_dynamo = {key: value for key, value in validated.items() if key in fields_set}
            data = model.construct(**fields_in_dynamo)
            return data, True
        else:
            data = model(**item)
            return data, False

    class BaseModel(pydantic.BaseModel):
        class Config:
            allow_population_by_field_name = True

else:
    from typing import Union

    try:
        # Python >= 3.8
        from typing import get_args, get_origin
    except ImportError:
        # Python 3.7
        def get_args(t):
            return getattr(t, "__args__", ())

        def get_origin(t):
            return getattr(t, "__origin__", None)

    from pydantic.fields import FieldInfo

    def model_fields(model: Type[pydantic.BaseModel]) -> Dict[str, pydantic.fields.FieldInfo]:
        return model.model_fields

    def model_dump(instance: pydantic.BaseModel, **kwargs) -> Dict[str, Any]:
        return instance.model_dump(**kwargs)

    def annotation(field: FieldInfo) -> Any:
        # Get rid of Optional[...] type if present (this is only used for
        # creating tables, where we want to know the actual inner type)
        type_ = field.annotation
        if get_origin(type_) is Union:
            args = get_args(type_)
            if len(args) == 2 and args[1] is type(None):  # noqa: E721
                return args[0]

        return type_

    def alias(field_name: str, field: FieldInfo) -> str:
        return field.alias or field_name

    def to_jsonable_python(value: Any) -> Any:
        import pydantic_core

        return pydantic_core.to_jsonable_python(value)

    class _FieldCollector(dict):
        pass

    def try_model_construct(model: Type[BaseModelT], item: dict) -> Tuple[BaseModelT, bool]:
        # Note: Hopefully there will be a better way to do this in the future
        # Related issue https://github.com/pydantic/pydantic/issues/7586

        collector = _FieldCollector()
        try:
            data = model.model_validate(item, context=collector)
            return data, False
        except pydantic.ValidationError:
            return model.model_construct(**collector), True

    class BaseModel(pydantic.BaseModel):
        model_config = pydantic.ConfigDict(populate_by_name=True)

        # TODO: eliminate the case where nested Dyntastic models overwrite the
        # top level _FieldCollector, potential to cause annoying bugs
        @pydantic.field_validator("*", mode="after")
        def collect_valid_fields(cls, v, info):
            if isinstance(info.context, _FieldCollector):
                info.context[info.field_name] = v
            return v


def field_type(model: Type[pydantic.BaseModel], field: str) -> type:
    fields = model_fields(model)
    model_field = None

    # Try to match by alias before by name, to be consistent with pydantic
    for field_info in fields.values():
        if field_info.alias == field:
            model_field = field_info
            break
    else:
        model_field = fields.get(field)

    if model_field is None:
        raise ValueError(f"Field {field} is not present in {model}")

    return annotation(model_field)


__all__ = [
    "BaseModel",
    "model_fields",
    "model_dump",
    "annotation",
    "alias",
    "to_jsonable_python",
    "try_model_construct",
    "field_type",
]
