"""Closed, immutable representation for JSON object payloads."""

from __future__ import annotations

from types import MappingProxyType
from typing import Any, Mapping

_MAPPING_TYPES = (dict, MappingProxyType)
_SEQUENCE_TYPES = (list, tuple)
_SCALAR_TYPES = (str, int, bool)


def freeze_json_object(value: object, *, field_name: str) -> Mapping[str, Any]:
    """Copy and recursively freeze a JSON-like object using a closed type set."""
    if type(value) not in _MAPPING_TYPES:
        raise ValueError(f"{field_name} must be a JSON object")
    return _freeze_mapping(value, field_name=field_name, active_ids=set())  # type: ignore[arg-type]


def _freeze_mapping(
    value: Mapping[object, object],
    *,
    field_name: str,
    active_ids: set[int],
) -> Mapping[str, Any]:
    container_id = id(value)
    if container_id in active_ids:
        raise ValueError(f"{field_name} must be an acyclic JSON object")
    active_ids.add(container_id)
    try:
        if any(type(key) is not str for key in value):
            raise ValueError(f"{field_name} JSON object keys must be strings")
        return MappingProxyType(
            {
                key: _freeze_value(
                    value[key],
                    field_name=field_name,
                    active_ids=active_ids,
                )
                for key in sorted(value)  # type: ignore[type-var]
            }
        )
    finally:
        active_ids.remove(container_id)


def _freeze_value(
    value: object,
    *,
    field_name: str,
    active_ids: set[int],
) -> Any:
    if value is None or type(value) in _SCALAR_TYPES:
        return value
    if type(value) in _MAPPING_TYPES:
        return _freeze_mapping(
            value,  # type: ignore[arg-type]
            field_name=field_name,
            active_ids=active_ids,
        )
    if type(value) in _SEQUENCE_TYPES:
        container_id = id(value)
        if container_id in active_ids:
            raise ValueError(f"{field_name} must be an acyclic JSON object")
        active_ids.add(container_id)
        try:
            return tuple(
                _freeze_value(
                    item,
                    field_name=field_name,
                    active_ids=active_ids,
                )
                for item in value  # type: ignore[union-attr]
            )
        finally:
            active_ids.remove(container_id)
    raise ValueError(f"{field_name} contains a non-JSON value")
