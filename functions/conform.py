"""
Conform a dbt 2.0-preview manifest dict so it can be deserialized
by dbt-core 1.11's WritableManifest / mashumaro.

Main issues addressed:
1. None values in dicts where dbt-core 1.11 expects non-Optional types
2. Missing `resource_type` field in docs and disabled entries
3. Missing required fields on some resource types (e.g., `description` on Metric)
4. Nested None values throughout (in columns, constraints, etc.)
"""

import dataclasses
import typing
from functools import lru_cache
from typing import Any, Dict, Optional

from dbt.artifacts.resources.v1.model import Model
from dbt.artifacts.resources.v1.seed import Seed
from dbt.artifacts.resources.v1.analysis import Analysis
from dbt.artifacts.resources.v1.singular_test import SingularTest
from dbt.artifacts.resources.v1.generic_test import GenericTest
from dbt.artifacts.resources.v1.hook import HookNode
from dbt.artifacts.resources.v1.sql_operation import SqlOperation
from dbt.artifacts.resources.v1.snapshot import Snapshot
from dbt.artifacts.resources.v1.source_definition import SourceDefinition
from dbt.artifacts.resources.v1.exposure import Exposure
from dbt.artifacts.resources.v1.metric import Metric
from dbt.artifacts.resources.v1.documentation import Documentation
from dbt.artifacts.resources.v1.macro import Macro
from dbt.artifacts.resources.v1.semantic_model import SemanticModel
from dbt.artifacts.resources.v1.saved_query import SavedQuery
from dbt.artifacts.resources.v1.unit_test_definition import UnitTestDefinition
from dbt.artifacts.resources.v1.group import Group


def strip_nones(obj: Any) -> Any:
    """Recursively remove None values from dicts.

    This is intentionally aggressive - after calling this, use
    ensure_required_fields() to re-add required Optional fields as None.
    """
    if isinstance(obj, dict):
        return {k: strip_nones(v) for k, v in obj.items() if v is not None}
    elif isinstance(obj, list):
        return [strip_nones(v) for v in obj]
    return obj


@lru_cache(maxsize=256)
def _get_resolved_hints(cls: type) -> dict:
    """Get resolved type hints for a dataclass, handling string annotations."""
    try:
        return typing.get_type_hints(cls)
    except Exception:
        # Fallback: return raw field types as strings
        return {}


def _is_optional_type(resolved_type: Any) -> bool:
    """Check if a resolved type is Optional[X] (i.e., Union[X, None])."""
    origin = getattr(resolved_type, "__origin__", None)
    args = getattr(resolved_type, "__args__", None)
    if origin is typing.Union and args and type(None) in args:
        return True
    return False


def _is_container_type(resolved_type: Any) -> bool:
    """Check if the type is a dict/list/mapping/sequence container."""
    origin = getattr(resolved_type, "__origin__", None)
    if origin in (dict, list, tuple, set, frozenset):
        return True
    # Check for typing.Dict, typing.Mapping, etc.
    if origin is not None:
        name = getattr(origin, "__name__", "")
        if name in ("Dict", "Mapping", "List", "Sequence", "Set"):
            return True
    return False


def _extract_single_dataclass(resolved_type: Any) -> Optional[type]:
    """Extract a dataclass from a type like X or Optional[X], but NOT from containers."""
    if _is_container_type(resolved_type):
        return None

    if isinstance(resolved_type, type) and dataclasses.is_dataclass(resolved_type):
        return resolved_type

    # Handle Optional[X] / Union[X, None]
    origin = getattr(resolved_type, "__origin__", None)
    args = getattr(resolved_type, "__args__", None)

    if origin is typing.Union and args:
        for arg in args:
            if arg is type(None):
                continue
            if _is_container_type(arg):
                return None
            if isinstance(arg, type) and dataclasses.is_dataclass(arg):
                return arg
    return None


def _extract_list_item_dataclass(resolved_type: Any) -> Optional[type]:
    """Extract a dataclass from List[X] or Sequence[X]."""
    origin = getattr(resolved_type, "__origin__", None)
    args = getattr(resolved_type, "__args__", None)

    if origin in (list,) and args:
        for arg in args:
            if isinstance(arg, type) and dataclasses.is_dataclass(arg):
                return arg
    return None


def _default_for_resolved_type(resolved_type: Any) -> Any:
    """Return a sensible default for a missing required field given its resolved type."""
    if _is_optional_type(resolved_type):
        return None

    type_str = str(resolved_type)
    if "List" in type_str or "Sequence" in type_str or resolved_type is list:
        return []
    if "Dict" in type_str or "Mapping" in type_str or resolved_type is dict:
        return {}
    if resolved_type is bool:
        return False
    if resolved_type is int:
        return 0
    if resolved_type is float:
        return 0.0
    return ""


def _default_for_field_raw(f: dataclasses.Field) -> Any:
    """Fallback default using raw (possibly string) type annotation."""
    type_str = str(f.type)
    if "Optional" in type_str or "None" in type_str:
        return None
    if "List" in type_str or "Sequence" in type_str:
        return []
    if "Dict" in type_str or "Mapping" in type_str:
        return {}
    if "bool" in type_str:
        return False
    if "int" in type_str:
        return 0
    if "float" in type_str:
        return 0.0
    return ""


def ensure_required_fields(entry: dict, cls: type, _seen: Optional[set] = None) -> dict:
    """Add missing required fields with sensible defaults based on the dataclass.

    Recurses into nested dataclass fields to fix them too.
    Uses typing.get_type_hints() to resolve string (forward reference) annotations.
    """
    if not dataclasses.is_dataclass(cls):
        return entry

    # Prevent infinite recursion
    if _seen is None:
        _seen = set()
    if id(entry) in _seen:
        return entry
    _seen.add(id(entry))

    hints = _get_resolved_hints(cls)

    for f in dataclasses.fields(cls):
        has_default = (
            f.default is not dataclasses.MISSING
            or f.default_factory is not dataclasses.MISSING
        )
        resolved = hints.get(f.name)

        if f.name not in entry:
            if not has_default:
                if resolved is not None:
                    entry[f.name] = _default_for_resolved_type(resolved)
                else:
                    entry[f.name] = _default_for_field_raw(f)
        else:
            # Recurse into nested dataclass fields
            val = entry[f.name]
            if resolved is not None:
                if isinstance(val, dict):
                    inner_cls = _extract_single_dataclass(resolved)
                    if inner_cls is not None:
                        ensure_required_fields(val, inner_cls, _seen)
                elif isinstance(val, list):
                    inner_cls = _extract_list_item_dataclass(resolved)
                    if inner_cls is not None:
                        for item in val:
                            if isinstance(item, dict):
                                ensure_required_fields(item, inner_cls, _seen)

    return entry


# Map resource_type string -> dataclass
_RESOURCE_TYPE_TO_CLASS = {
    "model": Model,
    "seed": Seed,
    "analysis": Analysis,
    "test": GenericTest,  # both singular and generic use 'test'
    "operation": HookNode,
    "sql_operation": SqlOperation,
    "snapshot": Snapshot,
    "source": SourceDefinition,
    "exposure": Exposure,
    "metric": Metric,
    "doc": Documentation,
    "macro": Macro,
    "semantic_model": SemanticModel,
    "saved_query": SavedQuery,
    "unit_test": UnitTestDefinition,
    "group": Group,
}

# Map unique_id prefix -> resource_type value
_PREFIX_TO_RESOURCE_TYPE = {
    "model": "model",
    "seed": "seed",
    "analysis": "analysis",
    "test": "test",
    "operation": "operation",
    "snapshot": "snapshot",
    "source": "source",
    "exposure": "exposure",
    "metric": "metric",
    "doc": "doc",
    "macro": "macro",
    "semantic_model": "semantic_model",
    "saved_query": "saved_query",
    "unit_test": "unit_test",
    "group": "group",
}

# Map manifest section -> dataclass (for sections with a uniform type)
_SECTION_CLASS = {
    "sources": SourceDefinition,
    "exposures": Exposure,
    "metrics": Metric,
    "docs": Documentation,
    "macros": Macro,
    "semantic_models": SemanticModel,
    "saved_queries": SavedQuery,
    "unit_tests": UnitTestDefinition,
    "groups": Group,
}


def _infer_resource_type(unique_id: str) -> Optional[str]:
    """Infer resource_type from the unique_id prefix."""
    prefix = unique_id.split(".")[0] if unique_id else None
    return _PREFIX_TO_RESOURCE_TYPE.get(prefix)


def _fix_entry(entry: dict, cls: type) -> dict:
    """Fix a single manifest entry: ensure required fields exist."""
    ensure_required_fields(entry, cls)
    return entry


def _fix_node(entry: dict) -> dict:
    """Fix a node entry (from 'nodes' or 'disabled' sections)."""
    rt = entry.get("resource_type")
    if rt is None:
        uid = entry.get("unique_id", "")
        rt = _infer_resource_type(uid)
        if rt is not None:
            entry["resource_type"] = rt

    cls = _RESOURCE_TYPE_TO_CLASS.get(rt)
    if cls is not None:
        _fix_entry(entry, cls)
    return entry


def conform_manifest(data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Preprocess raw manifest JSON dict to make it compatible with
    dbt-core 1.11's WritableManifest deserialization.
    """
    # 1. Recursively strip None values (fixes non-Optional fields that got None)
    data = strip_nones(data)

    # 2. Fix nodes section
    nodes = data.get("nodes", {})
    for key in nodes:
        _fix_node(nodes[key])

    # 3. Fix typed sections (sources, exposures, metrics, etc.)
    for section_name, cls in _SECTION_CLASS.items():
        section = data.get(section_name, {})
        if isinstance(section, dict):
            for key in section:
                entry = section[key]
                # Ensure resource_type is set
                if "resource_type" not in entry:
                    uid = entry.get("unique_id", "")
                    rt = _infer_resource_type(uid)
                    if rt is not None:
                        entry["resource_type"] = rt
                _fix_entry(entry, cls)

    # 4. Fix disabled section
    disabled = data.get("disabled", {})
    if isinstance(disabled, dict):
        for key in disabled:
            items = disabled[key]
            if isinstance(items, list):
                for item in items:
                    _fix_node(item)
            else:
                _fix_node(items)

    return data
