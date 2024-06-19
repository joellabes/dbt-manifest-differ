import json
from pathlib import Path

import jsondiff
import pandas as pd
import streamlit as st
from dbt.cli.flags import Flags
from dbt.cli.types import Command as CliCommand
from dbt.flags import set_flags
from streamlit.runtime.uploaded_file_manager import UploadedFile

from functions import tidy
from functions.flatten import flatten_keys

# we need to make sure that `~/.dbt` exists so that settings Flags doesn't crash
Path("~/.dbt").expanduser().mkdir(exist_ok=True)

flags = Flags.from_dict(CliCommand.LIST, {})
set_flags(flags)

# Minimal viable imports from dbt-core
from typing import List, Optional

import dbt.adapters.factory
from dbt.adapters.base import ConstraintSupport
from dbt.contracts.graph.manifest import WritableManifest
from dbt.graph.selector_methods import StateSelectorMethod
from dbt_common.contracts.constraints import ConstraintType


def mock_get_adapter_constraint_support(name: Optional[str]) -> List[str]:
    # return some fixed values as the FACTORY object is empty when running the program
    return {
        ConstraintType.check: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.not_null: ConstraintSupport.ENFORCED,
        ConstraintType.unique: ConstraintSupport.NOT_SUPPORTED,
        ConstraintType.primary_key: ConstraintSupport.NOT_ENFORCED,
        ConstraintType.foreign_key: ConstraintSupport.NOT_ENFORCED,
    }


dbt.adapters.factory.get_adapter_constraint_support = (
    mock_get_adapter_constraint_support
)


class MockPreviousState:
    def __init__(self, manifest: WritableManifest) -> None:
        self.manifest: Manifest = manifest


st.set_page_config(layout="wide")
st.title("dbt Manifest Differ")

st.info(
    """Work out why your models built in a Slim CI run.

Upload your production manifest on the left side, and the manifest from your Slim CI run on the right side. 

False positives in `modified.configs` are likely to be due to `config()` blocks in a node's definition or in its `.yml` resource file. 
        
To avoid false positives, define configs in `dbt_project.yml` instead. See [the docs on state comparison](https://docs.getdbt.com/reference/node-selection/state-comparison-caveats#false-positives) for more information.
""",
    icon="💡",
)

left_col, right_col = st.columns(2)
left_manifest: WritableManifest = None
right_manifest: WritableManifest = None

# Copy-paste from https://github.com/dbt-labs/dbt-core/blob/0ab954e1af9bb2be01fa4ebad2df7626249a1fab/core/dbt/graph/selector_methods.py#L676
state_options = [
    "modified",
    "new",
    "modified.body",
    "modified.configs",
    "modified.persisted_descriptions",
    "modified.relation",
    "modified.macros",
    "modified.contract",
]
state_method = st.selectbox(label="State comparison method:", options=state_options)
properties_to_ignore = st.multiselect(
    "Properties to ignore when showing node-level diffs:",
    [
        "created_at",
        "root_path",
        "build_path",
        "compiled_path",
        "deferred",
        "schema",
        "checksum",
        "compiled_code",
        "database",
        "relation_name",
    ],
    default=[
        "created_at",
        "checksum",
        "database",
        "schema",
        "relation_name",
        "compiled_path",
        "root_path",
        "build_path",
    ],
)
skipped_large_seeds = set()


def load_manifest(file: UploadedFile) -> WritableManifest:
    data = json.load(file)
    data, large_seeds = tidy.remove_large_seeds(data)
    skipped_large_seeds.update(large_seeds)
    return WritableManifest.upgrade_schema_version(data)


left_file = left_col.file_uploader(
    "First manifest", type="json", help="Pick your left json file"
)
if left_file is not None:
    left_manifest = load_manifest(left_file)

right_file = right_col.file_uploader(
    "Second manifest", type="json", help="Pick your right json file"
)
if right_file is not None:
    right_manifest = load_manifest(right_file)

if left_file and right_file:
    # TODO: also calculate diffs for sources, exposures, semantic_models, metrics
    included_nodes = set(left_manifest.nodes.keys())
    previous_state = MockPreviousState(right_manifest)
    state_comparator = StateSelectorMethod(left_manifest, previous_state, "")

    if len(skipped_large_seeds) > 0:
        st.warning(
            f"Some large seeds couldn't be compared from the manifest alone: {skipped_large_seeds}"
        )

    state_inclusion_counts = {}
    state_inclusion_reasons_by_node = {}
    for state_option in state_options:
        results = list(state_comparator.search(included_nodes, state_option))
        for node in results:
            if node in state_inclusion_reasons_by_node:
                state_inclusion_reasons_by_node[node].append(state_option)
            else:
                state_inclusion_reasons_by_node[node] = [state_option]
        state_inclusion_counts[state_option] = len((results))

    st.bar_chart(state_inclusion_counts)
    selected_nodes = list(state_comparator.search(included_nodes, state_method))

    if state_comparator.modified_macros:
        st.header("Modified macros")
        st.write(state_comparator.modified_macros)

    st.header(
        f"{len(selected_nodes)} Selected node{'s' if len(selected_nodes) != 1 else ''}"
    )
    for unique_id in selected_nodes:
        left_node = left_manifest.nodes.get(unique_id)
        right_node = right_manifest.nodes.get(unique_id)
        st.subheader(unique_id)

        if left_node and right_node:
            left_dict = left_node.to_dict()
            right_dict = right_node.to_dict()
            all_keys = set(left_dict.keys()) | set(right_dict.keys())
            diffs = {
                k: jsondiff.diff(
                    left_dict.get(k, None),
                    right_dict.get(k, None),
                    syntax="symmetric",
                    marshal=True,
                )
                for k in all_keys
                if k not in properties_to_ignore
                and (
                    k not in right_dict
                    or k not in left_dict
                    or left_dict[k] != right_dict[k]
                )
            }

            st.write("##### State selectors that find this node:")
            st.code(state_inclusion_reasons_by_node[unique_id])

            if left_node.depends_on.macros and state_comparator.modified_macros:
                st.write(f"Depends on macros: {left_node.depends_on.macros}")

            diff_json, right_full_json = st.columns(2)

            diff_json.write("##### JSON tree of diffs:")
            diff_json.json(diffs, expanded=False)

            right_full_json.write("##### JSON tree of all elements in right node:")
            right_full_json.json(right_dict, expanded=False)

            st.write("##### Flat table of diffs:")
            try:
                flattened_diff = flatten_keys(diffs)
                df = pd.DataFrame.from_dict(flattened_diff, orient="index")
                st.dataframe(df, use_container_width=True)
            except Exception as e:
                st.error(f"Couldn't print as table: {e}")

        elif not left_node:
            st.warning(f"Missing from left manifest (brand new node)")

        elif not right_node:
            st.warning(f"Missing from right manifest (deleted node)")
            st.write("State methods that pick this node up:")
            st.code(state_inclusion_reasons_by_node[unique_id])

        st.divider()

else:
    st.warning("Upload two manifests to begin comparison", icon="👯")
