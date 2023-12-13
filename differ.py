import streamlit as st
from streamlit.runtime.uploaded_file_manager import UploadedFile
import json
import jsondiff
import pandas as pd
from functions.flatten import flatten_keys
from functions import tidy

# Minimal viable imports from dbt-core
from dbt.contracts.graph.manifest import WritableManifest
from dbt.graph.selector_methods import StateSelectorMethod

class MockPreviousState:
    def __init__(self, manifest: WritableManifest) -> None:
        self.manifest: Manifest = manifest

st.title("dbt Manifest Differ")

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
    "modified.contract"
]
state_method = st.selectbox(label="State comparison method:", options=state_options)
properties_to_ignore = st.multiselect("Properties to ignore when showing node-level diffs:", ['created_at', 'root_path', 'build_path', 'compiled_path', 'deferred', 'schema', 'checksum', 'compiled_code', 'database', 'relation_name'], default=['created_at', 'checksum', 'database', 'schema', 'relation_name', 'compiled_path', 'root_path', 'build_path'])
skipped_large_seeds = set()

def load_manifest(file: UploadedFile) -> WritableManifest:
    data = json.load(file)
    data, large_seeds = tidy.remove_large_seeds(data)
    skipped_large_seeds.update(large_seeds)
    return WritableManifest.upgrade_schema_version(data)

left_file = left_col.file_uploader("First manifest", type='json', help="Pick your left json file")
if left_file is not None:
    left_manifest = load_manifest(left_file)

right_file = right_col.file_uploader("Second manifest", type='json', help="Pick your right json file")
if right_file is not None:
    right_manifest = load_manifest(right_file)

if left_file and right_file:
    # TODO: also calculate diffs for sources, exposures, semantic_models, metrics
    included_nodes = set(left_manifest.nodes.keys())
    previous_state = MockPreviousState(right_manifest)
    state_comparator = StateSelectorMethod(left_manifest, previous_state, "")

    if len(skipped_large_seeds) > 0:
        st.warning(f"Some large seeds couldn't be compared from the manifest alone: {skipped_large_seeds}" )

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
        
    st.header(f"{len(selected_nodes)} Selected node{'s' if len(selected_nodes) != 1 else ''}")
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
                    syntax='symmetric', 
                    marshal=True
                )
                for k in all_keys 
                if k not in properties_to_ignore and (
                    k not in right_dict 
                    or k not in left_dict 
                    or left_dict[k] != right_dict[k]
                )
            }
            
            st.write("State methods that pick this node up:")
            st.code(state_inclusion_reasons_by_node[unique_id])
            
            if left_node.depends_on.macros and state_comparator.modified_macros:
                st.write(f"Depends on macros: {left_node.depends_on.macros}")
            
            st.write("JSON tree of diffs:")
            st.json(diffs, expanded=False)

            st.write("Flat table of diffs:")
            flattened_diff = flatten_keys(diffs)
            df = pd.DataFrame.from_dict(flattened_diff, orient='index')
            st.dataframe(df)
        
        elif not left_node:
            st.write(f"Missing from left manifest (brand new node)")
        
        elif not right_node:
            st.write(f"Missing from right manifest (deleted node)")
            st.write("State methods that pick this node up:")
            st.code(state_inclusion_reasons_by_node[unique_id])

        st.divider()
        
else:
    st.warning("Upload two manifests to begin comparison", icon="👯")
