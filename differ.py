import streamlit as st
from streamlit.runtime.uploaded_file_manager import UploadedFile
import json
import jsondiff
import pandas as pd
from functions.flatten import flatten_keys

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
    "old",
    "unmodified",
    "modified.body",
    "modified.configs",
    "modified.persisted_descriptions",
    "modified.relation",
    "modified.macros",
    "modified.contract"
]
state_method = st.selectbox(label="State comparison method:", options=state_options)
properties_to_ignore = st.multiselect("Properties to ignore when showing node-level diffs:", ['created_at', 'root_path', 'build_path', 'compiled_path', 'deferred', 'schema', 'checksum', 'compiled_code'], default=['created_at', 'checksum'])

def load_manifest(file: UploadedFile) -> WritableManifest:
    data = json.load(file)
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

    state_inclusion_reasons_by_node = {}
    for state_option in state_options:
        results = state_comparator.search(included_nodes, state_option)
        for node in results:
            if node in state_inclusion_reasons_by_node:
                state_inclusion_reasons_by_node[node].append(state_option)
            else:
                state_inclusion_reasons_by_node[node] = [state_option]

    selected_nodes = list(state_comparator.search(included_nodes, state_method))
    
    st.header("Modified macros")
    if state_comparator.modified_macros:
        st.write(state_comparator.modified_macros)
        #macro_diffs = [
        #    {
        #        "unique_id": macro_uid,
        #        "left": left_manifest.macros.get(macro_uid).macro_sql,
        #        "right": right_manifest.macros.get(macro_uid).macro_sql
        #    }
        #    for macro_uid in state_comparator.modified_macros
        #]
        #st.write(macro_diffs)
    else:
        st.write("No modified macros")
    
    if len(selected_nodes) == 0:
        st.write("No diffs!")
    
    st.header("Selected nodes")
    for unique_id in selected_nodes:
        
        left_node = left_manifest.nodes.get(unique_id)
        right_node = right_manifest.nodes.get(unique_id)
        
        if left_node and right_node:
            left_dict = left_node.to_dict()
            right_dict = right_node.to_dict()
            diffs = {
                k: jsondiff.diff(left_dict[k], right_dict[k], syntax='symmetric', marshal=True)
                for k in left_dict if left_dict[k] != right_dict[k] and k not in properties_to_ignore
            }
            st.write(unique_id)
            st.write(state_inclusion_reasons_by_node[unique_id])
            # st.json(jdiff)
            if left_node.depends_on.macros and state_comparator.modified_macros:
                st.write(f"Depends on macros: {left_node.depends_on.macros}")
            st.json(diffs, expanded=False)
            df = pd.DataFrame.from_dict(flatten_keys(diffs)).transpose()
            st.dataframe(df, hide_index=False)
        elif not left_node:
            st.write(f"{unique_id} is missing in left manifest")
        elif not right_node:
            st.write(f"{unique_id} is missing in right manifest")
        
else:
    st.warning("Upload two manifests to begin comparison", icon="👯")
