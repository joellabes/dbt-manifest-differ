import streamlit as st
import json
import jsondiff as jd
from jsondiff import diff
from functions import tidy 

st.title("dbt Manifest Differ")

left_col, right_col = st.columns(2)
left_json = json.loads('{}')
right_json = json.loads('{}')

left_file = left_col.file_uploader("First manifest", type='json', help="Pick your left json file")
if left_file is not None:
    left_json = json.load(left_file)


right_file = right_col.file_uploader("Second manifest", type='json', help="Pick your right json file")
if right_file is not None:
    right_json = json.load(right_file)

properties_to_ignore = st.multiselect("Properties to ignore:", ['created_at', 'root_path', 'build_path', 'compiled_path', 'deferred', 'schema'], default=['created_at'])


sort_sources = {
    "nodes": ['sources']
}

if left_file and right_file:
    for node in ["nodes", "macros", "disabled", "exposures", "macros", "metrics", "semantic_models", "sources"]:
        for key in properties_to_ignore:
            tidy.remove_irrelevant_key(left_json[node], key)
            tidy.remove_irrelevant_key(right_json[node], key)

    tidy.sort_depends_ons(left_json["nodes"], "depends_on")
    tidy.sort_depends_ons(right_json["nodes"], "depends_on")

    tidy.sort_sources(left_json["nodes"])
    tidy.sort_sources(right_json["nodes"])

    result = diff(left_json, right_json, syntax='symmetric', marshal=True)
    st.json(result)
else:
    st.warning("Upload two manifests to begin comparison", icon="👯")