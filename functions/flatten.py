from collections.abc import Mapping


def flatten_keys(d, parent_key="", sep="."):
    items = []
    for k, v in d.items():
        if k == "$insert":
            for sub_k in v.keys():
                new_key = f"{parent_key}{sep}{sub_k}" if parent_key else sub_k
                items.append((new_key, ["", "--NEW--"]))
        elif k == "$delete":
            for sub_k in v.keys():
                new_key = f"{parent_key}{sep}{sub_k}" if parent_key else sub_k
                items.append((new_key, ["--DELETE--", ""]))
        else:
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(flatten_keys(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
    return dict(items)
