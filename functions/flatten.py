from collections.abc import Mapping


def flatten_keys(dictionary, separator="."):
    result = {}
    for key, value in dictionary.items():
        if isinstance(value, Mapping):
            result.update(
                (key + separator + k, v if v is not None else ['N/A'])
                for k, v in flatten_keys(value, separator).items()
            )
        else:
            result[key] = value

    return result
