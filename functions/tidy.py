import operator

def remove_irrelevant_key(object, key):
    for node in object.keys():
        if key in object[node]:
            del object[node][key]
        elif isinstance(object[node], list):
            # the `disabled` node contains single-item lists for some reason?
            if key in object[node][0]:
                del object[node][0][key]

def sort_depends_ons(object, key):
    for node in object.keys():
        if key in object[node]:
            for subnode in object[node][key]:
                object[node][key][subnode].sort()

def sort_sources(object):
    for node in object.keys():
        if "sources" in object[node]:
            object[node]["sources"].sort(key = operator.itemgetter(0, 1))