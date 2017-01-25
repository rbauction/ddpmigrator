import os
import yaml
from collections import OrderedDict


# Loads YAML as an ordered list
def ordered_load(stream, loader_class=yaml.SafeLoader, object_pairs_hook=OrderedDict):
    # noinspection PyClassHasNoInit
    class OrderedLoader(loader_class):
        pass

    def construct_mapping(loader, node):
        loader.flatten_mapping(node)
        return object_pairs_hook(loader.construct_pairs(node))

    OrderedLoader.add_constructor(
        yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
        construct_mapping)
    return yaml.load(stream, OrderedLoader)


def load_multiple_yaml(changed_rows, unique_key, root_dir):
    rows = dict()
    field_names = list()
    for file_name in changed_rows:
        with open(os.path.join(root_dir, file_name), 'r') as file:
            row = yaml.load(file)
            rows[row[unique_key]] = list(row.values())
            field_names = list(row.keys())
    return field_names, rows


def load_one_yaml(file_name):
    with open(file_name, 'r') as file:
        return yaml.load(file)
