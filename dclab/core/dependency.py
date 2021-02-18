import pathlib
import re
import json


class Dependency(object):
    def __init__(self, source_node, attribute):
        self.source_node = source_node
        self.attribute = attribute

    def get(self):
        return self.source_node.node_output[self.attribute]

    def __str__(self):
        return str(self.get())

    def __deepcopy__(self, memodict={}):
        return self


class FileDependency(Dependency):
    def __init__(
        self, source_node, attribute="output_files", file_filter="", select_id=0
    ):
        super(FileDependency, self).__init__(
            source_node=source_node, attribute=attribute
        )
        self.file_filter = file_filter
        self.select_id = select_id

    def get(self):
        possible_files = getattr(self.source_node, self.attribute)
        output_files = [f for f in possible_files if re.match(self.file_filter, f)]
        if self.select_id > -1:
            output_files = output_files[self.select_id]
        return output_files


def compare_node_states(node_file_1, node_file_2):
    node_file_1 = pathlib.Path(node_file_1)
    node_file_2 = pathlib.Path(node_file_2)

    # Loading node json
    with open(node_file_1, "r") as f:
        node_1 = json.loads(f.read())["dependencies"]
    with open(node_file_2, "r") as f:
        node_2 = json.loads(f.read())["dependencies"]

    # Parse step 1: state_variables

    sv_1 = node_1["state_variables"]
    sv_2 = node_2["state_variables"]

    if not (set(sv_1.keys()) == set(sv_2.keys())):
        return [False, "State variables key mismatch"]

    for key in sv_1:
        if not (sv_1[key] == sv_2[key]):
            return [False, "State variable value mismatch for {}".format(key)]

    # Parse step 2: Static files

    sf_1 = node_1["static_files"]
    sf_2 = node_2["static_files"]

    if not (len(sf_1) == len(sf_2)):
        return [False, "Static file list length mismatch"]

    if not (set(sf_1) == set(sf_2)):
        return [False, "Static file list content mismatch"]

    # Parse step 3: Parent nodes

    parents_1 = node_1["nodes"]
    parents_2 = node_2["nodes"]

    if not (len(parents_1) == len(parents_2)):
        return [False, "Number of nodes mismatch"]

    if not (set(parents_1) == set(parents_2)):
        return [False, "Node name mismatch"]

    for node in parents_1:
        new_node_1 = node_file_1.parents[1] / "{}/node_state.json".format(node)
        new_node_2 = node_file_2.parents[1] / "{}/node_state.json".format(node)
        compare = compare_node_states(new_node_1, new_node_2)
        if not compare[0]:
            return [False, "Parent node mismatch"]

    return [True, "OK"]

