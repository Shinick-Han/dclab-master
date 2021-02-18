import copy
import json
import pathlib
import pickle
import os
import numpy
from . import dependency
import fluxo.core.utilities


class Node(object):
    def __init__(self):
        self.__internal_state__ = {}
        self.parent_nodes = list()
        self.sim_dir = ""
        self.node_output = {}
        self.output_files = list()
        self.clean_up_output = False
        self.name = "Node"
        return

    def save_state(self):
        self.__internal_state__ = copy.deepcopy(self.__dict__)

    def reset_state(self):
        self.__dict__ = copy.deepcopy(self.__internal_state__)
        self.save_state()

    def collect_dependencies(self):
        for key, obj in self.__dict__.items():
            if isinstance(obj, dependency.Dependency):
                self.__dict__[key] = obj.get()

    def initialize(self, state):
        # Initialize output variables and set to zero
        pass

    def add_attribute(self, attribute_name, attribute_value):
        setattr(self, attribute_name, attribute_value)

    def run(self):
        return

    def output(self):
        return {}

    def clean_up(self):
        if self.clean_up_output:
            for filename in self.output_files:
                if os.path.isfile(filename):
                    os.remove(filename)

    def assemble_parent_nodes(self):
        self.parent_nodes = list()
        for key, value in self.__dict__.items():
            if isinstance(value, dependency.Dependency):
                self.parent_nodes.append(value.source_node)
                value.source_node.assemble_parent_nodes()

    def get_node_outputs(self):
        return self.node_output

    def save_node_to_disk(self, folder_name, state, dependencies_only=False):
        folder_name = pathlib.Path(folder_name)
        output_filename = folder_name / "node_state.json"
        ## Getting a list of all dependencies
        dependencies = self.list_dependencies(state)
        state_variables = dependencies["state_variables"]
        ## Resolving the state variables to bake into the node state
        ## Need to convert to 'object' to make sure it is serializable
        variable_dict = {x: state.astype("object")[x] for x in state_variables}
        dependencies["state_variables"] = variable_dict
        output_dict = {
            "dependencies": dependencies,
        }

        if not dependencies_only:
            outputs = self.get_node_outputs()
            output_file = folder_name / "outputs.pkl"
            with open(output_file, "wb") as f:
                f.write(pickle.dumps(outputs))
            output_dict = {
                **output_dict,
                "node_output": str(output_file),
                "output_files": self.output_files,
            }

        output_string = json.dumps(
            output_dict, indent=3, cls=fluxo.core.utilities.JsonEncoder
        )
        with open(output_filename, "w") as f:
            f.write(output_string)
        return output_filename

    def load_node_from_disk(self, folder_name):
        folder_name = pathlib.Path(folder_name)
        import_filename = folder_name / "node_state.json"
        with open(import_filename, "r") as f:
            import_string = f.read()

        node_state = json.loads(import_string)
        self.output_files = node_state["output_files"]
        node_output_file = node_state["node_output"]
        with open(node_output_file, "rb") as f:
            self.node_output = pickle.loads(f.read())

    def list_dependent_nodes(self):
        return [x.name for x in self.parent_nodes]

    def list_dependent_state_variables(self, state):
        return []

    def list_dependent_static_files(self):
        return []

    def list_dependencies(self, state):
        return {
            "nodes": self.list_dependent_nodes(),
            "state_variables": self.list_dependent_state_variables(state),
            "static_files": self.list_dependent_static_files(),
        }
