import copy
import os
import pathlib
import shutil
import pandas as pd
from .. import core
from dclab.core import data, schedule, dependency
from dclab import __version__ as fluxo_version


class Simulation(object):
    def __init__(
        self,
        doe_filename=None,
        defaults_filename={},
        simulation_directory=None,
        simulation_name=None,
        clean_slate=True,
        frequent_output=True,
        allow_execution_skipping=True,
        output_filename="output.csv",
    ):
        # Initialize simulation state
        initial_state = data.SimulationMemory()
        initial_state.load_defaults(defaults_filename)
        if doe_filename:
            initial_state.load_doe(doe_filename)

        self.state = initial_state
        self.state.add_data_to_all_rows("fluxo", fluxo_version)

        # Generate simulation directory

        if simulation_directory is None:
            if doe_filename:
                doe_filename_full = os.path.abspath(doe_filename)
                simulation_directory = os.path.dirname(doe_filename_full)
            else:
                simulation_directory = os.getcwd()
        self.simulation_directory = simulation_directory

        if simulation_name is None:
            if doe_filename:
                simulation_name = os.path.splitext(os.path.split(doe_filename)[1])[0]
            else:
                simulation_name = "simulation"

        base_directory = simulation_directory + os.sep + simulation_name
        self.base_directory = pathlib.Path(base_directory)
        self.__generate_simulation_directory(
            simulation_directory=base_directory, clean_slate=clean_slate
        )

        self.nodes = list()
        self.schedule = list()
        self.output_frame = pd.DataFrame()
        self.frequent_output = frequent_output
        self.output_filename = self.base_directory / output_filename
        self.allow_execution_skipping = allow_execution_skipping

    def __generate_simulation_directory(self, simulation_directory, clean_slate):

        # Master directory
        if os.path.exists(simulation_directory) and clean_slate:
            shutil.rmtree(simulation_directory)
        if not os.path.exists(simulation_directory):
            os.mkdir(simulation_directory)
        self.state.add_data_to_all_rows(name="base_dir", data=simulation_directory)
        # Simulation directories
        for i in range(self.state.size()):
            current_row = self.state.get_row(i)
            current_folder = current_row.base_dir + os.sep + "run_" + str(i)
            if not os.path.exists(current_folder):
                os.mkdir(current_folder)
            self.state.add_data_at_index(
                name="sim_dir", data=current_folder + os.sep, index=i
            )

    def add_node(self, nodes):
        if isinstance(nodes, list):
            for node in nodes:
                self.add_node(node)
        else:
            self.nodes.append(nodes)
        return

    def num_sims(self):
        return self.state.size()

    def assemble_parents_and_find_hidden_nodes(self, node):

        if node not in self.nodes:
            self.nodes.append(node)
        node.assemble_parent_nodes()
        for parent in node.parent_nodes:
            self.assemble_parents_and_find_hidden_nodes(parent)

    def assemble_schedule(self):
        initial_nodes_list = copy.copy(self.nodes)
        for node in initial_nodes_list:
            self.assemble_parents_and_find_hidden_nodes(node)
        current_schedule = schedule.extract_schedule_from_node_list(self.nodes)
        self.schedule = current_schedule
        schedule_string = schedule.schedule_to_string(self.schedule)
        print(schedule_string)
        schedule_path = self.base_directory / "schedule.txt"
        with open(schedule_path, "w") as f:
            f.write(schedule_string)

        schedule_consistency = schedule.schedule_consistency_check(self.schedule)
        if schedule_consistency:
            print("Schedule consistency check: Passed")
        else:
            print("Schedule consistency check: Failed")
            exit(1)

        ## Writing unique node names based on schedule ID
        for node_id, node in enumerate(self.schedule):
            node.name = "{:02d}_{}".format(node_id, type(node).__name__)

    def find_matching_nodes(self, node, state_id):
        ids = range(state_id - 1, -1, -1)
        current_node_path = pathlib.Path(
            self.state.get_row(state_id).sim_dir
        ) / "{}/node_state.json".format(node.name)
        for row_id in ids:
            foreign_node_path = pathlib.Path(
                self.state.get_row(row_id).sim_dir
            ) / "{}/node_state.json".format(node.name)
            match = dependency.compare_node_states(current_node_path, foreign_node_path)
            if match[0]:
                return foreign_node_path
        return None

    def run_simulation(self):

        self.assemble_schedule()
        for node in self.nodes:
            node.save_state()
        self.output_frame = pd.DataFrame()
        # Loop through DOE list
        for i in range(self.num_sims()):
            current_state = self.state.get_row(index=i)
            output = self.__run_schedule_with_state(state_id=i)
            new_row = pd.DataFrame(data={**current_state, **output}, index=list([i]))
            self.output_frame = pd.concat([self.output_frame, new_row])
            if self.frequent_output:
                self.export_results_to_csv(self.output_filename)

    def __run_schedule_with_state(self, state_id):
        state = self.state.get_row(index=state_id)
        row_output = {}
        for node_id, node in enumerate(self.schedule):
            folder_name = node.name
            node_sim_dir = pathlib.Path(state.sim_dir) / folder_name
            node_sim_dir.mkdir(parents=True, exist_ok=True)

            node.reset_state()
            node.collect_dependencies()
            node.sim_dir = str(node_sim_dir) + os.sep
            current_node_state = node.save_node_to_disk(
                node_sim_dir, state, dependencies_only=True
            )
            matching_node = self.find_matching_nodes(node, state_id)
            if self.allow_execution_skipping and not (matching_node == None):
                node.load_node_from_disk(matching_node.parent)
            else:
                node.initialize(state)
                node.run()

            node.save_node_to_disk(node_sim_dir, state)
            node_output = node.output()
            if not node_output:
                node_output = {}
            row_output.update(node_output)
        for row in self.schedule:
            row.clean_up()
        return row_output

    def export_results_to_csv(self, filename):
        self.output_frame.to_csv(path_or_buf=filename, index_label="id")

    def __add__(self, other):
        if isinstance(other, core.Node) or isinstance(other, list):
            self.add_node(other)
        return self
