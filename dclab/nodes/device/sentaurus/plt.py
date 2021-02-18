import fnmatch
import re
import numpy as np
import pandas
import dclab.core as core
import dclab.nodes.device.sentaurus.device as device


def plt_to_dataframe(plt_content):

    s_labels = plt_content.split("]")[0].split("[")[1].replace("\n", " ")

    a_labels = (
        re.sub(" +", " ", s_labels)
        .replace('" "', ",")
        .replace(' "', "")
        .replace('" ', "")
        .split(",")
    )

    # Convert string to list of strings
    data = plt_content.split("Data {")[1].replace("}", "").replace("\n", " ")
    data = re.sub(" +", " ", data).split(" ")
    data = list(filter(None, data))

    n_columns = len(a_labels)
    assert len(data) % n_columns == 0
    n_rows = int(len(data) / n_columns)
    data_n = np.asarray(data).astype(float).reshape((n_rows, n_columns))

    dataframe = pandas.DataFrame(data=data_n, columns=a_labels)
    return dataframe


class LoadPLT(core.Node):
    def __init__(self, source_node, filter=".*.plt", load_id=0, output_filename=""):
        super(LoadPLT, self).__init__()
        self.input_file = core.FileDependency(
            source_node=source_node, file_filter=filter, select_id=load_id
        )
        self.output_filename = output_filename

    def initialize(self, state):

        self.data = None

    def run(self):
        with open(self.input_file, "r") as f:
            self.data = plt_to_dataframe(f.read())
        return

    def output(self):
        if self.output_filename:
            self.data.to_csv(path_or_buf=self.sim_dir + self.output_filename)
        return {}


class Device(core.Node):
    def __init__(
        self,
        cmd_template_filename,
        grid_node,
        parameter_template_filename=None,
        grid_file_filter=".*_msh\.tdr",
        current_file="current.plt",
        plot_file="plot.tdr",
        suffix="_device",
        output_filename="",
        filter=".*\.plt",
        load_id=0,
    ):
        super(Device, self).__init__()
        device_node = device.Device(
            cmd_template_filename=cmd_template_filename,
            grid_node=grid_node,
            parameter_template_filename=parameter_template_filename,
            grid_file_filter=grid_file_filter,
            current_file=current_file,
            plot_file=plot_file,
            suffix=suffix,
        )
        output_node = LoadPLT(
            source_node=device_node,
            filter=filter,
            load_id=load_id,
            output_filename=output_filename,
        )

        self.data = core.Dependency(source_node=output_node, attribute="data")
