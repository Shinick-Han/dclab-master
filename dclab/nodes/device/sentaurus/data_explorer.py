import fnmatch
import re
import os
import subprocess
from io import StringIO
import numpy as np
import pandas as pd
import dclab.core as core
import dclab.core.decorators as decorators


def extract_region_masks(vertex_dict, target_frame):
    for key, value in vertex_dict.items():
        target_frame[key] = pd.Series(data=np.ones(np.array(value).shape), index=value)
        target_frame[key].fillna(value=0, inplace=True)
    return target_frame


def match_vertex_with_data_to_series(vertex_dict, data_dict):
    output_vertices = list()
    output_data = list()
    for key, data in data_dict.items():
        assert key in vertex_dict
        vertices = vertex_dict[key]
        assert len(data) == len(vertices)
        output_vertices += vertices
        output_data += data
    output_series = pd.Series(data=output_data, index=output_vertices).sort_index()
    output_series = output_series[~output_series.index.duplicated(keep="first")]
    return output_series


def extract_data_for_parameter(dat, parameter_name, is_integer=False):
    elements = dat.split("Data ")[1].split('"' + parameter_name + '"')[1:]
    output_dict = {}
    for element in elements:
        region_name = element.split("validity")[1].split('"')[1]
        type = element.split("type")[1].split("\n")[0].replace("=", "").replace(" ", "")
        if type == "scalar":
            data_str = (
                element.split("Values")[1]
                .split("{")[1]
                .split("}")[0]
                .replace("\n", "")
                .strip()
                .split(" ")
            )
            converter = lambda x: float(x)
            if is_integer:
                converter = lambda x: int(float(x))
            data = list(map(converter, data_str))
            output_dict[region_name] = data
    return output_dict


def load_dfise_file(grd_filename, dat_filename):
    grd = ""
    dat = ""

    with open(grd_filename, "r") as f:
        grd = f.read()
    with open(dat_filename, "r") as f:
        dat = f.read()

    return parse_dfise_str(grd, dat)


def parse_dfise_str(grd, dat):
    # Vertices
    vertices_str = (
        "X Y" + grd.split("Data {")[1].split("Vertices")[1].split("{")[1].split("}")[0]
    )
    output_frame = pd.read_csv(StringIO(vertices_str), delim_whitespace=True)

    # Parameters
    parameter_list_str = (
        dat.split("datasets")[1].split("[")[1].split("]")[0].strip().split('" "')
    )
    replacer = lambda s: s.replace('"', "")
    parameter_list = list(set(map(replacer, parameter_list_str)))
    assert "VertexIndex" in parameter_list
    indices = extract_data_for_parameter(
        dat=dat, parameter_name="VertexIndex", is_integer=True
    )

    for parameter in parameter_list:
        # Get data from dfise string
        dataset = extract_data_for_parameter(dat=dat, parameter_name=parameter)
        # Match to vertex index
        data_series = match_vertex_with_data_to_series(
            vertex_dict=indices, data_dict=dataset
        )
        # Add to output dataframe
        output_frame[parameter] = data_series
    output_frame = extract_region_masks(vertex_dict=indices, target_frame=output_frame)
    return output_frame

    pass


class DataExplorer(core.Node):
    def __init__(self, source_node, filter="*.tdr"):
        super(DataExplorer, self).__init__()
        self.input_files = core.FileDependency(
            source_node=source_node, file_filter=filter, select_id=-1
        )

    @decorators.record_output
    def run(self):

        for file in self.input_files:
            command = (
                "(cd "
                + str(self.sim_dir)
                + " && tdx -rel N-2017.09 -dd -M 0 -S 0 "
                + str(file)
                + ")"
            )
            subprocess.run(command, shell=True)

        return


class ImportDFISE(core.Node):
    def __init__(
        self, source_node, filter="", index=0, csv_output_filename="dfise.csv"
    ):
        super(ImportDFISE, self).__init__()
        self.source_file = core.FileDependency(
            source_node=source_node, file_filter=filter, select_id=index
        )
        self.csv_output_filename = csv_output_filename

    def initialize(self, state):
        self.input_files = {}

        source_file_list = self.source_file

        if isinstance(self.source_file, str):
            source_file_list = list([self.source_file])

        source_file_list = list(set([os.path.splitext(f)[0] for f in source_file_list]))
        source_key_list = [os.path.split(f)[1] for f in source_file_list]

        self.input_files = dict(zip(source_key_list, source_file_list))

        self.data = {}

    def run(self):
        for key, value in self.input_files.items():
            grd_file = value + ".grd"
            dat_file = value + ".dat"
            self.data[key] = load_dfise_file(
                grd_filename=grd_file, dat_filename=dat_file
            )
        if len(self.data) == 1:
            self.data = list(self.data.values())[0]

        # exposing node output
        self.node_output["data"] = self.data
        return

    def output(self):
        if self.csv_output_filename:
            if isinstance(self.data, dict):
                for key, value in self.data.items():
                    filename = (
                        os.path.splitext(self.csv_output_filename)[0]
                        + "_"
                        + key
                        + ".csv"
                    )
                    value.to_csv(path_or_buf=self.sim_dir + filename)
            else:
                self.data.to_csv(path_or_buf=self.sim_dir + self.csv_output_filename)
        return {}


class ImportTDR(core.Node):
    def __init__(self, source_node, filter=".*.tdr", index=0, csv_output_filename=""):
        super(ImportTDR, self).__init__()
        data_node = DataExplorer(source_node=source_node, filter=filter)
        dfise_node = ImportDFISE(
            source_node=data_node, index=index, csv_output_filename=csv_output_filename
        )
        self.data = core.Dependency(source_node=dfise_node, attribute="data")
        self.input_files = core.Dependency(
            source_node=data_node, attribute="input_files"
        )

