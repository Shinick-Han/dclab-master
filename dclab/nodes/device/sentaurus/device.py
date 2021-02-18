import subprocess
import dclab.core as core
import dclab.utilities.preprocessor as preprocessor
import dclab.core.decorators as decorators


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
    ):
        super(Device, self).__init__()
        self.cmd_template_filename = cmd_template_filename
        self.parameter_template_filename = parameter_template_filename
        self.msh_filename = core.FileDependency(
            source_node=grid_node, file_filter=grid_file_filter
        )
        self.current_file = current_file
        self.plot_file = plot_file
        self.suffix = suffix

    def initialize(self, state):

        parameter_dict = state.to_dict()
        self.current_file = self.sim_dir + self.current_file
        self.plot_file = self.sim_dir + self.plot_file
        self.parameter_file = ""
        if self.parameter_template_filename:
            self.parameter_file = preprocessor.preprocess_file(
                filename=self.parameter_template_filename,
                parameter_dict={**parameter_dict, **self.__dict__},
                output_directory=self.sim_dir,
                suffix=self.suffix,
            )
        self.cmd_filename = preprocessor.preprocess_file(
            filename=self.cmd_template_filename,
            parameter_dict={**parameter_dict, **self.__dict__},
            output_directory=self.sim_dir,
            suffix=self.suffix,
        )

    @decorators.record_output
    def run(self):
        exec_command = (
            "(cd "
            + self.sim_dir
            + " && sdevice -rel N-2017.09 "
            + str(self.cmd_filename)
            + ")"
        )
        run_result = subprocess.run(exec_command, shell=True)
        return run_result

    def list_dependent_static_files(self):
        static_files = [str(self.cmd_template_filename)]
        if not (self.parameter_template_filename == None):
            static_files += [str(self.parameter_template_filename)]
        return static_files

    def list_dependent_state_variables(self, state):
        file_deps = preprocessor.get_dependencies_from_file(self.cmd_template_filename)
        if not (self.parameter_template_filename == None):
            file_deps += preprocessor.get_dependencies_from_file(
                self.parameter_template_filename
            )
        file_deps = [x for x in file_deps if x in state]
        # Removing doubles
        file_deps = list(set(file_deps))
        return file_deps
