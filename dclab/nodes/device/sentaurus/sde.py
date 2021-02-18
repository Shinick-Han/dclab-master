import subprocess
import os
import dclab.core as core
import dclab.utilities.preprocessor as preprocessor
import dclab.core.decorators as decorators


class SDE(core.Node):
    def __init__(self, cmd_template_filename, suffix="_sde"):
        super(SDE, self).__init__()
        self.cmd_template_filename = cmd_template_filename
        self.cmd_filename = ""
        self.suffix = suffix

    def initialize(self, state):
        parameter_dict = state.to_dict()
        output_directory = self.sim_dir
        self.cmd_filename = preprocessor.preprocess_file(
            filename=self.cmd_template_filename,
            parameter_dict={**parameter_dict, **self.__dict__},
            output_directory=output_directory,
            suffix=self.suffix,
        )

    @decorators.record_output
    def run(self):
        exec_command = (
            "(cd "
            + self.sim_dir
            + " && sde -rel N-2017.09 -e -2D -l "
            + str(self.cmd_filename)
            + ")"
        )
        run_result = subprocess.run(exec_command, shell=True)
        return run_result

    def output(self):
        return {}

    def list_dependent_static_files(self):
        return [str(self.cmd_template_filename)]

    def list_dependent_state_variables(self, state):
        file_deps = preprocessor.get_dependencies_from_file(self.cmd_template_filename)
        file_deps = [x for x in file_deps if x in state]
        return file_deps
