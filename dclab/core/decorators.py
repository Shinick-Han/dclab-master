import functools
import pathlib
from os import listdir
from os.path import isfile, join, getmtime


def record_output(func):
    @functools.wraps(func)
    def wrapper_decorator(self, *args, **kwargs):

        pre_files = [f for f in listdir(self.sim_dir) if isfile(join(self.sim_dir, f))]
        pre_last_modified = [
            getmtime(str(pathlib.Path(self.sim_dir) / f)) for f in pre_files
        ]
        pre = dict(zip(pre_files, pre_last_modified))
        value = func(self, *args, **kwargs)
        post_files = [f for f in listdir(self.sim_dir) if isfile(join(self.sim_dir, f))]
        post_last_modified = [
            getmtime(str(pathlib.Path(self.sim_dir) / f)) for f in post_files
        ]
        post = dict(zip(post_files, post_last_modified))
        new_files = list(set(post_files) - set(pre_files))
        modified_files = [
            f for f in post_files if (f in pre) and not (pre[f] == post[f])
        ]
        new_files += modified_files
        self.output_files = [str(pathlib.Path(self.sim_dir) / f) for f in new_files]
        return value

    return wrapper_decorator
