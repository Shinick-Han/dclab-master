import pathlib
import pandas as pd


def is_numeric(s):
    try:
        float(s)
        return True
    except (ValueError, TypeError):
        return False


class SimulationMemory(object):
    def __init__(self):
        self.data_frame = pd.DataFrame()

    def load_defaults(self, filename):
        if isinstance(filename, str) or isinstance(filename, pathlib.Path):
            self.data_frame = pd.read_csv(filename)
        elif isinstance(filename, dict):
            self.data_frame = pd.DataFrame(data=filename, index=[0])
            self.data_frame.reset_index()

    def load_doe(self, filename):
        new_frame = pd.read_csv(filename, index_col=False)
        if self.data_frame.size == 0:
            self.data_frame = new_frame
        else:
            self.data_frame = SimulationMemory.merge_frames(self.data_frame, new_frame)

    @staticmethod
    def merge_frames(default_frame, doe_frame):
        # Step 0: retrieve rows for current and new test frames
        new_nrows = doe_frame.shape[0]
        current_nrows = default_frame.shape[0]
        assert current_nrows == 1
        # Step 1: remove defaults rows to be overridden
        default_frame = default_frame.drop(columns=doe_frame.columns)
        # Step 2: extend data frame to correct length
        if new_nrows > 1:
            default_frame = default_frame.append([default_frame] * (new_nrows - 1))
        default_frame = default_frame.set_index(keys=doe_frame.index)
        # Step 3: join new frame with existing frame
        default_frame = doe_frame.join(default_frame, how="inner")
        return default_frame

    def export_to_csv(self, filename):

        self.data_frame.to_csv(path_or_buf=filename, index_label="ID")

    def add_data_at_index(self, name, data, index=0):

        # Step 1: check if column already exists and if not create it
        if not name in self.data_frame.columns:
            self.data_frame[name] = pd.Series(index=self.data_frame.index, dtype=float)
            if not is_numeric(data):
                self.data_frame[name] = self.data_frame[name].astype(object)
            else:
                self.data_frame[name] = self.data_frame[name].astype(float)
        # Step 2: add data to frame
        self.data_frame.at[index, name] = data
        return

    def add_data_to_all_rows(self, name, data):
        for i in self.data_frame.index:
            self.add_data_at_index(name=name, data=data, index=i)

    def get_column(self, name, to_float=True):
        if name in self.data_frame.columns:
            return self.data_frame[name]

    def get_value(self, name, index=0):

        if name in self.data_frame.columns:
            return self.data_frame.at[index, name]

    def get_row(self, index=0):
        return self.data_frame.iloc[index]

    def size(self):
        return len(self.data_frame.index)
