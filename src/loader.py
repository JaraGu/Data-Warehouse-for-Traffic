import pandas as pd


class Loader:
    def __init__(self, file_path=None) -> None:
        self.filepath = file_path

    def get_uid(self, filename, row_num):
        return f"{filename}_{row_num}"

    def file_loader(self, file_path: str) -> list:
        """Load a file from the given path and return a list of its lines."""
        with open(file_path, 'r') as f:
            # Skips the header and strips newline characters
            lines = [line.strip('\n') for line in f.readlines()[1:]]
        return lines

    def parse(self, lines: list, filename: str) -> tuple:
        """Parse the lines into 5 columns and return pandas DataFrames."""
        veh_info = {"unique_id": [], "track_id": [],
                    "veh_type": [], "traveled_distance": [], "avg_speed": []}
        trajectories = {"unique_id": [], "lat": [], "lon": [],
                        "speed": [], "lon_acc": [], "lat_acc": [], "time": []}

        for row_num, line in enumerate(lines):
            uid = self.get_uid(filename, row_num)
            line = line.split("; ")[:-1]
            assert len(line[4:]) % 6 == 0, f"{line}"

            veh_info["unique_id"].append(uid)
            veh_info["track_id"].append(int(line[0]))
            veh_info["veh_type"].append(line[1])
            veh_info["traveled_distance"].append(float(line[2]))
            veh_info["avg_speed"].append(float(line[3]))

            for i in range(0, len(line[4:]), 6):
                trajectories["unique_id"].append(uid)
                trajectories["lat"].append(float(line[4+i+0]))
                trajectories["lon"].append(float(line[4+i+1]))
                trajectories["speed"].append(float(line[4+i+2]))
                trajectories["lon_acc"].append(float(line[4+i+3]))
                trajectories["lat_acc"].append(float(line[4+i+4]))
                trajectories["time"].append(float(line[4+i+5]))

        vehicle_df = pd.DataFrame(veh_info).reset_index(drop=True)
        trajectories_df = pd.DataFrame(trajectories).reset_index(drop=True)
        return vehicle_df, trajectories_df

    def get_dfs(self, file_path: str = None) -> tuple:
        """Load the file and parse its content into two pandas DataFrame objects."""
        if not file_path and self.filepath:
            file_path = self.filepath

        lines = self.file_loader(file_path)
        filename = file_path.split("/")[-1].strip(".csv")
        vehicle_df, trajectories_df = self.parse(lines, filename)

        return vehicle_df, trajectories_df
