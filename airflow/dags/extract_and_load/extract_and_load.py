import sys
sys.path.append("/data")
from loader import Loader
from postgres_operations import create_tables, insert_data


def extract_load(file_path=None) -> None:
    file_path = file_path
    loader = Loader()
    vehicle_df, trajectories_df = loader.get_dfs(file_path=file_path)
    create_tables()
    insert_data(trajectories_df, "trajectories")
    insert_data(vehicle_df, "vehicles")
    


extract_load("20181024_d1_0830_0900.csv")