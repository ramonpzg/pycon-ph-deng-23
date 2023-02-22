import pandas as pd
from pathlib import Path


def split_and_save_time_series_data(
    full_df, split_pct, data_path, train_file_name, test_file_name
):

    n_train = int(len(full_df) - len(full_df) * split_pct)

    if not data_path.exists():
        data_path.mkdir(parents=True)

    full_df[:n_train].reset_index(drop=True).to_parquet(
        data_path.joinpath(train_file_name), compression="snappy"
    )
    full_df[n_train:].reset_index(drop=True).to_parquet(
        data_path.joinpath(test_file_name), compression="snappy"
    )

    print("File Partitioned Successfully!")


if __name__ == "__main__":

    path = Path().cwd().joinpath("data", "02_part")
    split_pct = 0.30

    data = pd.read_parquet(path.joinpath("interim", "clean_data.parquet"))
    data_path = path.joinpath("processed")
    train_file_name, test_file_name = "train.parquet", "test.parquet"

    split_and_save_time_series_data(
        data, split_pct, data_path, train_file_name, test_file_name
    )
