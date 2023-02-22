from pathlib import Path
import pandas as pd
from flytekit import task, workflow
import re


#################################
#                               #
#         EXTRACT               #
#                               #
#################################

@task
def extract_from_csv(path_in: str, encoding: str) -> pd.DataFrame:
    data =  pd.read_csv(path_in, encoding=encoding)
    return data


#################################
#                               #
#         TRANSFORM             #
#                               #
#################################


@task
def clean_col_names(data: pd.DataFrame) -> pd.DataFrame:
    data.columns = [re.sub(r'[^a-zA-Z0-9\s_]', '', col).lower().replace(r" ", "_") for col in data.columns]
    return data


@task
def extract_dates(data: pd.DataFrame, date_col: str, hour_col: str) -> pd.DataFrame:

    data["date"] = pd.to_datetime(data[date_col], infer_datetime_format=True)
    data.sort_values(["date", hour_col], inplace=True)
    
    data["year"]           = data['date'].dt.year
    data["month"]          = data['date'].dt.month
    data["week"]           = data['date'].dt.isocalendar().week
    data["day"]            = data['date'].dt.day
    data["day_of_week"]    = data['date'].dt.dayofweek
    data["is_month_start"] = data['date'].dt.is_month_start    

    data.drop('date', axis=1, inplace=True)
    return data


@task
def add_location_cols(data: pd.DataFrame, place: str) -> pd.DataFrame:
    data["loc_id"] = place
    return data

#################################
#                               #
#         LOAD                  #
#                               #
#################################


@task
def save_data(data: pd.DataFrame, path_out: str, file_name: str) -> None:
    path_out = Path(path_out)
    if not path_out.exists(): path_out.mkdir(parents=True)
    data.to_parquet(path_out.joinpath(file_name))
    print("Data Saved Successfully!")


@workflow
def main_flow(
    path_in: str,
    path_out: str,
    encoding: str,
    file_name: str,
    date_col: str,
    place: str,
    hour_col: str,
) -> None:
    # EXTRACT
    data = extract_from_csv(path_in=path_in, encoding=encoding)

    # TRANSFORM
    data = extract_dates(data=data, date_col=date_col, hour_col=hour_col)
    data = clean_col_names(data=data)
    data = add_location_cols(data=data, place=place)

    # SAVE
    save_data(data=data, path_out=path_out, file_name=file_name)


if __name__ == "__main__":
    main_flow(
        path_in="https://archive.ics.uci.edu/ml/machine-learning-databases/00560/SeoulBikeData.csv",
        path_out="../../data/tmp",
        encoding="iso-8859-1",
        file_name="seoul_raw.parquet",
        date_col="Date",
        place="Seoul",
        hour_col = "Hour",
    )
