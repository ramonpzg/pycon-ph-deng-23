import pandas as pd, re
from pathlib import Path
from extract import save_data
import typer
from typing import Optional, List

def clean_col_names(index_of_cols: pd.Index):
    return [re.sub(r'[^a-zA-Z0-9\s_]', '', col).lower().replace(r" ", "_") for col in index_of_cols]

def extract_dates(data, date_col, hour_col=None):

    data["date"] = pd.to_datetime(data[date_col], infer_datetime_format=True)
    if not data.columns.isin(["hour", "Hour", "hr", "HR"]).any():
        data["hour"] = data['date'].dt.hour

    #Time series datasets need to be ordered by time.
    data.sort_values(["date", hour_col], inplace=True)

    data["year"]           = data['date'].dt.year
    data["month"]          = data['date'].dt.month
    data["week"]           = data['date'].dt.isocalendar().week
    data["day"]            = data['date'].dt.day
    data["day_of_week"]    = data['date'].dt.dayofweek
    data["is_month_start"] = data['date'].dt.is_month_start    

    data.drop('date', axis=1, inplace=True)
    return data

def one_hot(data, cols_list):
    return pd.get_dummies(data=data, columns=cols_list)

def add_location_cols(data, place):
    data["loc_id"] = place
    return data

def fix_and_drop(data, col_to_fix, mapping, cols_to_drop):
    data[col_to_fix] = data[col_to_fix].map(mapping)
    return data.drop(cols_to_drop, axis=1)

def order_and_merge(data_lists):
    pick_order = data_lists[0].columns #takes order of columns of the first dataset
    new_list = [d.reindex(columns=pick_order).sort_values(['date', 'hour']) for d in data_lists] #reindexing columns by the order of the first dataset, then sorting by date and hour.
    return pd.concat(new_list) #merge all

def main(
    path_in:   str           = typer.Option(...),
    date_col:  Optional[str] = typer.Option(None),
    hour_col:  Optional[str] = typer.Option(None),
    cols_list: List[str]     = typer.Option(None),
    place:     Optional[str] = typer.Option(None),
    path_out:  Optional[str] = typer.Option(None),
    file_name: Optional[str] = typer.Option(None)
):
    data = pd.read_parquet(path_in)
    # data.columns = clean_col_names(data.columns)
    data = extract_dates(data, date_col, hour_col)
    if cols_list:
        data = one_hot(data, cols_list)
    data.columns = clean_col_names(data.columns)
    data = add_location_cols(data, place)
    save_data(data, path_out, file_name)

    print("Data Extracted Successfully!")

if __name__ == "__main__":
    typer.run(main)