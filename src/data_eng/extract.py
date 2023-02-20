import urllib.request
from pathlib import Path
import whylogs as why
import pandas as pd
import json
import typer
from typing import Optional


def get_and_load_file(url, path_out, file_name):
    path = Path(path_out)
    if not path.exists():
        path.mkdir(parents=True)
    urllib.request.urlretrieve(url, path.joinpath(file_name))


def extract_from_csv(path_in, log_it=False):
    data =  pd.read_csv(path_in)
    if log_it:
        results = why(data)
        return data, results
    else:
        return data

def extract_from_db(path_in, query):
    conn = sqlite3.connect(path_in)
    data = pd.read_sql_query(query, conn)
    if log_it:
        results = why(data)
        return data, results
    else:
        return data

def extract_from_parquet(path_in, log_it=False, **kwargs):
    data =  pd.read_parquet(path_in, kwargs)
    if log_it:
        results = why(data)
        return data, results
    else:
        return data

def extract_from_json(path_in, log_it=False, **kwargs):
    try:
        data =  pd.read_json(path_in, kwargs)
    except:
        with open(path_in,'r') as f:
            data = json.loads(f.read())
            data = pd.json_normalize(data, kwargs)
    if log_it:
        results = why(data)
        return data, results
    else:
        return data

def save_data(data, path_out, file_name):
    path_out = Path(path_out)
    if not path_out.exists(): path_out.mkdir(parents=True)
    data.to_parquet(path_out.joinpath(file_name))


def main(
        arg: str = typer.Option(...),
        url: Optional[str] = typer.Option(None),
        path_in: Optional[str] = typer.Option(None),
        path_out: Optional[str] = typer.Option(None),
        file_name: Optional[str] = typer.Option(None),
        query: Optional[str] = typer.Option(None),
        log_it: Optional[bool] = typer.Option(False),
):
    if arg == "get":
        get_and_load_file(url, path_out, file_name)
    elif arg == "csv":
        data = extract_from_csv(path_in, log_it=False)
        save_data(data, path_out, file_name)
    elif arg == "pq":
        data = extract_from_parquet(path_in, log_it=False)
        save_data(data, path_out, file_name)
    elif arg == "json":
        data = extract_from_json(path_in, log_it=False)
        save_data(data, path_out, file_name)
    elif arg == "db":
        data = extract_from_db(path_in, query)
        save_data(data, path_out, file_name)
    else:
        print("Could not understand argument {arg}. Please use 'pq' for parquet files, 'db' for database, json, or csv in lowercase.")

    print("Data Extracted Successfully!")


if __name__ == "__main__":
    typer.run(main)

    # func = 
    # data_url = 
    # path = 
    # file_name = 
    # log_it = None

    # if func == "db":
    #     extract_from_db(path)
    
    # get_bikes_data(url, path, file_name)
    # print("File Downloaded Successfully!")


