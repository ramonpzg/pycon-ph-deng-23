import urllib.request
from pathlib import Path
import pandas as pd
import json
import typer
from typing import Optional
from load import save_data


def get_and_load_file(url, path_out, file_name):
    path = Path(path_out)
    if not path.exists():
        path.mkdir(parents=True)
    urllib.request.urlretrieve(url, path.joinpath(file_name))

def extract_from_csv(path_in, encoding=None):
    data =  pd.read_csv(path_in, encoding=encoding)
    return data

def extract_from_db(path_in, query):
    conn = sqlite3.connect(path_in)
    return pd.read_sql_query(query, conn)

def extract_from_parquet(path_in, **kwargs):
    data =  pd.read_parquet(path_in, kwargs)
    return data

def extract_from_json(path_in, **kwargs):
    try:
        data =  pd.read_json(path_in, kwargs)
    except:
        with open(path_in,'r') as f:
            data = json.loads(f.read())
            data = pd.json_normalize(data, kwargs)
    return data

def main(
    arg: str = typer.Option(...),
    url: Optional[str] = typer.Option(None),
    path_in: Optional[str] = typer.Option(None),
    path_out: Optional[str] = typer.Option(None),
    encoding: Optional[str] = typer.Option(None),
    file_name: Optional[str] = typer.Option(None),
    query: Optional[str] = typer.Option(None),
):
    if arg == "get":
        get_and_load_file(url, path_out, file_name)
    elif arg == "csv":
        data = extract_from_csv(path_in, encoding=encoding)
        save_data(data, path_out, file_name)
    elif arg == "pq":
        data = extract_from_parquet(path_in)
        save_data(data, path_out, file_name)
    elif arg == "json":
        data = extract_from_json(path_in)
        save_data(data, path_out, file_name)
    elif arg == "db":
        data = extract_from_db(path_in, query)
        save_data(data, path_out, file_name)
    else:
        print(f"Could not understand argument {arg}. Please use 'pq' for parquet files, 'db' for database, json, or csv in lowercase.")

    print("Data Extracted Successfully!")


if __name__ == "__main__":
    typer.run(main)
