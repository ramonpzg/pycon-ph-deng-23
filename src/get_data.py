import urllib.request
from pathlib import Path
import whylogs as why
import pandas as pd
import json



def get_and_load_file(url, path, file_name):

    if not path.exists():
        path.mkdir(parents=True)
    urllib.request.urlretrieve(url, path.joinpath(file_name))


def extract_from_csv(path, log_it=False, **kwargs):
    data =  pd.read_csv(path, kwargs)
    if log_it:
        results = why(data)
        return data, results
    else:
        return data

def extract_from_db(path, query):
    conn = sqlite3.connect(path)
    data = pd.read_sql_query(query, conn)
    if log_it:
        results = why(data)
        return data, results
    else:
        return data

def extract_from_parquet(path, log_it=False, **kwargs):
    data =  pd.read_parquet(path, kwargs)
    if log_it:
        results = why(data)
        return data, results
    else:
        return data

def extract_from_json(path, log_it=False, **kwargs):
    try:
        data =  pd.read_json(path, kwargs)
    except:
        with open(path,'r') as f:
            data = json.loads(f.read())
            data = pd.json_normalize(data, kwargs)
    if log_it:
        results = why(data)
        return data, results
    else:
        return data

def save_data(data: pd.DataFrame, data_path: Path, file_name: str) -> None:
    if not data_path.exists(): data_path.mkdir(parents=True)
    data.to_parquet(data_path.joinpath(file_name), compression="snappy")

if __name__ == "__main__":

    func = 
    data_url = 
    path = 
    file_name = 
    log_it = None

    if func == "db":
        extract_from_db(path)
    
    get_bikes_data(url, path, file_name)
    print("File Downloaded Successfully!")


