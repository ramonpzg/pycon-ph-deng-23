from pathlib import Path
import ibis
import typer
from typing import Optional
import pandas as pd


def create_db(path_in, path_out, file_name, table_name):
    path = Path(path_out)
    conn = ibis.duckdb.connect(path.joinpath(file_name))
    conn.register(path_in, table_name=table_name)
    print(f"Successfully loaded the {table_name} table!")

def create_parquet(path_in, path_out, file_name):
    path = Path(path_out)
    pd.read_parquet(path_in).to_parquet(path.joinpath(file_name))
    print(f"Successfully loaded the {file_name} table!")

def save_data(data, path_out, file_name):
    path_out = Path(path_out)
    if not path_out.exists(): path_out.mkdir(parents=True)
    data.to_parquet(path_out.joinpath(file_name))


def main(
    kind: str = typer.Option(...),
    path_in: Optional[str] = typer.Option(None), 
    path_out: Optional[str] = typer.Option(None),
    file_name: Optional[str] = typer.Option(None),
    table_name: Optional[str] = typer.Option(None)
):
    if kind == "db":
        create_db(path_in, path_out, file_name, table_name)
    elif kind == "pq":
        create_parquet(path_in, path_out, file_name)
    else:
        print(f"Could not understand argument {kind}. Please use 'pq' for parquet files or 'db' for database.")

    print("Data Extracted Successfully!")

if __name__ == "__main__":
    typer.run(main)