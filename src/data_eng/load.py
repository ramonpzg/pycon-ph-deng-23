from pathlib import Path
import ibis
import typer
from typing import Optional
import pandas as pd


def create_db(path_in, path_out, name, table_name):
    path = Path(path_out)
    conn = ibis.duckdb.connect(path.joinpath(name))
    conn.register(path_in, table_name=table_name)
    print(f"Successfully loaded the {table_name} table!")

def create_parquet(path_in, path_out, name):
    path = Path(path_out)
    pd.read_parquet(path_in).to_parquet(path.joinpath(name))
    print(f"Successfully loaded the {path_in.split('/')[-1]} table!")

def main(
    kind: str = typer.Option(...),
    path_in: Optional[str] = typer.Option(None), 
    path_out: Optional[str] = typer.Option(None),
    name: Optional[str] = typer.Option(None),
    table_name: Optional[str] = typer.Option(None)
):
    if kind == "db":
        create_db(path_in, path_out, name, table_name)
    elif kind == "pq":
        create_parquet(path_in, path_out, name)
    else:
        print(f"Could not understand argument {kind}. Please use 'pq' for parquet files or 'db' for database.")

    print("Data Extracted Successfully!")

if __name__ == "__main__":
    typer.run(main)