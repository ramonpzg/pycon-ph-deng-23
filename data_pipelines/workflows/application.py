"""A simple Flyte example."""

import typing as t
from flytekit import task, workflow
import pandas as pd
from pathlib import Path

app_data = Path(__file__).parent/"data/raw/application_train.csv"

# @task
def get_data(app_data: t.Union[Path, str]) -> pd.DataFrame:
    return pd.read_csv(
      app_data,
      na_values=["XNA", 365243]
    )

# @task
def get_features(df: pd.DataFrame, feat_to_exclude: str = None, y_is_cat: bool = None) -> t.Tuple[list, list]:

    if y_is_cat:
        cat_features = [col for col in df.columns if df[col].dtypes == 'object' and col != feat_to_exclude]
        num_features = [col for col in df.columns if df[col].dtypes != 'object']
    elif y_is_cat == False:
        cat_features = [col for col in df.columns if df[col].dtypes == 'object']
        num_features = [col for col in df.columns if df[col].dtypes != 'object' and col != feat_to_exclude]
    else:
        cat_features = [col for col in df.columns if df[col].dtypes == 'object']
        num_features = [col for col in df.columns if df[col].dtypes != 'object']

    return cat_features, num_features



# @task
def clean_features(
  df: pd.DataFrame, cat_vars: t.Iterable = None, num_vars: t.Iterable = None
) -> pd.DataFrame:
    if cat_vars:
        df[cat_vars].fillna("Unknown", axis=0, inplace=True)
    elif num_vars:
        df[num_vars].fillna(0, axis=0, inplace=True)
    return df

# @task
def load_table(df: pd.DataFrame, path: str):
    df.to_parquet(path)

# @workflow
def wf(name: str) -> t.Tuple[str, int]:
    """Declare workflow called `wf`.

    The @workflow decorator defines an execution graph that is composed of tasks
    and potentially sub-workflows. In this simple example, the workflow is
    composed of just one task.

    There are a few important things to note about workflows:
    - Workflows are a domain-specific language (DSL) for creating execution
      graphs and therefore only support a subset of Python's behavior.
    - Tasks must be invoked with keyword arguments
    - The output variables of tasks are Promises, which are placeholders for
      values that are yet to be materialized, not the actual values.
    """
    greeting = say_hello(name=name)
    greeting_len = greeting_length(greeting=greeting)
    return greeting, greeting_len


if __name__ == "__main__":
    
    df = get_data(app_data)
    cat_features, num_features = get_features(df, "TARGET", False)
