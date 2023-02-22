import pandas as pd, numpy as np, pickle, json
from pprint import pprint
import sklearn.metrics as metrics
from pathlib import Path


def get_model(model_path):
    with open(model_path, "rb") as fd:
        return pickle.load(fd)


def get_predictions(model, X_test_data):
    return model.predict(X_test_data.values)

def get_metrics(predictions, X_test_data, y_test_data, metrics_path):
    
    if not metrics_path.parent.exists(): metrics_path.parent.mkdir(parents=True)
    
    mae = metrics.mean_absolute_error(y_test_data.values, predictions)
    rmse = np.sqrt(metrics.mean_squared_error(y_test_data.values, predictions))
    r2_score = model.score(X_test_data.values, y_test_data.values)
    
    with open(metrics_path, "w") as fd:
        json.dump({"MAE": mae, "RMSE": rmse, "R^2": r2_score}, fd, indent=4)
        
    return mae, rmse, r2_score


if __name__ == "__main__":
    
    path = Path().cwd()
    model_path = path.joinpath('models', 'rf_model.pkl')
    metrics_path = path.joinpath("reports", 'metrics.json')
    test_path = path.joinpath("data", "02_part", 'processed', 'test.parquet')
    
    X_test = pd.read_parquet(test_path)
    y_test = X_test.pop('rented_bike_count')
    
    model = get_model(model_path)
    predictions = get_predictions(model, X_test)
    
    mae, rmse, r2_score = get_metrics(predictions, X_test, y_test, metrics_path)
    print(f"File Evaluated Successfully!\nMAE: {round(mae, 4)}\nRMSE: {round(rmse, 4)}\nR^2: {round(r2_score, 4)}")