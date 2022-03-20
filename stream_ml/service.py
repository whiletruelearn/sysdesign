from sql_utils import get_sensor_data
from simple_model import model_train
from fastapi import FastAPI

app = FastAPI()


@app.get("/api/sensor/")
def get_metric_agg(sensor_type: str, agg: str, start_time: str, end_time: str):
    if agg not in ["mean", "median", "max", "min"]:
        raise ValueError(f"Aggregation function :{agg} is not supported")

    df = get_sensor_data(sensor_type, start_time, end_time)
    return df.agg({"sensor_value": agg}).tolist()[0]


@app.get("/api/train/")
def train_model(sensor_type: str, start_time: str, end_time: str, percentile: int):
    df = get_sensor_data(sensor_type, start_time, end_time)
    model_train(df, percentile, sensor_type)
    return {"model_training": "success"}
