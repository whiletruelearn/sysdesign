import numpy as np
import mlflow
import pickle


class SimpleUpperTailADModel:

    def __init__(self, percentile):
        self.mean = None
        self.std = None
        self.percentile = percentile

    def fit(self, X):
        self.mean = X.mean()
        self.std = X.std()

    def predict(self, X):
        anomalies = np.zeroes_like(X)
        scores = (X - self.mean) / self.std
        p = np.percentile(scores)
        mask = scores > p
        anomalies[mask] = 1.0
        return anomalies


def model_train(df, percentile, sensor_type):
    mlflow.set_tracking_uri("http://127.0.0.1:5000")
    mlflow.set_experiment("test_experiment")

    df.to_csv("training_input.csv")

    with mlflow.start_run():
        mlflow.log_param("percentile", percentile)
        mlflow.log_param("sensor_type", sensor_type)
        mlflow.log_artifact("training_input.csv")
        model = SimpleUpperTailADModel(percentile=percentile)
        model.fit(df["sensor_value"])
        model_name = 'model.pkl'
        pickle.dump(model, open(model_name, 'wb'))
        mlflow.log_artifact("model.pkl")
