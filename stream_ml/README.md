
# Components 

- pub sub model for streaming data implemented via ZeroMQ
- Timeseries database used is QuestDB
- Webservice is built using FastAPI
- For model tracking, hyper parameter versioning, data versioning etc mlflow is used. 

# Setting up 

- Install questdb

```brew install questdb` 

- Install zeromq

```brew install zmq```

- Install other python dependencies 

```pip install requirements.txt```


## Run the simulator

``` python simulator.py```

## Run the Consumer 

``` python consumer.py```

## Start questDB 

``` questdb start ```

# Run the service 

```uvicorn service:app ```

Go to http://127.0.0.1:8000/docs to try out the service.

# Start MLFLOW

mlflow server --backend-store-uri sqlite:///mydb.sqlite --default-artifact-root /Users/whiletruelearn/projects/stream_ml/model_artifacts

http://127.0.0.1:5000