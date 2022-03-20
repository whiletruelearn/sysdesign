from datetime import datetime
import requests
import pandas as pd
import urllib
import io
from dataclasses import dataclass


@dataclass
class Sensor:
    """
    Dataclass for Sensor
    """
    type: str
    value: float
    arrival_time: datetime


def create_table():
    """
    Create table for storing sensor data
    """
    CREATE_TABLE_QUERY = "CREATE TABLE IF NOT EXISTS metrics (sensor_type string, sensor_value float, curr_time TIMESTAMP) timestamp(curr_time);"
    r = requests.get("http://localhost:9000/exec?query=" + CREATE_TABLE_QUERY)
    return r.status_code


def create_job_table():
    """
    A table for knowing about the job status of model training
    """
    CREATE_TABLE_QUERY = "CREATE TABLE IF NOT EXISTS job_status (job_id string, status string, mlflow_experiment string, job_creation_time timestamp, model_type string) timestamp(job_creation_time);"
    r = requests.get("http://localhost:9000/exec?query=" + CREATE_TABLE_QUERY)
    return r.status_code


def insert_job_details(job_id, job_status, mlflow_experiment, job_creation_time, model_type):
    JOB_STATUS_QUERY = f" insert into job_status values('{job_id}','{job_status}', '{mlflow_experiment}', '{job_creation_time}', '{model_type}'"
    r = requests.get("http://localhost:9000/exec?query=" + JOB_STATUS_QUERY)
    if r.status_code == 200:
        return True
    return False


def get_job_details(job_id):
    """
    Get status of job_id
    """
    JOB_STATUS_QUERY = f"SELECT * from 'job_status' where 'job_id' = {job_id} latest on job_creation_time partition by job_id"
    query = urllib.parse.quote(JOB_STATUS_QUERY)
    r = requests.get("http://localhost:9000/exp?query="+query)
    queryData = r.content
    df = pd.read_csv(io.StringIO(queryData.decode('utf-8')))
    return df


def insert_sensor_data(sensor: Sensor):
    """
    Insert the sensor data to the metrics table    
    """
    curr_time = sensor.arrival_time
    sensor_type = sensor.type
    sensor_value = sensor.value
    INSERT_TABLE_QUERY = "insert into metrics values(" \
                         + "'" + sensor_type + "'" + "," \
                         + str(sensor_value) + "," + \
        "'" + curr_time + "'" + ")"

    r = requests.get("http://localhost:9000/exec?query=" + INSERT_TABLE_QUERY)
    if r.status_code == 200:
        return True
    return False


def get_sensor_data(sensor_type, start_time, end_time):
    """
    Read the questdb and return data for that sensor_type
    between start_time and end_time
    """
    GET_DATA_QUERY = f"select sensor_type,sensor_value from 'metrics' where sensor_type = '{sensor_type}' and curr_time between '{start_time}' and '{end_time}'"
    query = urllib.parse.quote(GET_DATA_QUERY)
    r = requests.get("http://localhost:9000/exp?query="+query)
    queryData = r.content
    df = pd.read_csv(io.StringIO(queryData.decode('utf-8')))
    return df
