import sys
import zmq
from sql_utils import Sensor, insert_sensor_data
from datetime import datetime
port = "5556"

context = zmq.Context()
socket = context.socket(zmq.SUB)

socket.connect("tcp://localhost:%s" % port)
socket.setsockopt_string(zmq.SUBSCRIBE, "sensor")


def get_sensor_data():
    while True:
        string = socket.recv().decode("utf-8")
        _, sensor_type, value = string.split(",")
        s = Sensor(type=sensor_type, value=value,
                   arrival_time=datetime.now().isoformat())
        insert_sensor_data(s)


if __name__ == "__main__":
    get_sensor_data()
