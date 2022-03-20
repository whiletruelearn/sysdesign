import random
import time
import zmq

SENSORS = {"thermostat": [30, 35],
           "heart_rate": [72, 76], "car_fuel": [10, 12]}


def simulate_sensors(socket):
    """
    Insert data to the topic
    """
    while True:
        for sensor in SENSORS:
            start, end = SENSORS[sensor]
            value = random.randrange(start, end) * random.random()
            print(f"Sending Value : {value} to sensor : {sensor}")
            socket.send_string(f"sensor,{sensor},{value}")
        time.sleep(1)


if __name__ == "__main__":
    port = "5556"
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.bind("tcp://*:%s" % port)
    simulate_sensors(socket)
