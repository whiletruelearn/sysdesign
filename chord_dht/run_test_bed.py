import pandas as pd
import requests
import random
import time
nodes = ["node_3000","node_3001","node_3002","node_3003","node_3004",
"node_3005","node_3006","node_3007","node_3008","node_3009"]
test_bed_df = pd.read_csv("test_bed.csv")
for idx, row in test_bed_df.iterrows():
    key = row["key"]
    val = row["value"]
    node = random.choice(nodes)
    node_port = node.split("_")[1]
    print(f"Going to Insert #{idx} key: {key} value: {val} via node: {node}")
    resp = requests.post(f"http://0.0.0.0:{node_port}/client_insert_data",json={"key":key,"val" : val}).json()
    primary_node = resp["primary_node"]
    replica_node = resp["replica_node"]
    print(f"Inserted key: {key} value: {val} in primary node : {primary_node} replica_node : {replica_node} via node : {node}")
    