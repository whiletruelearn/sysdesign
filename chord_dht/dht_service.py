from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
import os
import sys
import pickledb
import requests


app = FastAPI()


class NodeBase:
    def __init__(self, node_id):
        self.node_id = node_id
        self.db = pickledb.load(f"{self.node_id}/{self.node_id}.db", False)

    def insert_data(self, key, value):
        self.db.set(key, value)
        self.db.dump()
        print(
            f"Inserted key->{key} value->{value} to node {self.node_id} stored at  {self.node_id}/{self.node_id}.db")
        return "OK"

    def get_data(self, key):
        val = self.db.get(key)
        if val is None:
            val = "Not Available"
        print(
            f"Returning value {val} for key {key} ")
        return val


@app.get("/get_keys")
def get_keys():
    return {"keys": list(n.db.getall())}


class Item(BaseModel):
    key: str
    val: str


@app.post("/client_insert_data")
def client_insert(item:  Item):
    resp = requests.post("http://0.0.0.0:9999/item_node_id/",
                         json={"item_key": item.key}).json()
    node, succ = resp["node"], resp["succ"]
    succ_port = succ.split("_")[1]
    if node == n.node_id:
        n.insert_data(item.key, item.val)
        requests.post(
            f"http://0.0.0.0:{succ_port}/insert_data/", json={"key": item.key, "val": item.val})
        return {"key": item.key, "val": item.val, "primary_node": n.node_id, "replica_node": succ}
    else:
        node_port = node.split("_")[1]
        requests.post(
            f"http://0.0.0.0:{node_port}/insert_data/", json={"key": item.key, "val": item.val})
        requests.post(
            f"http://0.0.0.0:{succ_port}/insert_data/", json={"key": item.key, "val": item.val})
        return {"key": item.key, "val": item.val, "primary_node": node, "replica_node": succ}


class KeyItem(BaseModel):
    key: str


@app.post("/delete_key/")
def delete_key(item: KeyItem):
    n.db.rem(item.key)
    print(f"Removed key {item.key} from node {n.node_id}")


@app.post("/client_get_data")
def client_get_data(item:  KeyItem):
    resp = requests.post("http://0.0.0.0:9999/item_node_id/",
                         json={"item_key": item.key}).json()
    node, succ = resp["node"], resp["succ"]
    succ_port = succ.split("_")[1]
    origin_node = None
    if node == n.node_id:
        origin_node = n.node_id
        val = n.get_data(item.key)
        if val == "Not Available":
            origin_node = succ

            val = requests.post(
                f"http://0.0.0.0:{succ_port}/get_data/", json={"key": item.key}).json()["value"]
            print(
                f"Key {item.key} not available in node {n.node_id} and found at {succ}")
    else:
        node_port = node.split("_")[1]
        origin_node = node
        val = requests.post(
            f"http://0.0.0.0:{node_port}/get_data/", json={"key": item.key}).json()["value"]
        if val == "Not Available":
            origin_node = succ
            val = requests.post(
                f"http://0.0.0.0:{succ_port}/insert_data/", json={"key": item.key}).json()["value"]
            print(
                f"Key {item.key} not available in node {node} and found at {succ}")
    return {"value": val, "key_origin_node" : origin_node }


@app.post("/insert_data/")
def insert_data(item: Item):
    status = n.insert_data(item.key, item.val)
    return {"insertion": status}


@app.post("/get_data/")
def insert_data(keyitem: KeyItem):
    val = n.get_data(keyitem.key)
    return {"value": val}


@app.get("/healthcheck/")
def healthcheck():
    return 'Health - OK'


class Node(BaseModel):
    node_id: str


@app.post("/shutdown/")
def shutdown(node: Node):
    consistent_hasing_service = "http://0.0.0.0:9999/"
    resp = requests.post(consistent_hasing_service +
                         "remove_node/", json={"node_id": node.node_id})
    return resp.json()


if __name__ == "__main__":
    node_id, port = sys.argv[1:]
    os.mkdir(node_id)
    consistent_hasing_service = "http://0.0.0.0:9999/"
    resp = requests.post(consistent_hasing_service +
                         "add_node/", json={"node_id": node_id})
    print(resp.json())
    n = NodeBase(node_id)
    uvicorn.run(app, host="0.0.0.0", port=int(port))
