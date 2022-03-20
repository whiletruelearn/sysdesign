
import hashlib
import uvicorn
from bisect import bisect, bisect_left, bisect_right
from fastapi import FastAPI
import requests
from pydantic import BaseModel
import os
import logging

logger = logging.getLogger('foo-logger')



app = FastAPI()


def hash_fn(key, max_slots):
    result = hashlib.md5(key.encode())
    return int(result.hexdigest(), 16) % max_slots


class ConsitentHashing:
    def __init__(self, max_slots):
        self.hash_keys = []
        self.nodes = []
        self.max_slots = max_slots

    def add_node(self, node_id):
        hash_key = hash_fn(node_id, self.max_slots)
        idx = bisect(self.hash_keys, hash_key)

        self.nodes.insert(idx, node_id)
        self.hash_keys.insert(idx, hash_key)

    def remove_node(self, node_id):
        hash_key = hash_fn(node_id, self.max_slots)
        idx = bisect_left(self.hash_keys, hash_key)
        self.hash_keys.pop(idx)
        self.nodes.pop(idx)

    def get_node(self, key):
        hash_key = hash_fn(key, self.max_slots)
        idx = bisect_right(self.hash_keys, hash_key) % len(self.nodes)
        return idx


@app.get("/healthcheck/")
def healthcheck():
    return 'Health - OK'


class Node(BaseModel):
    node_id: str


@app.post("/add_node/")
async def add_node(node: Node):
    ch.add_node(node.node_id)
    return {"created_node": node.node_id}




@app.post("/remove_node/")
async def remove_nodes(node: Node):
    ch.remove_node(node.node_id)
    os.remove(f"{node.node_id}/{node.node_id}.db")
    os.rmdir(node.node_id)
    return {"removed_node": node.node_id}


class Item(BaseModel):
    item_key: str


@app.post("/item_node_id/")
def get_node_succesor_for_item(item: Item):
    idx = ch.get_node(item.item_key)
    node_id = ch.nodes[idx]
    if idx == 0:
        succ = ch.nodes[1]
        pred = ch.nodes[-1]
    elif idx == len(ch.nodes) - 1:
        succ = ch.nodes[0]
        pred = ch.nodes[idx - 1]

    else:
        succ = ch.nodes[idx + 1]
        pred = ch.nodes[idx - 1]

    return {"node": node_id, "succ": succ, "pred": pred}



@app.post("/add_new_node_with_redistribution/")
async def add_node_with_redistribution(node: Node):
    logger.info('Inside redistribution')
    curr_node_idx = ch.nodes.index(node.node_id)
    pred_node, succ_node = ch.nodes[curr_node_idx-1], ch.nodes[curr_node_idx+1]
    logger.info(f"Current hash ring {ch.nodes}")
    logger.info(f"Pred {pred_node} succ_node {succ_node}")
    pred_node_port = pred_node.split("_")[1]
    succ_node_port = succ_node.split("_")[1]
    node_port = node.node_id.split("_")[1]

    pred_keys = requests.get(
        f"http://0.0.0.0:{pred_node_port}/get_keys/").json()["keys"]

    logger.info(f"pred keys {pred_keys}")
    succ_keys = requests.get(
        f"http://0.0.0.0:{succ_node_port}/get_keys/").json()["keys"]

    logger.info(f"succ keys {succ_keys}")
    for key in pred_keys:
        logger.info(f"Processing key {key}")
        item = Item(item_key = key)
        expected_node = get_node_succesor_for_item(item)["node"]
        if expected_node != pred_node:
            val = requests.post(
                f"http://0.0.0.0:{pred_node_port}/get_data/", json={"key": key}).json()["value"]
            requests.post(
                f"http://0.0.0.0:{pred_node_port}/delete_key/", json={"key": key})
            requests.post(f"http://0.0.0.0:{node_port}/insert_data/", json={"key": key, "val": val})
            
    for key in succ_keys:
        item = Item(item_key = key)
        expected_node = get_node_succesor_for_item(item)["node"]
        if expected_node != succ_node:
            val = requests.post(
                f"http://0.0.0.0:{succ_node_port}/get_data/", json={"key": key}).json()["value"]
            requests.post(
                f"http://0.0.0.0:{succ_node_port}/delete_key/", json={"key": key})
            requests.post(
                f"http://0.0.0.0:{node_port}/insert_data/", json={"key": key, "val": val})
            
    return {"created_node": node.node_id}

@app.get("/slots/")
def get_slots_details():
    return {"nodes": ch.nodes, "slots": ch.hash_keys, "max_slot": ch.max_slots}


if __name__ == "__main__":
    ch = ConsitentHashing(max_slots=32)
    uvicorn.run(app, host="0.0.0.0", port=9999)
