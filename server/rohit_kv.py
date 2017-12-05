import functions as fn
from globals import *

# KVS Nested Dictionary
store = {}


def get(self, key):
    try:
        return store[key]
    except KeyError:
        return fn.local_error("Key not found", 403)


def put(key, value="", causal_payload=[0, 0, 0, 0]):

    val = {
        "value": value,
        "causal_payload": causal_payload,
        "timestamp": int(time.time())
    }

    store[key] = value
    status_code = 200 if fn.is_replica(META.ID) else 201
    return fn.local_success("", status_code)


def get_payload(key):
    try:
        return store[key].causal_payload
    except KeyError:
        return None
