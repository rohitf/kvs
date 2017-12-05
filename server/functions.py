#     VECTOR CLOCK FUNCTIONS   #
#     Team Valetta             #
#     November 21st 2017       #

import re
import sys
import hashlib
import math
from globals import *

# General functions
# Returns the URL of a random replica


def random_replica():
    replicas = get_replicas()
    return replicas[0].ip_port


def count_nodes():
    return len(VIEW)


def get_id(ip_port):
    return ip_port[8:-5]


def get_replicas():
    return {node: val for (node, val) in VIEW.items() if node.type == REPLICA}


def get_proxies():
    return {node: val for (node, val) in VIEW.items() if node.type != REPLICA}


def is_replica(node_id):
    return VIEW[node_id]["type"] == REPLICA


def add_node(node_id, node_type, node_ip_port):
    METADATA.VIEW[node_id] = {
        "ip_port": node_ip_port,
        "type": node_type
    }


def generate_response(payload, status_code):
    resp = ({json.dumps(payload), status_code, {
            'Content-Type': 'application/json'}})
    return resp


def local_error(message="", status_code=403):
    resp = {
        "result": "ERROR",
        "msg": message,
        "status_code": status_code
    }
    return resp


def local_success(message="", status_code=200):
    resp = {
        "result": "SUCCESS",
        "msg": message,
        "status_code": status_code
    }
    return resp


def http_error(message, status_code):
    payload = {
        "result": "error",
        "msg": message
    }
    resp = ({json.dumps(payload), status_code, {
            'Content-Type': 'application/json'}})
    return resp


def compareVC(VC1, VC2):
    diffLen = abs(len(VC1) - len(VC2))
    placeholders = [0] * diffLen
    VC2.extend(placeholders) if len(VC1) > len(
        VC2) else VC1.extend(placeholders)

    if equalityVC(VC1, VC2):
        return True # Equal

    clockDiffs = [a - b for a, b in zip(VC1, VC2)]
    sameSign = not (min(clockDiffs) < 0 < max(clockDiffs))
    if not sameSign:
        return None  # Concurrent

    return True if clockDiffs[0] >= 0 else False

# Check if two vector clocks are equal


def equalityVC(VC1, VC2):
    if len(VC1) < len(VC2):
        diff = len(VC1) - len(VC2)
        VC1.extend([0] * diff)
    elif len(VC2) < len(VC1):
        diff = len(VC2) - len(VC1)
        VC2.extend([0] * diff)
    for x in range(0, len(VC1)):
        if not(VC1[x] == VC2[x]):
            return False
    return True

# To convert Strings into Vector Clocks from JSON Objects for saving


def parseVC(VC_string):
    vc_s = VC_string.split(".")
    vc = []
    for i in range(0, len(vc_s)):
        vc.append(int(vc_s[i]))
    return vc

# To convert Vector Clocks into Strings for JSON objects
# EX [0 , 1 , 2 , 3] => "0.1.2.3"


def deparseVC(VC):
    m = str(VC[0])
    for x in VC[1:]:
        m = m + "." + str(x)
    return m

# Checks if key is valid for putting


def keyCheck(key, value):
    if not re.match('^[0-9a-zA-Z_]*$', key) or len(key) > 200 or len(key) == 0:
        return False, {"result": "error", "msg": "Key not valid"}, 403
    elif value is None:
        return False, {"result": "error", "msg": "No value provided"}, 403
    elif sys.getsizeof(value) > 1000000:
        return False, {"result": "error", "msg": "Object too large. Size limit is 1MB"}, 403
    return True, {"result": "success", "msg": str(value)}, 200


def onlyKeyCheck(key):
    if not re.match('^[0-9a-zA-Z_]*$', key) or len(key) > 200 or len(key) == 0:
        return False, {"result": "error", "msg": "Key not valid"}, 403
    return True, {"result": "success"}, 200


hashMax = 10000
hasher = hashlib.sha1()
# given a key and a list of partitions (where each partition is a list of
# ip-port pairs for nodes in that partiotion), it returns a list of ip-port
# pairs that the key is stored in
def getPartitionID(key):
    number_of_partitions = len(METADATA.GLOBAL_VIEW)
    individual_size = int(hashMax/number_of_partitions)
    hasher.update(key)
    keyHash = int(hasher.hexdigest(), 16) % hashMax
    val = int(math.ceil(keyHash/individual_size))
    return val
