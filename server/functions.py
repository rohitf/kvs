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
    META.VIEW[node_id] = {
        "ip_port": node_ip_port,
        "type": node_type
    }


def http_success(payload, status_code):
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


def http_error(payload, status_code):
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
def compare_payload(VC1=[0] * META.REPLICAS_PER_PART, VC2=[0] * META.REPLICAS_PER_PART):
    diff_len = abs(len(VC1) - len(VC2))

    # Compare VC's
    if equalityVC(VC1, VC2):
        return CONCURRENT

    # Subtract elements in VC2 from VC1
    clockDiffs = [a - b for a, b in zip(VC1, VC2)]
    # Check if diffs are all positive OR all negative
    sameSign = not (min(clockDiffs) < 0 < max(clockDiffs))
    if not sameSign:
        return CONCURRENT

    return LATEST if clockDiffs[0] > 0 else UPDATE

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

def listToString(lst):
    m = '[' + ''.join(lst) + ']'
    return m

def stringToList(stg):
    return (stg[1:-1]).split(",")


hashMax = 10000
hasher = hashlib.sha1()

# given a key and a list of partitions (where each partition is a list of
# ip-port pairs for nodes in that partiotion), it returns a list of ip-port
# pairs that the key is stored in
def getPartitionId(key):
    hasher.update(key)
    keyHash = int(hasher.hexdigest(), 16) % hashMax
    for part in META.DIRECTORY:
        if META.DIRECTORY[part][0] <= keyHash and META.DIRECTORY[part][1] >= keyHash:
            return part
    print("FAILURE in getPartitionId")
    return -1

def generateDirectory(num_of_partitions):
    individual_size = int(hashMax/num_of_partitions)
    extra = hashMax % num_of_partitions
    m = 0
    for x in range(0, num_of_partitions - 1):
        META.DIRECTORY[x+1] = [m, (m+individual_size-1)]
        m += individual_size
    META.DIRECTORY[num_of_partitions] = [m, (m+individual_size-1 + extra)]

def generateGlobalView(ALL_VIEWS):
    split = META.REPLICAS_PER_PART
    local_views = []
    for i in range(1, len(ALL_VIEWS) + 1):
        if i%split == 0: # partition is full, so create new one
            new_partition_id = len(META.GLOBAL_VIEW) + 1
            META.GLOBAL_VIEW[new_partition_id] = []
        else:
            META.GLOBAL_VIEW[len(META.GLOBAL_VIEW)].append(ALL_VIEWS[i-1])

def getLocalView():
    return META.GLOBAL_VIEW[META.THIS_PARTITION]

def getNodeID(IP_PORT):
    return META.GLOBAL_VIEW[META.THIS_PARTITION].find(IP_PORT)
