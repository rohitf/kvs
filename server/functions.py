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

def count_nodes():
    return len(get_all_replicas() + proxies())

def get_node_type():
    return PROXY if META.IP_PORT in proxies() else REPLICA

def get_id(ip_port):
    return ip_port[8:-5]

def get_all_nodes():
    return get_all_replicas().extend(proxies())

def get_all_replicas():
    all_replicas = []
    for partition in META.GLOBAL_VIEW:
        all_replicas.extend(partition)

    return all_replicas

def get_replicas(partition_id):
    return META.GLOBAL_VIEW[partition_id]

def proxies():
    return META.GLOBAL_VIEW[0]

def add_replicas(partition_id, *node_ips):
    META.GLOBAL_VIEW[partition_id].extend(node_ips)

def add_proxy(node_ip):
    META.GLOBAL_VIEW[0].append(node_ip)

def remove_proxy(node_ip):
    META.GLOBAL_VIEW[0].remove(node_ip)

def clear_proxies():
    META.GLOBAL_VIEW[0] = []

def node_type(node_ip):
    return REPLICA if node_ip in get_all_replicas() else PROXY

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

def clean_proxy(proxy_ip):
    try:
        data = {"directory": {}, "global_view": {}}
        url = "http://" + proxy_ip + "/kv-store/stupid_update"
        resp = requests.put(url, timeout=5)
        break
    except requests.exceptions.Timeout:
        continue

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

def put_broadcast(params, urls):
    rs = [grequests.put(url, data=params, timeout=5) for url in urls]
    grequests.map(rs, exception_handler=broadcast_failed)

    return rs

def broadcast_failed(request, exception):
    url = request.url.split("//")[1].split("/")[0]
    print("\nBROADCAST FAILED: " + url)


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
    m = '[' + ','.join(map(str, lst)) + ']'
    return m

def stringToList(stg):
    return (stg[1:-1]).split(",")

def getThisPartition():
    return META.GLOBAL_VIEW[META.THIS_PARTITION]

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

def generateGlobalView(all_views):
    if all_views is None:
        return

    split = META.REPLICAS_PER_PART
    GV = META.GLOBAL_VIEW

    for i in range(len(all_views)):
        if i % split == 0:  # partition is full, so create new one
            add_partition()
        GV[len(GV)].append(all_views[i])
        # all_views will append ip as string to gv

    # check if last partition needs to become proxies
    last_partition_index = len(GV)
    if len(GV[last_partition_index]) < META.REPLICAS_PER_PART:
        temp = GV[last_partition_index]
        del GV[last_partition_index]
        GV[0] = temp

def add_partition(nodes=[]):
    new_partition_index = len(META.GLOBAL_VIEW) + 1
    META.GLOBAL_VIEW[new_partition_index] = nodes[:] # pass a shallow copy of the nodes to avoid unwanted references

def count_partitions():
    return len(META.GLOBAL_VIEW) - 1

def last_partition_id():
    return META.GLOBAL_VIEW[count_partitions()]

def getLocalView():
    return META.GLOBAL_VIEW[META.THIS_PARTITION]

def getNodeID(IP_PORT):
    return META.GLOBAL_VIEW[META.THIS_PARTITION].find(IP_PORT)

def dictionaryToString(dictionary):
    return json.dumps(dictionary)

def stringToDictionary(strng):
    return json.loads(strng)
