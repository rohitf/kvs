#     VECTOR CLOCK FUNCTIONS   #
#     Team Valetta             #
#     November 21st 2017       #

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
    VIEW[node_id] = {
        "ip_port": node_ip_port,
        "type": node_type
    }


def generate_response(payload, status_code):
    resp = ({jsonify(payload), status_code, {
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
    resp = ({jsonify(payload), status_code, {
            'Content-Type': 'application/json'}})
    return resp

# Causal payload helpers


# Check if two vector clocks are equal
def compare_payload(VC1=[0] * count_nodes(), VC2=[0] * count_nodes()):
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


def compare_timestamps(TS1=0, TS2=0):
    return LATEST if TS1 > TS2 else UPDATE

def equalityVC(VC1, VC2):
    for x in range(0, len(VC1)):
        if not(VC1[x] == VC2[x]):
            return False
    return True

#ewfwef
# To convert Strings into Vector Clocks from JSON Objects for saving

def parse_client_payload(VC_string):
    return [int(VC) for VC in VC_string.split(".")]

# To convert Vector Clocks into Strings for JSON objects
# EX [0 , 1 , 2 , 3] => "0.1.2.3"


def stringify_payload(VC):
    m = str(VC[0])
    for x in VC[1:]:
        m = m + "." + str(x)
    return m

# Checks if key is valid for putting


# def keyCheck(key, value=""):
#     if not re.match('^[0-9a-zA-Z_]*$', key) or len(key) > 200 or len(key) == 0:
#         return False, {"result": "error", "msg": "Key not valid"}, 403
#     elif value is None:
#         return False, {"result": "error", "msg": "No value provided"}, 403
#     elif sys.getsizeof(value) > 1000000:
#         return False, {"result": "error", "msg": "Object too large. Size limit is 1MB"}, 403
#     return True, {"result": "success", "msg": str(value)}, 200
