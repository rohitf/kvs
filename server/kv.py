#     KEY VALUE FUNCTIONS   #
#     Team Valetta          #
#     November 21st 2017    #

import re
# this is used for seeing the size of a value in put.
import sys
import time
import functions as app_funct

d = {}
vc = {}
timestamp = {}

def get(key):
    try:
        return d[key], vc[key], timestamp[key]
    except KeyError:
        return None, [0], None

# This function assumes that the causal payload is already the most recent
def put(key, value, causal_payload, ts):
    global d
    global vc
    global timestamp

    # print("client_cp in put: ", causal_payload, file=sys.stderr)

    # Confirms that there are no errors in the key and value
    b, m, status_code = app_funct.keyCheck(key, value)
    if not b:
        return b, status_code

    d[key] = value
    vc[key] = causal_payload
    timestamp[key] = int(time.time()) if ts is None else ts
    rep = key in d.keys()
    status_code = 201 if rep else 200
    return True, status_code

def getDictionaries():
    global d
    global vs
    global timestamp
    return d, vc, timestamp

def setDictionaries(di, vec, ts):
    global d
    global vc
    global timestamp
    (d, vc, timestamp) = (di, vec, ts)
