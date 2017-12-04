#     VECTOR CLOCK FUNCTIONS   #
#     Team Valetta             #
#     November 21st 2017       #
import re
import sys

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
        VC1.extend([0]*diff)
    elif len(VC2) < len(VC1):
        diff = len(VC2) - len(VC1)
        VC2.extend([0]*diff)
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
