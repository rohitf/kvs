#     VECTOR CLOCK FUNCTIONS   #
#     Team Valetta             #
#     November 21st 2017       #

# Check if two vector clocks, VC1 & VC2 are comparable, if not, returns False
# If they are comparable, then it returns True to show that they are comparable
# and a second return, True or False, depending on if VC1 is bigger than VC2
# This function returns None if the vector clocks are the same

# this is for checking if key has only letters, numbers, and underscores.
import re
# this is used for seeing the size of a value in put.
import sys

def compareVC(VC1, VC2):
    # print("VC1: ", VC1, " VC2: ", VC2, file=sys.stderr)
    # Check if lengths are different
    diffLen = abs(len(VC1) - len(VC2))
    placeholders = [0] * diffLen
    VC2.extend(placeholders) if len(VC1) > len(
        VC2) else VC1.extend(placeholders)

    # Compare VC's
    if equalityVC(VC1, VC2):
        return True

    # Subtract elements in VC2 from VC1
    clockDiffs = [a - b for a, b in zip(VC1, VC2)]
    # Check if diffs are all positive OR all negative
    sameSign = not (min(clockDiffs) < 0 < max(clockDiffs))
    if not sameSign:
        return None  # Concurrent!

    # print("sameSign : ", sameSign, file=sys.stderr)
    # print("clockDiffs : ", clockDiffs, file=sys.stderr)
    return True if clockDiffs[0] >= 0 else False

# Check if two vector clocks are equal
def equalityVC(VC1, VC2):
    # print("vc1: ", VC1, " vc2: ", VC2, file=sys.stderr)
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
    # if len(VC_string) > 1:
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
    # print("key", key, " value : ", value, file=sys.stderr)
    if not re.match('^[0-9a-zA-Z_]*$', key) or len(key) > 200 or len(key) == 0:
        return False, {"result": "error", "msg": "Key not valid"}, 403
    elif value is None:
        return False, {"result": "error", "msg": "No value provided"}, 403
    elif sys.getsizeof(value) > 1000000:
        return False, {"result": "error", "msg": "Object too large. Size limit is 1MB"}, 403
    return True, {"result": "success", "msg": str(value)}, 200

def onlyKeyCheck(key):
    # print("key1", key, file=sys.stderr)
    if not re.match('^[0-9a-zA-Z_]*$', key) or len(key) > 200 or len(key) == 0:
       return False, {"result": "error", "msg": "Key not valid"}, 403
    return True, {"result": "success"}, 200


# test = [0, 1]
# test2 = [0]
# print(compareVC(test, test2))
# test3 = [0, 1, 2, 0]
# test4 = [1, 1, 2, 1]
# print(compareVC(test3, test4))
# test5 = [1, 2, 4, 5]
# test6 = [2, 1, 4, 3]
# print(compareVC(test5, test6))
# print(parseVC("0.1.2.3"))
# print(parseVC("0.1.2.3"))
