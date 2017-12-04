#!/usr/bin/python
import kv as kv
import functions as app_funct
from globals import *

# Replica Specific Information
VIEWS_os_env = os.getenv('VIEW')  # Stores all views that this Node knows of
VIEWS = []
if VIEWS_os_env is not None:
    VIEWS = VIEWS_os_env.split(",")
IP_PORT = os.getenv('IPPORT')
MY_ID = int(IP_PORT[8:-5])
REPLICAS_WANTED = os.getenv('K')  # K value given in command line
if REPLICAS_WANTED is not None:
    REPLICAS_WANTED = int(REPLICAS_WANTED)
else:
    REPLICAS_WANTED = 0
# Stores and updates list of replicas, no proxies
REPLICAS = VIEWS[:REPLICAS_WANTED]
IS_REPLICA = IP_PORT in REPLICAS  # Is this node a replica
PROXIES = VIEWS[len(REPLICAS):]
EXTERNAL_IP = (IP_PORT[:-5])
PORT = (IP_PORT[-4:])

app = Flask(__name__)


@app.route('/kv-store/<key>')
def get(key):
    isKeyValid = app_funct.onlyKeyCheck(key)
    client_CP = request.form.get('causal_payload')
    if not client_CP or client_CP == "''":
        client_CP = [0] * (MY_ID + 1)

    else:
        client_CP = app_funct.parseVC(client_CP)
    if not isKeyValid[0]:
        return (json.dumps(isKeyValid[1]), isKeyValid[2], {'Content-Type': 'application/json'})

    if IS_REPLICA:
        current_max_value, current_max_VC, current_max_timestamp = findNewest(
            key)
        my_value, my_VC, my_timestamp = kv.get(key)

        no_result = current_max_value is None
        need_update = not app_funct.equalityVC(current_max_VC, my_VC)
        status_code = 200

        if need_update:
            success, status_code = kv.put(
                key, current_max_value, current_max_VC, current_max_timestamp)
            if success:
                message = {"result": "success", "value": current_max_value, "node_id": MY_ID,
                           "causal_payload": current_max_VC, "timestamp": current_max_timestamp}
                return (json.dumps(message), status_code, {'Content-Type': 'application/json'})
            # The put fails then:
            a, message, status_code = app_funct.keyCheck(key, value)[1]
            return (json.dumps(message), status_code, {'Content-Type': 'application/json'})
        elif no_result:
            return (json.dumps({"result": "error", "msg": "Key does not exist"}), 404, {'Content-Type': 'application/json'})

        message = {"result": "success", "value": current_max_value, "node_id": MY_ID,
                   "causal_payload": current_max_VC, "timestamp": current_max_timestamp}
        return (json.dumps(message), status_code, {'Content-Type': 'application/json'})

    # Proxy
    else:
        for current_replica in REPLICAS:
            try:
                A = requests.get("http://" + current_replica + "/kv-store/" +
                                 key, data={'causal_payload': client_CP}, timeout=2)
                status_code = A.status_code
                response = A.json()
                if status_code == 404:
                    message = {"result": "error", "msg": "Key does not exist"}
                else:
                    message = {"result": response['result'], "value": response['value'], "node_id": MY_ID,
                               "causal_payload": response['causal_payload'], "timestamp": response['timestamp']}
                return (json.dumps(message), status_code, {'ContentType': 'application/json'})
            except requests.exceptions.Timeout:
                continue
        return (json.dumps({"result": "Error", "msg": "Server unavailable"}), 500, {'Content-Type': 'application/json'})

# Just return val, no checking with other nodes


@app.route('/kv-store/verify/<key>')
def stupidGet(key):
    value, causal_payload, timestamp = kv.get(key)
    return (jsonify({"value": value, "causal_payload": causal_payload, "timestamp": timestamp}), 200, {'ContentType': 'application/json'})


@app.route('/kv-store/<key>', methods=['PUT'])
def put(key):
    if not request.json:

        # getting json seems to be right
        # still get 500 error, doesn't start?

        val = request.form.get('val')
        client_CP = request.form.get('causal_payload')
        client_timestamp = int(time.time())

        if not client_CP or client_CP == "''":
            client_CP = [0] * (MY_ID + 1)

        else:
            client_CP = app_funct.parseVC(client_CP)

        # Check if key uses valid characters, value is small enough, etc.
        isKeyValid = app_funct.keyCheck(key, val)
        if not isKeyValid[0]:
            return (json.dumps(isKeyValid[1]), isKeyValid[2], {'Content-Type': 'application/json'})

        if IS_REPLICA:
            # If the client's payload isn't equal or more recent to current
            # payload, return failure
            my_val, my_vc, my_ts = kv.get(key)

            # commented this out because equalityVC is called in compareVC
            # if not app_funct.equalityVC(client_CP, my_val) and

            if not app_funct.compareVC(client_CP, my_vc):
                # TODO look into updating status code
                return (json.dumps({"result": "error", "causal_payload": app_funct.deparseVC(client_CP), "msg": "Client payload not most recent, get again before trying again"}), 404, {'Content-Type': 'application/json'})
            else:
                current_max_value, current_max_VC, current_max_timestamp = findNewest(
                    key)
                # print("MEOW1: current_max_VC ", current_max_VC, file=sys.stderr)
                # need to fix!! why does it return causal payload as a str at times
                # i changed findnewest to only return three variables, not a dict of three
                # if current_max_VC == "causal_payload":
                if len(current_max_VC) < len(REPLICAS):
                    diff = len(REPLICAS) - len(current_max_VC)
                    current_max_VC = [0] * diff
                # print("MEOW2: current_max_VC ", current_max_VC, file=sys.stderr)

                compare_VC_results = app_funct.compareVC(
                    client_CP, current_max_VC)

                # if current_max_VC is None:
                #     current_max_VC = [0] * len(VIEWS)

                if app_funct.equalityVC(client_CP, current_max_VC):
                    # update value + increment
                    client_CP[MY_ID] += 1
                    result, status_code = kv.put(
                        key, val, client_CP, client_timestamp)
                    message = {"result": "success", "value": val, "node_id": MY_ID, "causal_payload": app_funct.deparseVC(
                        client_CP), "timestamp": str(client_timestamp)}
                    return (json.dumps(message), status_code, {'ContentType': 'application/json'})

                elif (compare_VC_results is None and client_timestamp > int(current_max_timestamp)) or compare_VC_results:
                    # increment client_CP and update our value
                    # print("its me", file=sys.stderr)
                    client_CP[MY_ID] += 1
                    result, status_code = kv.put(
                        key, val, client_CP, client_timestamp)
                    message = {"result": "success", "value": val, "node_id": MY_ID, "causal_payload": app_funct.deparseVC(
                        client_CP), "timestamp": str(client_timestamp)}
                    return (json.dumps(message), status_code, {'ContentType': 'application/json'})

                else:
                    # client is smaller
                    # update our value and return failure
                    result, status_code = kv.put(
                        key, current_max_value, current_max_VC, current_max_timestamp)
                    message = {"result": "failure", "value": current_max_value, "node_id": MY_ID,
                               "causal_payload": app_funct.deparseVC(client_CP), "timestamp": str(current_timestamp)}
                    return (json.dumps(message), status_code, {'ContentType': 'application/json'})
        else:
            send_data = {'causal_payload': client_CP, 'val': val}
            for current_replica in REPLICAS:
                try:
                    A = requests.put("http://" + current_replica +
                                     "/kv-store/" + key, data=send_data, timeout=2)
                    status_code = A.status_code
                    response = A.json()
                    message = {"result": response['result'], "value": response['value'], "node_id": MY_ID,
                               "causal_payload": response['causal_payload'], "timestamp": response['timestamp']}
                    return (json.dumps(message), status_code, {'ContentType': 'application/json'})
                except requests.exceptions.Timeout:
                    continue
            return (json.dumps({"result": "Error", "msg": "Server unavailable"}), 500, {'Content-Type': 'application/json'})
    else:
        return (json.dumps({"result": "Error", "msg": "No VALUE provided"}), 403, {'Content-Type': 'application/json'})


@app.route('/kv-store/get_node_details')
def get_node():
    # when would this ever be a failure?
    r = "success"
    m = "Yes" if IS_REPLICA else "No"
    return (jsonify({"result": r, "replica": m}))


@app.route('/kv-store/get_all_replicas')
def get_all():
    r = "success" if REPLICAS else "failure"
    return (jsonify({"result": r, "replicas": REPLICAS}))


@app.route('/kv-store/update_view', methods=['PUT'])
def update():
    global VIEWS
    global REPLICAS

    type = request.args.get('type')
    update_ip = request.form.get('ip_port')

    if type == "add":
        print(update_ip)
        if update_ip not in VIEWS:
            VIEWS.append(update_ip)
        print("************************")
        print("http://" + update_ip + "/kv-store/duplicateview/")
        print(REPLICAS)
        print(VIEWS)
        try:
            m = requests.put("http://" + update_ip + "/kv-store/duplicateview", data={
                             'REPLICAS': listToString(REPLICAS), 'VIEWS': listToString(VIEWS)})
            print(m)
        except:
            print("______________________ERROR_______________________")
        # new ip should be replica if Nodes <= k
        # check for live nodes? just assume
        if len(REPLICAS) < REPLICAS_WANTED:
            REPLICAS.append(update_ip)
            duplicateReplica(update_ip)
            for i in REPLICAS:
                # Don't send to own node id
                if IS_REPLICA and i is not REPLICAS[MY_ID]:
                    try:
                        requests.get("http://" + i + "/add/",
                                     data={'update_ip': update_ip}, timeout=5)
                    except requests.exceptions.Timeout:
                        continue
        # else if proxy
        elif len(REPLICAS) > REPLICAS_WANTED:
            PROXIES.append(update_ip)
            for i in REPLICAS:
                try:
                    requests.get("http://" + i + "/add/",
                                 data={'update_ip': update_ip}, timeout=5)
                except requests.exceptions.Timeout:
                    continue

        return jsonify({"msg": "success", "node_id": VIEWS.index(update_ip), "number_of_nodes": len(REPLICAS)})

    elif type == "remove":
        # If just remove from REPLICAS, leave in VIEWS
        if update_ip in REPLICAS:
            REPLICAS.remove(update_ip)
            for i in REPLICAS:
                if IS_REPLICA and i is not REPLICAS[MY_ID]:
                    try:
                        requests.put("http://" + i + "/remove/",
                                     data={'update_ip': update_ip}, timeout=5)
                    except requests.exceptions.Timeout:
                        continue

        elif update_ip in PROXIES:
            PROXIES.remove(update_ip)
            for i in REPLICAS:
                try:
                    requests.put("http://" + i + "/remove/",
                                 data={'update_ip': update_ip}, timeout=5)
                except requests.exceptions.Timeout:
                    continue

        return jsonify({"result": "success", "number_of_nodes": len(REPLICAS)})

    else:
        return jsonify({"result": "failure", "replicas": str(REPLICAS)})


@app.route('/add', methods=['PUT'])
def add():
    global VIEWS
    global REPLICAS

    update_ip = request.form.get('update_ip')
    if update_ip not in VIEWS:
        VIEWS.append(update_ip)

    # Redundant check for replica or proxy because ?? just to avoid sending
    # request to self in update_view
    if (len(REPLICAS) + len(PROXIES)) <= REPLICAS_WANTED:
        if update_ip not in REPLICAS:
            REPLICAS.append(update_ip)
    elif (len(REPLICAS) + len(PROXIES)) > REPLICAS_WANTED:
        PROXIES.append(update_ip)


@app.route('/remove', methods=['PUT'])
def remove():
    global VIEWS
    global REPLICAS

    update_ip = request.form.get('update_ip')
    if update_ip in REPLICAS:
        REPLICAS.remove(update_ip)

    elif update_ip in PROXIES:
        PROXIES.remove(update_ip)


@app.route('/kv-store/duplicate', methods=['PUT'])
def duplicate():
    d = json.loads(request.form.get('d'))
    vc = json.loads(request.form.get('vc'))
    timestamp = json.loads(request.form.get('timestamp'))
    kv.setDictionaries(d, vc, timestamp)


@app.route('/kv-store/gossip', methods=['POST'])
def getGossip():
    # Goes through all data, updates to newer data, and saves info about data that
    # this node has that is more recent and sends that back

    sent_d = json.loads(request.form.get('d'))
    sent_vc = json.loads(request.form.get('vc'))
    sent_timestamp = json.loads(request.form.get('timestamp'))
    to_update = {}

    for key in sent_d:
        my_d_val, my_vc_val, my_ts_val = kv.get(key)
        sent_d_val = sent_d[key]
        sent_vc_val = sent_vc[key]
        sent_ts_val = sent_timestamp[key]

        # if sent has a value and we do not
        if my_d_val is None:
            result, status_code = kv.put(
                key, sent_d_val, sent_vc_val, sent_ts_val)
            continue

        # if equal, do nothing
        if app_funct.equalityVC(my_vc_val, sent_vc_val):
            continue

        compare_VC_results = app_funct.compareVC(my_vc_val, sent_vc_val)
        if (compare_VC_results is None and my_ts_val > sent_ts_val) or compare_VC_results:
            # mine is bigger, save it
            to_update[key] = {"value": my_d_val,
                              "causal_payload": my_vc_val, "timestamp": my_ts_val}
        else:
            # theirs is bigger, update mine
            result, status_code = kv.put(
                key, sent_d_val, sent_vc_val, sent_ts_val)

    return json.dumps(to_update)

# Quick check to see if it's alive


@app.route('/hey', methods=['GET'])
def hey():
    return (json.dumps({"result": "success"}), 200, {'Content-Type': 'application/json'})


def duplicateReplica(proxy_ipp):
    d, vc, timestamp = kv.getDictionaries()
    d, vc, timestamp = json.dumps(d), json.dumps(vc), json.dumps(timestamp)
    requests.put("http://" + proxy_ipp + "/kv-store/duplicate/",
                 data={"d": d, "vc": vc, "timestamp": timestamp})

# omg did you hear what Becky did???


def gossip(gossip_ipp):
    # Send all information to other node
    d, vc, timestamp = kv.getDictionaries()
    d, vc, timestamp = json.dumps(d), json.dumps(vc), json.dumps(timestamp)
    try:
        resp = requests.post("http://" + gossip_ipp + "/kv-store/gossip",
                             data={"d": d, "vc": vc, "timestamp": timestamp})
        print("RESP")
        print(vars(resp))
        resp = resp.json()

        # Gets info that it needs to update, and updates it's own data
        for key in resp:
            s, status_code = kv.put(
                key, resp[key]["value"], resp[key]["causal_payload"], resp[key]["timestamp"])
        return "SUCCESS"
    except:
        return "FAILED"
    # TODO: add something for this to return


def findNewest(key):
    replica_req = []
    current_max_value, current_max_VC, current_max_timestamp = kv.get(key)
    for current_replica in REPLICAS:
        if current_replica == IP_PORT:
            continue

        try:
            A = requests.get("http://" + current_replica +
                             "/kv-store/verify/" + key, timeout=.5)
            res = A.json()
            replica_req.append(res)

        except requests.exceptions.Timeout:
            continue

        temp_value = replica_req[-1]['value']
        temp_VC = replica_req[-1]['causal_payload']
        temp_timestamp = replica_req[-1]['timestamp']
        # print("MEOW: temp_VC", temp_VC, " current_max_VC ", current_max_VC, file=sys.stderr)
        compare_VC_results = app_funct.compareVC(temp_VC, current_max_VC)

        # check if given value, if no value
        if(temp_value is None):
            continue
        else:
            no_result = False

        # two VC's are the same, then do nothing
        if app_funct.equalityVC(current_max_VC, temp_VC):
            continue
        if (compare_VC_results is None and current_timestamp > current_max_timestamp) or compare_VC_results:
            (current_max_value, current_max_VC, current_max_timestamp) = (
                temp_value, temp_VC, temp_timestamp)

    # return {"value" : current_max_value, "causal_payload" : current_max_VC, "timestamp" : current_max_timestamp}
    # return IP_PORT
    return current_max_value, current_max_VC, current_max_timestamp


def ping():
    while True:
        reqs = [grequests.get("http://" + node_address + "/hey", timeout=5)
                for node_address in REPLICAS]
        grequests.map(reqs, exception_handler=ping_failed)
        time.sleep(2)


def ping_failed(request, exception):
    url = request.url.split("//")[1].split("/")[0]
    print("PING Failed: " + url)
    for m in REPLICAS:
        try:
            requests.put(
                "http://" + m + "/kv-store/update_view?type=remove", data={'url': url}, timeout=5)
            print("SUCC")
            break
        except:
            print("CAN't find")
            continue

# hmm...fixed


def listToString(lst):
    m = '[' + ''.join(lst) + ']'
    return m


def stringToList(stg):
    return (stg[1:-1]).split(",")


def runGossip():
    while True:
        for r in REPLICAS:
            status = gossip(r)
    sleep(10)


background_thread = Thread(target=ping, args=())
background_thread.start()

background_thread = Thread(target=runGossip, args=())
background_thread.start()


@app.route('/kv-store/duplicateview', methods=['PUT'])
def duplicateView():
    global REPLICAS
    global VIEWS
    print("_________________DUPLICATED______________________")
    REPLICAS = stringToList(request.form.get('REPLICAS'))
    VIEWS = stringToList(request.form.get('VIEWS'))
    return (json.dumps({"result": "success"}), 200, {'Content-Type': 'application/json'})


if __name__ == '__main__':
    # Run Command
    app.run(host=EXTERNAL_IP, port=int(PORT), debug=True)

# print(REPLICAS_WANTED, file=sys.stderr)
# print(REPLICAS, file=sys.stderr)
# print(IS_REPLICA, file=sys.stderr)
# print(MY_ID, file=sys.stderr)
# print(VIEWS, file=sys.stderr)
# print(PROXIES, file=sys.stderr)
