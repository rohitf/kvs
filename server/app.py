#!/usr/bin/python
import kv as kv
import functions as fn
import math
from globals import *

# Replica Specific Information
VIEWS_os_env = os.getenv('VIEW')  # Stores all views that this Node knows of
VIEWS = []
if VIEWS_os_env is not None:
    VIEWS = VIEWS_os_env.split(",")
    fn.generateGlobalView(VIEWS)

META.IP_PORT = os.getenv('IPPORT') # Own ip port
META.ID = META.GLOBAL_VIEW[META.THIS_PARTITION].find(META.IP_PORT)
META.REPLICAS_PER_PART = int(os.getenv('K'))  # K value given in command line



# At init, proxies & replicas should be derived from views
N = len(VIEWS)
num_of_partitions = math.floor(N/K) # Number of partitions

META.REPLICAS = VIEWS[:num_of_partitions * META.REPLICAS_PER_PART] # List of ip ports of all replicas
META.PROXIES = list(set(VIEWS) - set(META.REPLICAS)) # List of ip ports of remainder proxy nodes
META.NODE_TYPE = getNodeType() # TODO implement this please someone
IS_REPLICA = META.NODE_TYPE == REPLICA  # T/F if node is replica
META.DIRECTORY = fn.generateDirectory(num_of_partitions)
if IS_REPLICA:
    META.THIS_PARTITION = (VIEWS.index(META.IP_PORT)+1)/META.REPLICAS_PER_PART # ex. [1, 1, 1, 2, 2, 2, 3, 3]
META.EXTERNAL_IP = (META.IP_PORT[:-5])
META.PORT = (META.IP_PORT[-4:])

app = Flask(__name__)

@app.route('/kv-store/<key>')
def get(key):
    isKeyValid = fn.onlyKeyCheck(key)
    client_CP = request.form.get('causal_payload')
    if not client_CP or client_CP == "''":
        client_CP = [0] * (META.REPLICAS_PER_PART)
    else:
        client_CP = fn.parseVC(client_CP)

    if not isKeyValid[0]:
        return fn.http_error(isKeyValid[1], isKeyValid[2])

    if IS_REPLICA and fn.getPartitionId(key) == META.THIS_PARTITION:

        current_max_value, current_max_VC, current_max_timestamp = findNewest(key)
        my_value, my_VC, my_timestamp = kv.get(key)

        no_result = current_max_value is None
        need_update = not fn.equalityVC(current_max_VC, my_VC)
        status_code = 200

        if need_update:
            success, status_code = kv.put(
                key, current_max_value, current_max_VC, current_max_timestamp)
            if success:
                message = {"result": "success", "value": current_max_value, "node_id": MY_ID,
                           "causal_payload": current_max_VC, "timestamp": current_max_timestamp}
                return fn.http_success(message, status_code)
            # The put fails then:
            a, message, status_code = fn.keyCheck(key, value)[1]
            return fn.http_error(message, status_code)
        elif no_result:
            return fn.http_error({"result": "error", "msg": "Key does not exist"}, 404)
        message = {"result": "success", "value": current_max_value, "node_id": MY_ID,
                   "causal_payload": current_max_VC, "timestamp": current_max_timestamp}
        return fn.http_error(message, status_code)

    # Proxy
    else:
        correct_part = fn.getPartitionId(key)
        for current_replica in META.GLOBAL_VIEW[correct_part]:
            try:
                A = requests.get("http://" + current_replica + "/kv-store/" +
                                 key, data={'causal_payload': client_CP}, timeout=2)
                status_code = A.status_code
                response = A.json()
                if status_code == 404:
                    return fn.http_error({"result": "error", "msg": "Key does not exist"}, 404)
                message = {"result": response['result'], "value": response['value'], "partition_id": META.THIS_PARTITION,
                            "causal_payload": response['causal_payload'], "timestamp": response['timestamp']}
                return fn.http_success(message, status_code)
            except requests.exceptions.Timeout:
                continue
        return fn.http_error({"result": "Error", "msg": "Server unavailable"}, 500)

# Just return val, no checking with other nodes
@app.route('/kv-store/verify/<key>')
def stupidGet(key):
    value, causal_payload, timestamp = kv.get(key)
    message = {"value": value, "causal_payload": causal_payload, "timestamp": timestamp}
    return fn.http_success(message, 200)

@app.route('/kv-store/<key>', methods=['PUT'])
def put(key):
    if not request.json:

        val = request.form.get('val')
        client_CP = request.form.get('causal_payload')
        client_timestamp = int(time.time())

        if not client_CP or client_CP == "''":
            client_CP = [0] * (META.REPLICAS_PER_PART)
        else:
            client_CP = fn.parseVC(client_CP)

        # Check if key uses valid characters, value is small enough, etc.
        isKeyValid = fn.keyCheck(key, val)
        if not isKeyValid[0]:
            return fn.http_error(isKeyValid[1], isKeyValid[2])

        if IS_REPLICA and fn.getPartitionId(key) == META.THIS_PARTITION:
            my_val, my_vc, my_ts = kv.get(key)

            # Client has old CP
            if fn.compare_payload(my_vc, client_CP) == LATEST:
                message = {"result": "error", "causal_payload": fn.deparseVC(client_CP), "msg": "Client payload not most recent, get again before trying again"}
                return fn.http_error(message, 404)

            else:
                current_max_value, current_max_VC, current_max_timestamp = findNewest(key)
                if len(current_max_VC) < META.REPLICAS_PER_PART:
                    diff = len(META.REPLICAS_PER_PART) - len(current_max_VC)
                    current_max_VC = [0] * diff

                compare_VC_results = fn.compare_payload(current_max_VC, client_CP)

                if fn.equalityVC(client_CP, current_max_VC):
                    # update value + increment
                    client_CP[fn.getNodeID(META.IP_PORT)] += 1
                    result, status_code = kv.put(key, val, client_CP, client_timestamp)
                    message = {"result": "success", "value": val, "partition_id": META.THIS_PARTITION, "causal_payload": fn.deparseVC(
                        client_CP), "timestamp": str(client_timestamp)}
                    return fn.http_success(message, status_code)
                elif (compare_VC_results == CONCURRENT and client_timestamp > int(current_max_timestamp)) or compare_VC_results == UPDATE:
                    # increment client_CP and update our value
                    client_CP[fn.getNodeID(META.IP_PORT)] += 1
                    result, status_code = kv.put(key, val, client_CP, client_timestamp)
                    message = {"result": "success", "value": val, "partition_id": META.THIS_PARTITION, "causal_payload": fn.deparseVC(
                        client_CP), "timestamp": str(client_timestamp)}
                    return fn.http_success(message, status_code)
                else:
                    # client is smaller: update our value and return failure
                    result, status_code = kv.put(key, current_max_value, current_max_VC, current_max_timestamp)
                    message = {"result": "failure", "value": current_max_value, "partition_id": META.THIS_PARTITION,
                               "causal_payload": fn.deparseVC(client_CP), "timestamp": str(current_timestamp)}
                    return fn.http_error(message, status_code)
        else:
            correct_part = fn.getPartitionId(key)
            send_data = {'causal_payload': client_CP, 'val': val}
            for current_replica in META.GLOBAL_VIEW[correct_part]:
                try:
                    A = requests.put("http://" + current_replica +
                                     "/kv-store/" + key, data=send_data, timeout=2)
                    status_code = A.status_code
                    response = A.json()
                    message = {"result": response['result'], "value": response['value'], "partition_id": META.THIS_PARTITION,
                               "causal_payload": response['causal_payload'], "timestamp": response['timestamp']}
                    return fn.http_success(message, status_code) # this could be error or success
                except requests.exceptions.Timeout:
                    continue
            return fn.http_error({"result": "Error", "msg": "Server unavailable"}, 500)
    else:
        return fn.http_error({"result": "Error", "msg": "No VALUE provided"}, 403)


@app.route('/kv-store/get_node_details')
def get_node():
    r = "success"
    m = "Yes" if IS_REPLICA else "No"
    return (jsonify({"result": r, "replica": m}))


@app.route('/kv-store/get_all_replicas')
def get_all():
    r = "success" if META.REPLICAS else "failure"
    return (jsonify({"result": r, "replicas": META.REPLICAS}))


@app.route('/kv-store/update_view', methods=['PUT'])
def update():
    type = request.args.get('type')
    update_ip = request.form.get('ip_port')

    if type == "add":
        print(update_ip)
        if update_ip not in getLocalView():
            getLocalView().append(update_ip)
        try:
            m = requests.put("http://" + update_ip + "/kv-store/duplicateview", data={
                             'REPLICAS': fn.listToString(META.REPLICAS), 'VIEWS': fn.listToString(VIEWS)})
            print(m)
        except:
            print("______________________ERROR_______________________")
        # new ip should be replica if Nodes <= k
        # check for live nodes? just assume
        if len(META.REPLICAS) < REPLICAS_WANTED:
            META.REPLICAS.append(update_ip)
            duplicateReplica(update_ip)
            for i in META.REPLICAS:
                # Don't send to own node id
                if IS_REPLICA and i is not META.REPLICAS[MY_ID]:
                    try:
                        requests.put("http://" + i + "/add/",
                                     data={'update_ip': update_ip}, timeout=5)
                    except requests.exceptions.Timeout:
                        continue
        # else if proxy
        elif len(META.REPLICAS) > REPLICAS_WANTED:
            PROXIES.append(update_ip)
            for i in META.REPLICAS:
                try:
                    requests.put("http://" + i + "/add/",
                                 data={'update_ip': update_ip}, timeout=5)
                except requests.exceptions.Timeout:
                    continue

        return jsonify({"msg": "success", "node_id": VIEWS.index(update_ip), "number_of_nodes": len(META.REPLICAS)})

    elif type == "remove":
        # If just remove from REPLICAS, leave in VIEWS
        if update_ip in META.REPLICAS:
            META.REPLICAS.remove(update_ip)
            for i in META.REPLICAS:
                if IS_REPLICA and i is not META.REPLICAS[MY_ID]:
                    try:
                        requests.put("http://" + i + "/remove/",
                                     data={'update_ip': update_ip}, timeout=5)
                    except requests.exceptions.Timeout:
                        continue

        elif update_ip in PROXIES:
            PROXIES.remove(update_ip)
            for i in META.REPLICAS:
                try:
                    requests.put("http://" + i + "/remove/",
                                 data={'update_ip': update_ip}, timeout=5)
                except requests.exceptions.Timeout:
                    continue

        return jsonify({"result": "success", "number_of_nodes": len(META.REPLICAS)})

    else:
        return jsonify({"result": "failure", "replicas": str(META.REPLICAS)})


@app.route('/add', methods=['PUT'])
def add():
    update_ip = request.form.get('update_ip')
    if update_ip not in VIEWS:
        VIEWS.append(update_ip)

    # Redundant check for replica or proxy to avoid sending
    # request to self in update_view
    if (len(META.REPLICAS) + len(PROXIES)) <= REPLICAS_WANTED:
        if update_ip not in META.REPLICAS:
            META.REPLICAS.append(update_ip)
    elif (len(META.REPLICAS) + len(PROXIES)) > REPLICAS_WANTED:
        PROXIES.append(update_ip)


@app.route('/remove', methods=['PUT'])
def remove():
    update_ip = request.form.get('update_ip')
    if update_ip in META.REPLICAS:
        META.REPLICAS.remove(update_ip)

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
        if fn.equalityVC(my_vc_val, sent_vc_val):
            continue

        compare_VC_results = fn.compareVC(my_vc_val, sent_vc_val)
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


def findNewest(key):
    replica_req = []
    current_max_value, current_max_VC, current_max_timestamp = kv.get(key)
    for current_replica in META.REPLICAS:
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
        compare_VC_results = fn.compareVC(temp_VC, current_max_VC)

        # check if given value, if no value
        if(temp_value is None):
            continue
        else:
            no_result = False

        # two VC's are the same, then do nothing
        if fn.equalityVC(current_max_VC, temp_VC):
            continue
        if (compare_VC_results is None and current_timestamp > current_max_timestamp) or compare_VC_results:
           (current_max_value, current_max_VC , current_max_timestamp) = (temp_value, temp_VC, temp_timestamp)

    return current_max_value, current_max_VC, current_max_timestamp


def ping():
    while True:
        reqs = [grequests.get("http://" + node_address + "/hey", timeout=5)
                for node_address in META.REPLICAS]
        grequests.map(reqs, exception_handler=ping_failed)
        time.sleep(2)


def ping_failed(request, exception):
    url = request.url.split("//")[1].split("/")[0]
    print("PING Failed: " + url)
    for m in META.REPLICAS:
        try:
            requests.put(
                "http://" + m + "/kv-store/update_view?type=remove", data={'url': url}, timeout=5)
            print("SUCC")
            break
        except:
            print("CAN'T find")
            continue

def runGossip():
    while True:
        for r in META.REPLICAS:
            status = gossip(r)
    sleep(10)


background_thread = Thread(target=ping, args=())
background_thread.start()

background_thread = Thread(target=runGossip, args=())
background_thread.start()


@app.route('/kv-store/duplicateview', methods=['PUT'])
def duplicateView():
    META.REPLICAS = fn.stringToList(request.form.get('REPLICAS'))
    VIEWS = fn.stringToList(request.form.get('VIEWS'))
    return (json.dumps({"result": "success"}), 200, {'Content-Type': 'application/json'})


if __name__ == '__main__':
    # Run Command
    app.run(host=EXTERNAL_IP, port=int(PORT), debug=True)
