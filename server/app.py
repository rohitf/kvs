#!/usr/bin/python
import kv as kv
import functions as fn
import math
from globals import *

VIEWS_os_env = os.getenv('VIEW')  # Stores all views that this Node knows of
META.IP_PORT = os.getenv('IPPORT')  # Own ip port

if VIEWS_os_env is not None:
    VIEWS = VIEWS_os_env.split(",")
    fn.generateGlobalView(VIEWS)

    META.ID = META.GLOBAL_VIEW[META.THIS_PARTITION].find(META.IP_PORT)
    # K value given in command line
    META.REPLICAS_PER_PART = int(os.getenv('K'))

    # At init, proxies & replicas should be derived from views
    N = len(VIEWS)
    num_of_partitions = math.floor(
        N / META.REPLICAS_PER_PART)  # Number of partitions
    fn.generateDirectory(num_of_partitions)

    META.NODE_TYPE = fn.get_node_type()
    IS_REPLICA = META.NODE_TYPE == REPLICA  # T/F if node is replica

if IS_REPLICA:
    META.THIS_PARTITION = (VIEWS.index(META.IP_PORT) +
                           1) / META.REPLICAS_PER_PART

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

        current_max_value, current_max_VC, current_max_timestamp = findNewest(
            key)
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


@app.route('/kv-store/verify/<key>')
def stupidGet(key):
    value, causal_payload, timestamp = kv.get(key)
    message = {"value": value, "causal_payload": causal_payload,
               "timestamp": timestamp}
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
                message = {"result": "error", "causal_payload": fn.deparseVC(
                    client_CP), "msg": "Client payload not most recent, get again before trying again"}
                return fn.http_error(message, 404)

            else:
                current_max_value, current_max_VC, current_max_timestamp = findNewest(
                    key)
                if len(current_max_VC) < META.REPLICAS_PER_PART:
                    diff = len(META.REPLICAS_PER_PART) - len(current_max_VC)
                    current_max_VC = [0] * diff

                compare_VC_results = fn.compare_payload(
                    current_max_VC, client_CP)

                if fn.equalityVC(client_CP, current_max_VC):
                    # update value + increment
                    client_CP[fn.getNodeID(META.IP_PORT)] += 1
                    result, status_code = kv.put(
                        key, val, client_CP, client_timestamp)
                    message = {"result": "success", "value": val, "partition_id": META.THIS_PARTITION, "causal_payload": fn.deparseVC(
                        client_CP), "timestamp": str(client_timestamp)}
                    return fn.http_success(message, status_code)
                elif (compare_VC_results == CONCURRENT and client_timestamp > int(current_max_timestamp)) or compare_VC_results == UPDATE:
                    # increment client_CP and update our value
                    client_CP[fn.getNodeID(META.IP_PORT)] += 1
                    result, status_code = kv.put(
                        key, val, client_CP, client_timestamp)
                    message = {"result": "success", "value": val, "partition_id": META.THIS_PARTITION, "causal_payload": fn.deparseVC(
                        client_CP), "timestamp": str(client_timestamp)}
                    return fn.http_success(message, status_code)
                else:
                    # client is smaller: update our value and return failure
                    result, status_code = kv.put(
                        key, current_max_value, current_max_VC, current_max_timestamp)
                    message = {"result": "failure", "value": current_max_value, "partition_id": META.THIS_PARTITION,
                               "causal_payload": fn.deparseVC(client_CP), "timestamp": str(current_max_timestamp)}
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
                    # this could be error or success
                    return fn.http_success(message, status_code)
                except requests.exceptions.Timeout:
                    continue
            return fn.http_error({"result": "Error", "msg": "Server unavailable"}, 500)
    else:
        return fn.http_error({"result": "Error", "msg": "No VALUE provided"}, 403)


@app.route('/kv-store/stupid_update', methods=['PUT'])
def updateMeta():
    update_attrs = request.form.to_dict()

    for attr in update_attrs:
        attr = attr.upper()
        META[attr] = request.form.get(attr)


def rebalanceNodes():
    for partition in META.GLOBAL_VIEW:
        if partition == 0:  # ignore proxies
            continue
        for node_ipp in META.GLOBAL_VIEW[partition]:
            try:
                url = "http://" + node_ipp + "/rebalance"
                resp = requests.put(url, timeout=5)
                break
            except requests.exceptions.Timeout:
                continue


@app.route('/rebalance')
def rebalance():
    for node_ipp in fn.getLocalView():
        if node_ipp == META.IP_PORT:
            continue
        resp = gossip(node_ipp)
        if resp == ERROR:
            print("REBALANCE FAILED")

    toSend = {}
    for key in d:
        correct_part = fn.getPartitionId(key)
        if correct_part == META.THIS_PARTITION:
            continue
        curr_val, curr_vc, curr_ts = kv.get(key)
        try:
            toSend[correct_part].append({'key': key, 'value': curr_val, 'casual_payload': fn.listToString(
                curr_vc), 'timestamp': curr_ts})  # raises Error the first time, disgusting!
        except KeyError:
            toSend[correct_part] = [{'key': key, 'value': curr_val, 'casual_payload': fn.listToString(
                curr_vc), 'timestamp': curr_ts}]
        kv.delete(key)

    for partition_num in toSend:
        for node_ipp in META.GLOBAL_VIEW[partition_num]:
            try:
                # convert content to string
                content = json.dumps(toSend[partition_num])
                resp = requests.put(
                    "http://" + node_ipp + "/key_dump", data={'content': content}, timeout=5)
                break
            except requests.exceptions.Timeout:
                continue

    d, vc, ts = kv.getDictionaries()
    kv_data = {'d': json.dumps(d), 'vc': json.dumps(
        vs), 'timestamp': json.dumps(ts)}

    # send updated kvs to other replicas in partition
    urls = ["http://" + node_ipp +
            "/kv-store/duplicate" for node_ipp in fn.get_replicas(META.THIS_PARTITION)]
    fn.put_broadcast(urls, kv_data)


@app.route('/kv-store/update_view', methods=['PUT'])
def update():
    type = request.args.get('type')
    update_ip = request.form.get('ip_port')

    # update our global view
    if type == "add":
        resp = addNode(update_ip)
        r = resp["result"]
        part_id = resp["message"]
    else:
        resp = addNode(update_ip)
        r = resp["result"]
        part_id = resp["message"]

    if r == SUCCESS:
        # put this stuff in a callback
        message = {
            "result": "success",
            "partition_id": part_id,
            "number_of_partitions": len(META.GLOBAL_VIEW)
        }

        try:
            data = {'global_view': META.GLOBAL_VIEW,
                    'directory': META.DIRECTORY}
            urls = ["http://" + node_ip +
                    "/kv-store/stupid_update" for node_ip in fn.get_all_nodes()]
            responses = fn.put_broadcast(data, urls)

            return fn.http_success(message)
        except:
            print("FAILED STUPID UPDATE")

    else:
        message = {
            "result": "error"  # TODO check if correct response
        }


def addNode(update_ip):
    if update_ip not in fn.get_all_nodes():  # Check if not already in views
        # First add to GLOBAL VIEW as proxy
        fn.add_proxy(update_ip)

        # If should be replica
        if len(fn.proxies() >= META.REPLICAS_PER_PART):

            # returns id of new partition
            part_id = updateProxies()
            for partition in META.GLOBAL_VIEW:
                if partition == 0:  # ignore proxies
                    continue

                # Tell all replicas to rebalance
                for node_ipp in META.GLOBAL_VIEW[partition]:
                    try:
                        url = "http://" + node_ipp + "/rebalance"
                        resp = requests.put(url, timeout=5)
                        break
                    except requests.exceptions.Timeout:
                        continue
        else:  # If just proxy, no promotion, just update new proxy
            updateProxy(update_ip)
            part_id = 0

        return fn.local_success(part_id)

    else:  # Node already existed in views, prob won't need this
        return ERROR, None


def removeNode(update_ip):
    # remove node from partition
    partition_id = fn.get_partition_id(update_ip)
    
    if fn.get_node_type(update_ip) == REPLICA:
    
        if len(META.GLOBAL_VIEW[0]) > 0:  # proxies exist
            replaceProxy = fn.proxies()[-1]

            # update our global view
            fn.remove_node(update_ip)  # remove from our global view
            fn.add_node(partition_id, replaceProxy)  # add to our global view

            fn.generateDirectory(fn.count_partitions())  # update our directory
            # upgrade the last proxy to a replica with latest data
            upgradeProxy(replaceProxy)

            if broadcastGlobals() == SUCCESS:
                pass
            else:
                raise ValueError('A very specific bad thing happened.')
        else:
            downgradeReplicas = fn.get_replicas(partition_id)

            fn.remove_partition(update_ip)
            # strip ex-replicas of their data, ruthlessly
            fn.downgrade_replicas(downgradeReplicas)

            if broadcastGlobals() == SUCCESS:
                rebalance()
            else:
                raise ValueError('A very specific bad thing happened.')
    else: # if proxy, stupid remove
        fn.remove_node(update_ip)  # remove from our global view

    return fn.local_success(partition_id)


def upgradeProxy(update_ip, partition_id):
    # data dump to give proxy God Data from replicas
    try:
        data = {
            "global_view": fn.dictionaryToString(META.GLOBAL_VIEW),
            "directory": fn.dictionaryToString(META.DIRECTORY),
            "node_type": REPLICA,
            "this_partition": fn.last_partition_id(),
            "replicas_per_part": META.REPLICAS_PER_PART
        }

        request.put("http://" + update_ip +
                    "/kv-store/stupid_update", data=data)
    except:
        print("FAILED UPDATE PROXY (remove)")

# Only ever called from add_Node()


def updateProxies():
    proxies = fn.proxies()[:]
    fn.add_partition(proxies)
    fn.clear_proxies()

    fn.generateDirectory(fn.count_partitions())

    # data dump to give all proxies God Data from replicas
    try:
        data = {
            "global_view": fn.dictionaryToString(META.GLOBAL_VIEW),
            "directory": fn.dictionaryToString(META.DIRECTORY),
            "node_type": REPLICA,
            "this_partition": fn.last_partition_id(),
            "replicas_per_part": META.REPLICAS_PER_PART
        }
        urls = ["http://" + node_ip +
                "/kv-store/stupid_update" for node_ip in proxies]
        responses = fn.put_broadcast(data, urls)
    except:
        print("FAILED UPDATE PROXIES")


@app.route('/key_dump', methods=['PUT'])
def key_dump():
    update_content = json.loads(request.form.get('content'))
    for key_info in update_content:
        resp = kv.put(key_info['key'], key_info['value'], fn.stringToList(
            key_info['causal_payload']), int(key_info['timestamp']))
    return resp


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

@app.route('/kv-store/get_partition_id')
def get_partition_id():
    m = META.THIS_PARTITION
    return fn.http_success({"result": "success", "partition_id": str(m)}, 200)

@app.route('/kv-store/get_all_partition_ids')
def get_all_partition_ids():
    LV = range(1, fn.count_partitions() + 1)
    return fn.http_success({"result": "success", "partition_id_list": ','.join(LV)}, 200)

@app.route('/kv-store/get_partition_members')
def get_partition_members():
	LV = []
	partition_id = request.form.get('partition_id')
    if 1 <= partition_id <= fn.count_partitions:
		LV = fn.get_replicas(partition_id)
		return fn.http_success({"result": "success", "partition_members": ', '.join(LV)}, 200)
	else:
		return fn.http_error({"result": "error", "partition_members": "failed"}, 404)

# HELPERS - MOVE TO FUNCTIONS #

def broadcastGlobals():
    try:
        data = {'global_view': META.GLOBAL_VIEW, 'directory': META.DIRECTORY}
        urls = ["http://" + node_ip +
                "/kv-store/stupid_update" for node_ip in fn.get_all_nodes()]
        responses = fn.put_broadcast(data, urls)

        return fn.http_success("SUCCESS")
    except:
        print("FAILED STUPID UPDATE")


def duplicateReplica(proxy_ipp):
    d, vc, timestamp = kv.getDictionaries()
    d, vc, timestamp = json.dumps(d), json.dumps(vc), json.dumps(timestamp)
    requests.put("http://" + proxy_ipp + "/kv-store/duplicate/",
                 data={"d": d, "vc": vc, "timestamp": timestamp})

# def duplicateNode(proxy_ipp):
#     d, vc, timestamp = kv.getDictionaries()
#     d, vc, timestamp = json.dumps(d), json.dumps(vc), json.dumps(timestamp)
#     requests.put("http://" + proxy_ipp + "/kv-store/duplicate/", data={"d": d, "vc": vc, "timestamp": timestamp})

# omg did you hear what Becky did???
def gossip(gossip_ipp):
    # Send all information to other node
    d, vc, timestamp = kv.getDictionaries()
    d, vc, timestamp = json.dumps(d), json.dumps(vc), json.dumps(timestamp)
    try:
        resp = requests.post("http://" + gossip_ipp + "/kv-store/gossip",
                             data={"d": d, "vc": vc, "timestamp": timestamp})
        resp = resp.json()

        # Gets info that it needs to update, and updates it's own data
        for key in resp:
            s, status_code = kv.put(
                key, resp[key]["value"], resp[key]["causal_payload"], resp[key]["timestamp"])
        return SUCCESS
    except:
        return ERROR


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
            (current_max_value, current_max_VC, current_max_timestamp) = (
                temp_value, temp_VC, temp_timestamp)

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
        for r in fn.get_replicas(META.THIS_PARTITION):
            status = gossip(r)
    sleep(10)


background_thread = Thread(target=ping, args=())
background_thread.start()

background_thread = Thread(target=runGossip, args=())
background_thread.start()

if __name__ == '__main__':
    # Run Command
    app.run(host=META.EXTERNAL_IP, port=int(META.PORT), debug=True)

