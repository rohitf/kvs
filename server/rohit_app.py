import functions as fn
from globals import *

app = Flask(__name__)

@app.route('/kv-store/get_all_replicas')
def get_all():
    replicas = {"replicas": fn.get_replicas()}
    return fn.generate_response(replicas)

@app.route('/kv-store/<key>', methods=['PUT'])
def put_key(key):
    # If Replica
    # Update local KV
    # Broadcast message to clients on socket connection
    # message: KV store
    # Clients update their vectors as needed
    # Profit!
    # if not replica
    # fwd request to replica

    # return fn.generate_response({"hi": 5}, 200)

    val = request.form.get('val')
    client_cp = request.form.get('causal_payload')
    timestamp = int(time.time())

    if fn.is_replica(METADATA.ID):
        if client_cp is not None:
            curr_cp = kv.get_payload(key)

            if curr_cp is not None:
                status = fn.compare_payload(curr_cp, fn.parse_client_payload(client_cp))
                if status == LATEST:
                    return fn.http_error("Old Data!", 403)
            else:
                # Create key with value and payload
                kv.put(key, val, client_cp)

        else:
            kv.put(key, val)

    else: # A proxy, so fwd request
        random_replica = fn.random_replica()
        requests.get("http://" + random_replica + "/kv-store/" + key, data={'val': val, 'client_cp': client_cp})

if __name__ == '__main__':
    view_list = os.getenv('VIEW').split(",")
    REPLICAS_WANTED = int(os.getenv('K'))  # K value given in command line
    
    r = REPLICAS_WANTED

    # Update VIEW
    for view in view_list:
        node_type = REPLICA if r > 0 else PROXY
        node_id = fn.get_id(view)
        fn.add_node(node_id, node_type, view)
        r -= r - 1
    
    # Update METADATA
    METADATA.REPLICAS_WANTED = int(os.getenv('K'))
    METADATA.IP_PORT = view
    METADATA.ID = fn.get_id(view)
    METADATA.REPLICAS = fn.get_replicas()
    METADATA.PROXIES = fn.get_proxies()
    METADATA.EXTERNAL_IP = os.getenv('IP')
    METADATA.PORT = os.getenv('PORT')

    app.run(host=METADATA.EXTERNAL_IP, port=int(METADATA.PORT), debug=True)