'''
asgn4
- replicas within a partition should all have same data
- data should be split among partitions
- proxies forward requests to appropriate partition
'''

'''
kv-store initializing ops
'''
K = os.getenv('K') # Number of replicas per partition

VIEW # List of ALL (dead or alive) ip ports (ex. [10.0.0.21:8080])

IPPORT # Own ip port

MY_ID = IPPORT[IPPORT.index(':')-1] # Unique node id (ex. 0, 1, 2)

# At init, proxies & replicas should be derived from views
REPLICAS = VIEW[(N%K):] # List of ip ports of all replicas

PROXIES = VIEW[-(N%K):] # List of ip ports of remainder proxy nodes

N = PROXIES + REPLICAS # Total number of alive nodes

PARTITIONS = N/K # Number of partitions

IS_REPLICA = IPPORT in REPLICAS # T/F if node is replica

# Id of partition node is in, ALL nodes have one
PARTITION_ID = (VIEW.index(IPPORT)+1)/K # ex. [1, 1, 1, 2, 2, 2, 3, 3]
# or
# if IS_REPLICA else -1


'''
kv-store get, put
'''
@app.route('/kv-store/<key>')
def get(key):
	
	try:
		keyCheck(key) # also keep a {} of only all keys and check if it exists?
	except:
		return "Key does not exist"

	client_cp = PARSE(client_cp)

	if is replica and if key is in my own kv-store:
		broadcast to other replicas in partition to get most updated {value, cp, time}
		
		try:
			client_cp is < cp
		except:
			r = "Invalid causal payload"

		# TODO: make one parse() that parses according to which format it's in
		return message = "success", value, PARSE(cp), time

	elif (is proxy) or (if is replica but key is not in own store):
		broadcast to appropriate partition, any replica to get {rvalue, rcp, rtime}

		try:
			client_cp is < rcp
		except:
			return r = "Invalid causal payload"

		return message = "success", rvalue, PARSE(rcp), rtime

	else:
		return "failure"


@app.route('/kv-store/<key>', methods=['PUT'])
def put(key):
	try:
		keyCheck(key, value)
	except:
		return "Invalid key/value"

	check if existing key:
		get {value, cp, time}
		if PARSE(client_cp) > cp or client_time > time:
			return "Invalid causal payload"

	if is replica and key should be in my own kv-store:
		if client_cp == "''":
			initialize(client_cp)
		kv.put in own store with client_cp
		if size of own partition > 1:
			broadcast to other replicas in partition

	elif (is proxy) or (if is replica but key is not in own store):
		put to appropriate partition, any replica

	return "success", value, PARSE(cp), time
'''
kv-store resizing ops
'''
@app.route('/kv-store/update_view', methods=['PUT'])
def update_view(ip_port):

	# Input data {}
    type = request.args.get('type')
    update_ip = request.form.get('ip_port')

    if type == "add":
    	broadcast stupidAdd request to ALL nodes, including self

    elif type == "remove":
    	broadcast stupidRemove request to ALL nodes, including self

def stupidAdd(update_ip):
	if update_ip is not in VIEW:
		add to VIEW
		update REPLICAS & PROXIES list
		bring new node up-to-date # or just wait for gossip if proxy?
	else: # already existed before
		if alive:
			return "failure"
		if dead:
			update REPLICAS & PROXIES list
			bring zombie node up-to-date

def stupidRemove(update_ip):
	try:
		remove from REPLICAS/PROXIES
		update REPLICAS & PROXIES list
	else: # is not even in views
		return "failure"

'''
errorResponse:
{
    "result":"error",
    "error":"key value store is not available"
}
'''

'''
new APIs
'''
@app.route('/kv-store/get_partition_id')
@app.route('/kv-store/get_all_partition_ids')
@app.route('/kv-store/get_partition_members')

'''
BG functions:
'''
def gossip():
	broadcast to all alive replicas

def ping():
	ping all replicas
	if replica does not respond:
		if replica was only one in partition:
			if proxies:
				update_view(proxy) # promote proxy
			else:
				rebalance key store across remaining partitions
				partition - 1
		remove replica from REPLICAS
		