from imports import *

class META:
    # __slots__ = ("VIEW", "REPLICAS_WANTED", "PROXIES", "REPLICAS", "IP_PORT", "ID", "PORT", "EXTERNAL_IP")

    # Set defaults
    REPLICAS_PER_PART = 1
    IP_PORT = 10.0.0.21:8080 # or localhost?
    ID = 0
    PORT = 8080
    EXTERNAL_IP = None
    THIS_PARTITION = 0
    NODE_TYPE = PROXY
    GLOBAL_VIEW = {0: []}
    DIRECTORY = {}

# # KVS variables
# KVS = {
# #     # Key (String): Value (dict)
# #     # ex. Key - "Bob"
# #     # ex. Value - {value: "Smith",
# #     #              causal_payload: [1,2,4,2],
# #     #              timestamp: 13340948230
# #     #             }
# }

# VIEW = {
# #     # Set to Node Address mapping id's to Node URL's
# #     # Node_ID: {
# #     #     type: REPLICA or PROXY
# #     #     ip_port: 10.1.1.21:8080
# #     # }
# }
