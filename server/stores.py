from imports import *

class METADATA:
    # __slots__ = ("VIEW", "REPLICAS_WANTED", "PROXIES", "REPLICAS", "IP_PORT", "ID", "PORT", "EXTERNAL_IP")

    VIEW = {}
    REPLICAS_WANTED = None
    PROXIES = None
    REPLICAS = None
    IP_PORT = None
    ID = None
    PORT = None
    EXTERNAL_IP = None

# # KVS variables
# KVS = {
#     # Key (String): Value (dict)
#     # ex. Key - "Bob"
#     # ex. Value - {value: "Smith",
#     #              causal_payload: [1,2,4,2],
#     #              timestamp: 13340948230
#     #             }
# }

# VIEW = {
#     # Set to Node Address mapping id's to Node URL's
#     # Node_ID: {
#     #     type: REPLICA or PROXY
#     #     ip_port: 10.1.1.21:8080
#     # }
# }