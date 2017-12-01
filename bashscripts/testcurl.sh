curl -i ver http://192.168.99.100:8083/kv-store/get_all_replicas
curl -i -ver http://192.168.99.100:8083/kv-store/dog -X PUT -d "val=bark" -d "causal_payload=''"
curl -i -ver http://192.168.99.100:8083/kv-store/dog -X PUT -d {"val":"bark", "causal_payload":"''"}
