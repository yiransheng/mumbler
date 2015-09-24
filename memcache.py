import json
from pymemcache.client.base import Client
from nodeconfig import masternode, localnode

def json_serializer(key, value):
    if type(value) == str:
        return value, 1
    return json.dumps(value), 2

def json_deserializer(key, value, flags):
    if flags == 1:
        return value
    if flags == 2:
        return json.loads(value)
    raise Exception("Unknown serialization format")

if masternode.name == localnode.name:
    host = 'localhost'
else:
    host = masternode.name
memcached = Client((host, 11211), serializer=json_serializer,
                deserializer=json_deserializer)


