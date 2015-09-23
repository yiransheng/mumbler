import json

from pymemcache.client.base import Client

def json_serializer(key, value):
     key = str(key)
     if type(value) == str:
         return value, 1
     return json.dumps(value), 2

def json_deserializer(key, value, flags):
    if flags == 1:
        return value
    if flags == 2:
        return json.loads(value)
    raise Exception("Unknown serialization format")

memcached = Client(('127.0.0.1', 11211), serializer=json_serializer,
                deserializer=json_deserializer)
