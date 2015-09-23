from pymemcache.client.base import Client

def _serializer(key, value):
     key = str(key)
     if type(value) == str:
         return value, 1
     elif type(value) is tuple:
         x,y,z = value
         value = str(x) + ',' + str(y) + ',' + str(z)
         return value, 2
     else:
         raise Exception("Unknown serialization format")


def _deserializer(key, value, flags):
    if flags == 1:
        return value
    if flags == 2:
        x,y,z = [int(d) for d in value.split(',')]
        return x,y,z
    raise Exception("Unknown serialization format")

memcached = Client(('127.0.0.1', 11211), serializer=_serializer,
                deserializer=_deserializer)


