import os

import pickle
from memcache import memcached

from binindex import IndexEntry
from nodeconfig import localnode, GPFS_STORAGE

def build_master_index(cache=False):
    master_index = dict()
    size = IndexEntry.size()
    for node in localnode.nodes():
        print("Processing %s_index" % node)
        node_index_file = os.path.join(GPFS_STORAGE, "%s_index" % node)
        with open(node_index_file, 'r') as f:
            while True:
                chunk = f.read(size)
                if chunk == '':
                    break
                index_entry = IndexEntry.unpack(chunk)
                if index_entry.id not in master_index:
                    master_index[index_entry.id] = []

                index_content = (index_entry.index,
                     index_entry.offset,
                     index_entry.chunk_size)
                master_index[index_entry.id].append(index_content)
                if cache:
                    memcached.set(index_entry.id, index_content)

    print("done")
    return master_index

if __name__ == "__main__":
    build_master_index(cache=True)




