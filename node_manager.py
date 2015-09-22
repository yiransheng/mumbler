import socket

import os.path


class GPFSNodeManager(object):

    local_name = socket.gethostname().split('.')[0]
    local_storage = "/tmp/2gram"
    gpfs_storage = "/gpfs/gpfsfpo"

    def __init__(self, node_file="/root/nodefile"):
        self.node_file = node_file
        self.__load_node_file()

    def __load_node_file(self):
        self._nodes = []
        if os.path.isfile(self.node_file):
            with open(self.node_file, 'r') as node_lines:
                for node_line in node_lines:
                    node = node_line.split(':')[0]
                    node = node.split('.')[0]
                    self._nodes.append(node)

            self.index_offset = self._nodes.index(GPFSNodeManager.local_name)
        else:
            self._nodes.append(GPFSNode.local_name)
            self.index_offset = 0

    def nodes(self):
        return list(self._nodes)

    def count(self):
        return len(self._nodes)


    def remotes(self):
        remotes = list(self._nodes)
        remotes.remove(self.local())
        return remotes

    @staticmethod
    def local():
        return GPFSNode.local_name

local_node = GPFSNode()

