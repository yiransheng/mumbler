import socket

import os.path

class GPFSNode(object):
    LOCAL_STORAGE = "/root/tmp/2gram"
    GPFS_STORAGE = "/gpfs/gpfsfpo"

    def __init__(self, node_file="/root/nodefile", name=socket.gethostname().split('.')[0]):
        self.name = name
        self._node_file = node_file
        self.__load_node_file()

    def __load_node_file(self):
        self._nodes = []
        if os.path.isfile(self._node_file):
            with open(self._node_file, 'r') as node_lines:
                for node_line in node_lines:
                    node = node_line.split(':')[0]
                    node = node.split('.')[0]
                    self._nodes.append(node)

            try:
                self.index_offset = self._nodes.index(self.name)
            except ValueError:
                # invalid nodefile, probably running in local/testing mode
                self._nodes = [self.name]
                self.index_offset = 0
        else:
            self._nodes.append(self.name)
            self.index_offset = 0

    def indices(self):
        # 100 2gram data files
        return xrange(self.index_offset, 100, len(self._nodes))

    def filenames(self):
        for index in self.indices():
            yield "gram2_" + str(index)

    def nodes(self):
        return list(self._nodes)

    def remotes(self):
        remotes = self.nodes()
        remotes.remove(self.name)
        return remotes

localnode = GPFSNode()
masternode = GPFSNode(name="gpfs2")
LOCAL_STORAGE = GPFSNode.LOCAL_STORAGE
GPFS_STORAGE = GPFSNode.GPFS_STORAGE
