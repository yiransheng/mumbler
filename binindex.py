from struct import unpack, pack, calcsize

from nodeconfig import localnode, GPFS_STORAGE


class IndexEntry(object):
    '''
    unsigned int | short | unsigned int | unsigned int
    md5hash % 2^32 | file index (0 to 100) | starting byte | chunk size
    '''
    FMT = "<IhII"

    def __init__(self, id_or_hash, file_index, starting_byte, chunk_size):
        self.index = file_index
        self.offset = starting_byte
        self.chunk_size = chunk_size

        if isinstance(id_or_hash, int):
            self.id = id_or_hash
        else:
            self.id = int(id_or_hash, 16) % 4294967296 # 2**32


    def pack(self):
        return pack(self.FMT, self.id, self.index, self.offset, self.chunk_size)

    @classmethod
    def unpack(cls, bytes):
        return IndexEntry(*unpack(cls.FMT, bytes))

    @classmethod
    def size(cls):
        return calcsize(cls.FMT)




