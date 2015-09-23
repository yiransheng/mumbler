import os

import hashlib

from contextlib import nested

from nodeconfig import localnode, GPFS_STORAGE
from parse import is_parent_line, parse_parent_line

from binindex import IndexEntry


def index_all():
    index_file = os.path.join(GPFS_STORAGE, localnode.name + "_index")
    writer = open(index_file, 'w')
    for i in localnode.indices():
        print("Indexing file: gram2_%s.processed" % str(i))
        index_processed_file(i, writer)
    writer.close()
    print("done")



def index_processed_file(index, writer):
    datafile = os.path.join(GPFS_STORAGE, "gram2_%s.processed" % str(index))
    with open(datafile, 'r') as f:
        pos = 0
        line = f.readline()
        while True:
            if is_parent_line(line):
                word, skip_lines, _ = parse_parent_line(line)
                starting_pos = pos
                md5 = hashlib.md5()
                md5.update(word)
                word_hash = md5.hexdigest()
                for i in range(0, skip_lines):
                    f.readline()

                chunk_size = f.tell() - starting_pos
                index_entry = IndexEntry(word_hash, index, starting_pos, chunk_size)
                writer.write(index_entry.pack())
                pos = f.tell()
            else:
                if line == '':
                    break # last line is empty in the data file, we are done here
                else:
                    raise ValueError('Improper data file %s' % datafile)

            line = f.readline()

if __name__ == "__main__":
    index_all()
