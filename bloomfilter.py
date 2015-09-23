import os
import sys

from pybloom import BloomFilter

from parse import parse_parent_line, is_parent_line
from nodeconfig import localnode, GPFS_STORAGE


def create_filter_all(force=False):
    for filename in localnode.filenames():
        datafile = os.path.join(GPFS_STORAGE, filename + ".processed")
        create_filter(datafile, force)

def create_filter(datafile, force=False):
    assert os.path.isfile(datafile)
    datadir, datafilename = os.path.split(datafile)
    filter_file = os.path.join(datadir, datafilename + ".filter")
    if force or not os.path.isfile(filter_file):
        bf = BloomFilter(capacity=1e6)
        with open(datafile) as df:
            line = next(df)
            try:
                while True:
                    if is_parent_line(line):
                        word, skips, _ = parse_parent_line(line)
                        bf.add(word)
                        for i in xrange(1, skips):
                            next(df)
                    line = next(df)
            except StopIteration:
                with open(filter_file, 'w') as ff:
                    bf.tofile(ff)
                del bf

        print("%s done." % filter_file)

if __name__ == '__main__':
    force = False
    index = -1
    if len(sys.argv) >= 2:
        force = sys.argv[1] == "--force" or sys.argv[1] == "-f"
    if len(sys.argv) >= 3:
        index = int(sys.argv[2])

    if index == -1:
        create_filter_all(force)
    else:
        create_filter(
            os.path.join(GPFS_STORAGE,
                localnode.filenames()[index] + ".processed"),
            True)

