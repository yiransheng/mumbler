import os

from pybloom import BloomFilter

def parse_parent_line(line):
    tokens = line.split("\t")
    if len(tokens) == 3:
        word_token = tokens[0]
        child_count = int(tokens[1])
        l = len(word_token)
        word = word_token[2:l-2] \
                .replace("\\-\\[", "-[").replace("\\]\\-", "]-")
        return word, child_count

def is_parent_line(line):
    return len(line)>0 and line.startswith("-[")

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
                        word, skips = parse_parent_line(line)
                        bf.add(word)
                        for i in xrange(1, skips):
                            next(df)
                    line = next(df)
            except StopIteration:
                with open(filter_file, 'w') as ff:
                    bf.tofile(ff)
                del bf

        print("%s done." % filter_file)

create_filter("test.tsv")

with open("test.tsv.filter", 'r') as ff:
    bf = BloomFilter.fromfile(ff)
print("world" in bf)
print("fincial" in bf)
