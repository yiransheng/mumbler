#!/opt/rh/python27/root/usr/bin/python

import os
import sys
import json
import time

import subprocess
import pipes

import hashutils
from nodeconfig import masternode, localnode, GPFS_STORAGE
from index_step_2 import build_master_index
from parse import is_parent_line, parse_parent_line, extract_next_words_fast
from search import search_next

from memcache import memcached

max_size = 4194304 # 4MB
base_dir = os.path.join(GPFS_STORAGE, "data")

if not os.path.exists(base_dir):
    os.makedirs(base_dir)

islocal = masternode.name == localnode.name

def process():
    print("Starting up...")
    offset = localnode.index_offset
    nnodes = len( localnode.nodes() )
    words_index = build_master_index()
    n = -1
    new_index = dict()
    print("Processing hash by hash...")
    it = gen_files()
    outfile, outfile_name = it.next()
    for hash32 in words_index:
        n += 1
        if n % nnodes != offset:
            continue

        data = load_hash32(hash32, words_index)
        if len(data) == 0:
            continue
        first_word = data.iterkeys().next()
        print("Handling word %s" % first_word)
        # hex decimal
        for word, content in data.iteritems():
            start_pos, end_pos, has_space = write_data_main(outfile, word, content)
            new_index[word] = {
              "file" : outfile_name,
              "start" : start_pos,
              "chunk_size" : end_pos - start_pos
            }
            print("Bytes: %d" % end_pos)
            if not has_space:
                print("%s is full" % outfile)
                outfile, outfile_name = it.next()
                print("moving on to %s" % outfile)

        try:
            it.send(True)
        except StopIteration:
            pass

    return new_index


def gen_files():
    node_id = localnode.index_offset
    index = 0
    done = None
    fl = None
    while done is not None:
        # fixed width hex decimal format of file index with leading node id (0,1,2)
        filename = "%d%0.8X" % (node_id, index)
        index +=1
        outfile = os.path.join(GPFS_STORAGE,
                           base_dir,
                           filename)
        if fl is not None:
            fl.close()

        fl = open(outfile, 'a')
        done = yield (fl, filename)

    if fl is not None:
        fl.close()




def load_hash32(hash32, words_index):
    if not isinstance(hash32, str):
        hash32 = str(hash32)

    locs = words_index.get(hash32)

    data = dict()
    if locs is None:
        return data

    print("reading %s data files" % str(len(locs)))

    for index, starting_pos, chunk_size in locs:
        word = extract_parent_word(index, starting_pos, chunk_size)
        if word is None:
            continue
        child_words, counts = extract_next_words_fast(word, index, starting_pos, chunk_size)
        if word in data:
            data[word]["counts"] += counts
            data[word]["children"].append(child_words)
        else:
            data[word] = { "counts": counts, "children": [child_words] }

    return data


def write_data_main(file, word, data):

    w = file
    start = w.tell()
    # SPACE word TAB counts NEW_LINE
    w.write(" %s\t%s\n" % (word, str(data["counts"])))
    for child in data["children"]:
        for child_word, child_count in child.iteritems():
            print(child_word, child_count)
        # word TAB count NEW_LINE
            if child_count > 0:
                w.write("%s\t%s\n" % (child_word, str(child_count)))

    end = w.tell()
    # file is not full, we can write some more stuff in
    if end < max_size:
        return start, end, True
    else:
        return start, end, False

def extract_parent_word(index, starting, chunk_size):
    datafile = os.path.join(GPFS_STORAGE, "gram2_%s.processed" % str(index))
    with open(datafile, 'r') as df:
        df.seek(starting)
        lines = df.read(chunk_size).split("\n")

    line = lines[0]
    if not is_parent_line(line):
        return None

    parent_word, _, counts = parse_parent_line(lines[0])
    return parent_word

if __name__ == "__main__":
    new_index = process()
    with open(os.path.join(GPFS_STORAGE, "master_index_%s" % localnode.name), 'w') as w:
        w.write(json.dumps(new_index))



