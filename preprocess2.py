import os

import hashutils
from nodeconfig import localnode, GPFS_STORAGE
from index_step_2 import build_master_index
from parse import is_parent_line, parse_parent_line, extract_next_words_fast
from search import search_next

from memcache import memcached

max_size = 1048576 # 1MB
base_dir = os.path.join(GPFS_STORAGE, "data")

if not os.path.exists(base_dir):
    os.makedirs(base_dir)

def process():
    offset = localnode.index_offset
    nnodes = len( localnode.nodes() )
    words_index = build_master_index()
    n = -1
    print("Processing hash by hash...")
    for hash32 in words_index:
        n += 1
        if n % nnodes != offset:
            continue

        data = load_hash32(hash32, words_index)
        if len(data) == 0:
            continue
        first_word = data.iterkeys().next()
        print("Handling word %s" % first_word)
        hash16 = hashutils.hashword16(first_word)
        tail_word, tail_file, data, has_space = write_data_main(hash16, data)
        print("Wrote into file up to %s" % tail_file)
        if has_space:
            print("we have space add more stuff")
            next_words = search_next(tail_word, words_index)
            write_data_residuals(tail_file, next_words, words_index)


def load_hash32(hash32, words_index):
    if isinstance(hash32, int):
        hash32 = str(hash32)

    locs = words_index.get(hash32)

    data = dict()
    for index, starting_pos, chunk_size in locs:
        word = extract_parent_word(index, starting_pos, chunk_size)
        if word is None:
            continue
        child_words, counts = extract_next_words_fast(word, index, starting_pos, chunk_size)
        if word in data:
            data[word]["counts"] += counts
            merge_counters(data[word]["children"], child_words)
        else:
            data[word] = { "counts": counts, "children": child_words }

    return data


def write_data_main(filename, data):

    file_index = 0
    outfile = os.path.join(GPFS_STORAGE,
                           base_dir,
                           filename + "." + str(file_index))

    w = open(outfile, 'w')
    for word in data:
        # SPACE word TAB counts NEW_LINE
        w.write(" %s\t%s\n" % word, str(data[word]["counts"]))
        for child_word in data[word]["children"]:
            child_count = data[word]["children"]["child_word"]
        # word TAB count NEW_LINE
            w.write("%s\t%s\n" % child_word, str(child_count))

        if w.tell() >= max_size:
            w.close()
            file_index += 1
            outfile = os.path.join(GPFS_STORAGE,
                                   base_dir,
                                   filename + "." + str(file_index))
            w = open(outfile, 'w')

    # file is not full, we can write some more stuff in
    if w.tell() < max_size / 2:
        w.close()
        return word, outfile, data, True
    else:
        w.close()
        return word, outfile, data, False

def write_data_residuals(outfile, next_words, words_index):
    w = open(outfile, 'a')
    top_words = next_words.most_common(10)
    for top_word in top_words:
        hash32 = hashutils.hashword32int(top_word)
        data =  load_hash32(hash32, words_index)
        for word in data:
            # SPACE word TAB counts NEW_LINE
            w.write(" %s\t%s\n" % word, str(data[word]["counts"]))
            for child_word in data[word]["children"]:
                child_count = data[word]["children"]["child_word"]
            # word TAB count NEW_LINE
                w.write("%s\t%s\n" % child_word, str(child_count))

        if w.tell() >= max_size:
            break

    w.close()

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

def merge_counters(counter1, counter2):
    for word in counter2:
        if counter2[word] > 0:
            counter1[word] += counter2[word]

    return counter1

if __name__ == "__main__":
    process()


