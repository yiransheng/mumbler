import os
import pickle
import hashlib

from collections import Counter

import nodeconfig

from parse import extract_next_words

def load_index_from_file(index_file="gram2_index"):
    index_file = os.path.join(nodeconfig.GPFS_STORAGE, index_file)
    print("Loading gram2 index file into RAM...")
    with open(index_file, 'r') as f:
        master_index = pickle.load(f)

    print("done")
    return master_index


def search_next(word, words_index):
    counter = Counter()
    next_words_count = 0

    md5 = hashlib.md5()
    md5.update(word)
    word_hash = md5.hexdigest()
    word_id = int(word_hash, 16) % 4294967296 # 2**32

    # is an list of tuples: (index, starting_pos, chunk_size)
    word_locs = words_index.get(word_id)
    for index, starting_pos, chunk_size in word_locs:
        words, count = extract_next_words_fast(word, index, starting_pos, chunk_size)
        for w in words:
            counter[w] += words[w]
        next_words_count += count

    return counter, next_words_count


