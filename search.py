import os
import pickle

from collections import Counter

import nodeconfig

from parse import extract_next_words

def load_index(index_file="gram2_index"):
    index_file = os.path.join(nodeconfig.GPFS_STORAGE, index_file)
    print("Loading gram2 index file into RAM...")
    with open(index_file, 'r') as f:
        master_index = pickle.load(f)

    print("done")
    return master_index


def search_next(word):
    counter = Counter()
    next_words_count = 0
    for i in xrange(0, 100):
        if word in filters[i]:
            data_file = os.path.join(nodeconfig.GPFS_STORAGE,
                "gram2_" + str(i) + ".processed")
            words, count = extract_next_words(word, data_file)
            for w in words:
                counter[w] += words[w]
            next_words_count += count

    return counter, next_words_count


