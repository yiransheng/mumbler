import os

from collections import Counter

import nodeconfig

from pybloom import BloomFilter
from parse import extract_next_words

filters = []
for i in xrange(0, 100):
    filter_file = os.path.join(nodeconfig.GPFS_STORAGE,
        "gram2_" + str(i) + ".processed.filter")
    with open(filter_file, 'r') as f:
        filters.append(BloomFilter.fromfile(f))

def search_next(word):
    counter = Counter()
    next_words_count = 0
    for i in xrange(0, 100):
        if word in filters[i]:
            data_file = os.path.join(nodeconfig.GPFS_STORAGE,
                "gram2_" + str(i) + ".processed")
            words, count = extract_next_words(data_file)
            for w in words:
                counter[w] += words[w]
            next_words_count += count

    return counter, next_words_count


