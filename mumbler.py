#!/opt/rh/python27/root/usr/bin/python

import sys
import random

import search
from index_step_2 import build_master_index
from memcache import memcached



def mumbler(word, depth, words_index=memcached):
    if depth < 1:
        return word

    next_words, next_words_count = search.search_next(word, words_index)
    if next_words_count == 0:
        return word

    next_word = sample(next_words, next_words_count)

    return word + " " + mumbler(next_word, depth-1, words_index)


def sample(words, total_count):
    threshold = random.randint(0, total_count-1)
    index = 0
    for (word, count) in words.iteritems():
        index += count
        if threshold <= index:
            return word

def usage(cmd):
    print "Usage:", cmd, "word", "depth"
    print " - word: string, starting word for mumbler"
    print " - depth: int, how many words to generate"

if __name__ == '__main__':
    if len(sys.argv) >= 3:
        start_word = sys.argv[1]
        try:
            start_count = int(sys.argv[2])
        except ValueError:
            usage(sys.argv[0])
            sys.exit(1)
        words_index = memcached
        if len(sys.argv) >= 4 && sys.argv[3] == "--slow":
            print('Loading index files from disk, this takes a while...')
            words_index = build_master_index()
    else:
        usage(sys.argv[0])
        sys.exit(1)

    print mumbler(start_word, start_count - 1, words_index)



