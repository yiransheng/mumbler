import sys
import random

import search

def mumbler(word, depth):
    if depth < 1:
        return word

    next_words, next_words_count = search.search_next(word)
    if next_words_count == 0:
        return word

    next_word = sample(next_words, next_words_count)

    return word + " " + mumbler(next_word, depth-1)


def sample(words, total_count):
    threshold = random.randint(0, total_count-1)
    index = 0
    for (word, count) in words.iteritems():
        index += count
        if threshold <= index:
            return word

if __name__ == '__main__':
    if len(sys.argv) == 3:
        start_word = sys.argv[1]
        try:
            start_count = int(sys.argv[2])
        except ValueError:
            sys.exit(1)
    else:
        sys.exit(1)

    print mumbler(start_word, start_count - 1)


